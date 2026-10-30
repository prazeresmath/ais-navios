"""
Regras de eventos de fundeadouro — Python puro, sem Flink.

O job do Flink (anchorage_job.py) só guarda o estado e agenda timers; toda a
decisão "isso é um evento?" mora aqui, para poder ser testada sem cluster.

Eventos emitidos (campo event_type):
    anchorage_enter  navio entrou num polígono de fundeadouro
    wait_start       ficou parado >= WAIT_MIN_S dentro da zona (ts = quando parou)
    anchorage_exit   saiu do polígono (duração total e, se esperou, tempo de espera)
    port_arrival     entrou na zona do porto depois de ter esperado no fundeadouro
    signal_lost      ficou LOST_AFTER_S sem sinal dentro da zona; encerra a sessão

Incerteza (a cobertura AIS tem buracos e a coleta pode parar):
    - A saída só é vista quando o navio aparece FORA da zona. Por isso o
      anchorage_exit traz last_inside_ts_ms (última vez visto dentro) e a espera
      vira um intervalo: wait_min_s (até a última vez dentro) e wait_s (até ser
      visto fora, o máximo). exit_gap_s = tamanho desse "buraco".
    - start_censored = True quando o navio já estava parado no primeiro
      avistamento: a espera começou antes de o vermos e está subestimada.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone

POSITION_TYPES = {"PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport"}

STOP_KN = 0.5                 # abaixo disso o navio está parado
WAIT_MIN_S = 15 * 60          # parado por pelo menos 15 min = começou a esperar
LOST_AFTER_S = 6 * 3600       # 6 h sem sinal dentro da zona = sessão encerrada
ARRIVAL_WINDOW_S = 24 * 3600  # chegada ao porto conta se ocorrer até 24 h depois de sair do fundeadouro
ZONE_STICKY_DEG = 0.002       # ~200 m de tolerância para não "piscar" na borda da zona
FIRST_SIGHT_GAP_S = 60 * 60   # sem nenhuma posição do navio há 1 h = "primeiro avistamento"


# ------------------------------------------------------------------ posição


@dataclass(frozen=True)
class Position:
    mmsi: int
    ts_ms: int
    lat: float
    lon: float
    sog: float | None
    nav_status: int | None

    @property
    def stopped(self) -> bool:
        # nav_status 1 = fundeado (classe A); vale mesmo com SOG ausente
        return (self.sog is not None and self.sog < STOP_KN) or self.nav_status == 1


def _parse_time_ms(value: str | None) -> int | None:
    """'2026-10-14 15:28:07.506871542 +0000 UTC' -> epoch ms."""
    if not value:
        return None
    for fmt, size in (("%Y-%m-%d %H:%M:%S.%f", 26), ("%Y-%m-%d %H:%M:%S", 19)):
        try:
            dt = datetime.strptime(value[:size], fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    return None


def parse_position(raw: str | bytes) -> Position | None:
    """JSON bruto do aisstream.io -> Position (só mensagens de posição válidas)."""
    try:
        msg = json.loads(raw)
    except (ValueError, TypeError):
        return None
    msg_type = msg.get("MessageType")
    if msg_type not in POSITION_TYPES:
        return None
    meta = msg.get("MetaData") or {}
    body = (msg.get("Message") or {}).get(msg_type) or {}
    mmsi, lat, lon = meta.get("MMSI"), body.get("Latitude"), body.get("Longitude")
    ts_ms = _parse_time_ms(meta.get("time_utc"))
    if not isinstance(mmsi, int) or ts_ms is None:
        return None
    if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
        return None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):  # 91/181 = indisponível
        return None
    sog = body.get("Sog")
    sog = float(sog) if isinstance(sog, (int, float)) and sog < 102.3 else None
    nav = body.get("NavigationalStatus") if msg_type == "PositionReport" else None
    return Position(mmsi, ts_ms, float(lat), float(lon), sog, nav if isinstance(nav, int) else None)


# ------------------------------------------------------------------ zonas


class Zones:
    """Polígonos de um GeoJSON com properties {name, kind}; kind = 'anchorage' | 'port'."""

    def __init__(self, features: list[dict]):
        from shapely.geometry import Point, shape  # import tardio: só quem usa zonas precisa do shapely
        from shapely.prepared import prep

        self._Point = Point
        self.zones = []
        for f in features:
            props = f.get("properties") or {}
            kind = props.get("kind")
            if kind not in ("anchorage", "port"):
                continue
            geom = shape(f["geometry"])
            self.zones.append({
                "name": props["name"],
                "kind": kind,
                "geom": prep(geom),
                "sticky": prep(geom.buffer(ZONE_STICKY_DEG)),
            })
        if not self.zones:
            raise ValueError("Nenhuma zona 'anchorage' ou 'port' no GeoJSON")

    @classmethod
    def load(cls, path: str) -> "Zones":
        with open(path, encoding="utf-8") as fh:
            return cls(json.load(fh)["features"])

    def zone_of(self, lat: float, lon: float, current: str | None = None) -> tuple[str, str] | None:
        """(nome, tipo) da zona que contém o ponto. A zona atual tem ~200 m de tolerância."""
        pt = self._Point(lon, lat)
        if current:
            for z in self.zones:
                if z["name"] == current and z["sticky"].contains(pt):
                    return z["name"], z["kind"]
        for z in self.zones:
            if z["geom"].contains(pt):
                return z["name"], z["kind"]
        return None


# ------------------------------------------------------------------ máquina de estados


def _event(event_type: str, pos_or_mmsi, ts_ms: int, zone: str, **extra) -> dict:
    mmsi = pos_or_mmsi.mmsi if isinstance(pos_or_mmsi, Position) else pos_or_mmsi
    ev = {"event_type": event_type, "mmsi": mmsi, "zone": zone, "ts_ms": ts_ms}
    if isinstance(pos_or_mmsi, Position):
        ev["lat"], ev["lon"] = round(pos_or_mmsi.lat, 6), round(pos_or_mmsi.lon, 6)
    ev.update({k: v for k, v in extra.items() if v is not None})
    return ev


def _exit_event(pos: Position, session: dict) -> dict:
    wait_start = session.get("wait_start_ts")
    last_inside = session["last_ts"]
    return _event(
        "anchorage_exit", pos, pos.ts_ms, session["zone"],
        enter_ts_ms=session["enter_ts"],
        wait_start_ts_ms=wait_start,
        last_inside_ts_ms=last_inside,
        duration_s=(pos.ts_ms - session["enter_ts"]) // 1000,
        wait_s=(pos.ts_ms - wait_start) // 1000 if wait_start else None,
        wait_min_s=(last_inside - wait_start) // 1000 if wait_start else None,
        exit_gap_s=(pos.ts_ms - last_inside) // 1000,
        start_censored=bool(session.get("start_censored")),
    )


class AnchorageTracker:
    """
    Estado por navio (um dict serializável em JSON):
        last_ts  último horário processado
        session  sessão aberta no fundeadouro: zone, enter_ts, stopped_since, wait_start_ts, last_ts
        pending  navio que esperou e saiu do fundeadouro, aguardando chegar ao porto
    """

    def process(self, state: dict | None, pos: Position, zone: tuple[str, str] | None):
        """Devolve (eventos, novo_estado, horário_do_próximo_timer)."""
        state = dict(state or {})
        prev_ts = state.get("last_ts")
        if prev_ts is not None and pos.ts_ms <= prev_ts:
            return [], state, self._next_timer(state)  # repetida ou fora de ordem
        first_sight = prev_ts is None or pos.ts_ms - prev_ts > FIRST_SIGHT_GAP_S * 1000
        state["last_ts"] = pos.ts_ms
        events: list[dict] = []
        session = state.get("session")
        pending = state.get("pending")

        # pendência de chegada expirada
        if pending and pos.ts_ms - pending["exit_ts"] > ARRIVAL_WINDOW_S * 1000:
            pending = None

        if zone and zone[1] == "anchorage":
            name = zone[0]
            if session and session["zone"] != name:
                events.append(_exit_event(pos, session))
                session = None
            if session is None:
                session = {"zone": name, "enter_ts": pos.ts_ms, "stopped_since": None, "wait_start_ts": None,
                           # já parado no primeiro avistamento: a espera começou antes de o vermos
                           "start_censored": bool(first_sight and pos.stopped)}
                pending = None  # voltou a fundear: a espera anterior não terminou no porto
                events.append(_event("anchorage_enter", pos, pos.ts_ms, name))
            if pos.stopped:
                session["stopped_since"] = session["stopped_since"] or pos.ts_ms
                if session["wait_start_ts"] is None and pos.ts_ms - session["stopped_since"] >= WAIT_MIN_S * 1000:
                    session["wait_start_ts"] = session["stopped_since"]
                    events.append(_event("wait_start", pos, session["stopped_since"], name,
                                         enter_ts_ms=session["enter_ts"],
                                         start_censored=bool(session.get("start_censored"))))
            else:
                session["stopped_since"] = None
            session["last_ts"] = pos.ts_ms
        else:
            if session:
                events.append(_exit_event(pos, session))
                if session.get("wait_start_ts"):
                    pending = {"zone": session["zone"], "wait_start_ts": session["wait_start_ts"],
                               "exit_ts": pos.ts_ms, "last_inside_ts": session["last_ts"],
                               "start_censored": bool(session.get("start_censored"))}
                session = None
            if zone and zone[1] == "port" and pending:
                events.append(_event(
                    "port_arrival", pos, pos.ts_ms, zone[0],
                    from_zone=pending["zone"],
                    wait_start_ts_ms=pending["wait_start_ts"],
                    exit_ts_ms=pending["exit_ts"],
                    last_inside_ts_ms=pending.get("last_inside_ts"),
                    wait_s=(pending["exit_ts"] - pending["wait_start_ts"]) // 1000,
                    wait_min_s=((pending["last_inside_ts"] - pending["wait_start_ts"]) // 1000
                                if pending.get("last_inside_ts") else None),
                    exit_gap_s=((pending["exit_ts"] - pending["last_inside_ts"]) // 1000
                                if pending.get("last_inside_ts") else None),
                    total_s=(pos.ts_ms - pending["wait_start_ts"]) // 1000,
                    start_censored=bool(pending.get("start_censored")),
                ))
                pending = None

        state["session"], state["pending"] = session, pending
        return events, state, self._next_timer(state)

    def on_timer(self, state: dict | None, mmsi: int, timer_ts: int):
        """Timer de horário do evento: encerra sessão sem sinal e limpa pendência vencida."""
        state = dict(state or {})
        events: list[dict] = []
        session, pending = state.get("session"), state.get("pending")
        if session and timer_ts >= session["last_ts"] + LOST_AFTER_S * 1000:
            wait_start = session.get("wait_start_ts")
            events.append(_event(
                "signal_lost", mmsi, session["last_ts"], session["zone"],
                enter_ts_ms=session["enter_ts"],
                wait_start_ts_ms=wait_start,
                last_inside_ts_ms=session["last_ts"],
                duration_s=(session["last_ts"] - session["enter_ts"]) // 1000,
                wait_s=(session["last_ts"] - wait_start) // 1000 if wait_start else None,
                wait_min_s=(session["last_ts"] - wait_start) // 1000 if wait_start else None,
                start_censored=bool(session.get("start_censored")),
            ))
            session = None
        if pending and timer_ts >= pending["exit_ts"] + ARRIVAL_WINDOW_S * 1000:
            pending = None
        state["session"], state["pending"] = session, pending
        return events, state, self._next_timer(state)

    @staticmethod
    def _next_timer(state: dict) -> int | None:
        if state.get("session"):
            return state["session"]["last_ts"] + LOST_AFTER_S * 1000
        if state.get("pending"):
            return state["pending"]["exit_ts"] + ARRIVAL_WINDOW_S * 1000
        return None
