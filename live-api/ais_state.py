"""
Estado em memória: a última posição e os dados estáticos de cada navio.

Não depende de Kafka nem de FastAPI — recebe o JSON bruto do aisstream.io e
devolve o que mudou. Isso facilita testar o parse isoladamente.

Regras de parse alinhadas com a camada silver do ClickHouse
(clickhouse/schema/02_silver_mvs.sql): valores "indisponível" do AIS viram None.
"""

from __future__ import annotations

import json
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

POSITION_TYPES = {"PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport"}

# Status de navegação (só classe A), ITU-R M.1371
NAV_STATUS = {
    0: "Navegando com motor",
    1: "Fundeado",
    2: "Sem governo",
    3: "Manobrabilidade restrita",
    4: "Restrito pelo calado",
    5: "Atracado",
    6: "Encalhado",
    7: "Pescando",
    8: "Navegando a vela",
}


def category_from_type(ship_type: int | None) -> str:
    """Mesma regra da view ais.ships no ClickHouse."""
    t = ship_type
    if t is None:
        return "Desconhecido"
    if t == 30:
        return "Pesca"
    if t in (31, 32, 52):
        return "Rebocador"
    if t in (33, 34, 35) or 51 <= t <= 59:
        return "Serviço"
    if t in (36, 37):
        return "Recreio"
    if 40 <= t <= 49:
        return "Alta velocidade"
    if t == 50:
        return "Prático"
    if 60 <= t <= 69:
        return "Passageiros"
    if 70 <= t <= 79:
        return "Carga"
    if 80 <= t <= 89:
        return "Tanque"
    return "Outros"


def parse_time_utc(value: str | None) -> float | None:
    """'2026-10-14 15:28:07.506871542 +0000 UTC' -> epoch em segundos."""
    if not value:
        return None
    try:
        dt = datetime.strptime(value[:26], "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        try:
            dt = datetime.strptime(value[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return dt.replace(tzinfo=timezone.utc).timestamp()


def _clean(value, unavailable) -> float | None:
    if not isinstance(value, (int, float)) or value >= unavailable:
        return None
    return float(value)


def _text(value) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip().strip("@").strip()
    return value or None


@dataclass
class Ship:
    mmsi: int
    name: str | None = None
    ais_class: str | None = None
    ship_type: int | None = None
    length_m: int | None = None
    destination: str | None = None
    lat: float | None = None
    lon: float | None = None
    sog: float | None = None
    cog: float | None = None
    heading: float | None = None
    nav_status: int | None = None
    ts: float | None = None  # horário da última posição (epoch s, UTC)
    track: deque = field(default_factory=lambda: deque(maxlen=30))

    def to_dict(self) -> dict:
        return {
            "mmsi": self.mmsi,
            "name": self.name,
            "ais_class": self.ais_class,
            "category": category_from_type(self.ship_type),
            "length_m": self.length_m,
            "destination": self.destination,
            "lat": self.lat,
            "lon": self.lon,
            "sog": self.sog,
            "cog": self.cog,
            "heading": self.heading,
            "nav_status": self.nav_status,
            "nav_status_text": NAV_STATUS.get(self.nav_status) if self.nav_status is not None else None,
            "ts": self.ts,
            "track": [list(p) for p in self.track],
        }


class ShipState:
    def __init__(self, max_age_s: float = 6 * 3600):
        self.ships: dict[int, Ship] = {}
        self.max_age_s = max_age_s
        self.messages = 0

    def _ship(self, mmsi: int) -> Ship:
        ship = self.ships.get(mmsi)
        if ship is None:
            ship = self.ships[mmsi] = Ship(mmsi=mmsi)
        return ship

    def apply(self, raw: bytes | str) -> Ship | None:
        """Aplica uma mensagem. Devolve o navio se algo visível mudou, senão None."""
        try:
            msg = json.loads(raw)
        except (ValueError, TypeError):
            return None
        msg_type = msg.get("MessageType")
        meta = msg.get("MetaData") or {}
        mmsi = meta.get("MMSI")
        if not isinstance(mmsi, int) or not msg_type:
            return None
        body = (msg.get("Message") or {}).get(msg_type) or {}
        self.messages += 1

        if msg_type in POSITION_TYPES:
            return self._apply_position(mmsi, msg_type, meta, body)
        if msg_type == "ShipStaticData":
            return self._apply_static_a(mmsi, body)
        if msg_type == "StaticDataReport":
            return self._apply_static_b(mmsi, body)
        return None

    def _apply_position(self, mmsi, msg_type, meta, body) -> Ship | None:
        lat, lon = body.get("Latitude"), body.get("Longitude")
        # 91 / 181 = posição indisponível
        if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
            return None
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return None
        ts = parse_time_utc(meta.get("time_utc")) or time.time()
        ship = self._ship(mmsi)
        if ship.ts is not None and ts < ship.ts:
            return None  # mensagem mais velha que a que já temos (reentrega/atraso)
        ship.lat, ship.lon, ship.ts = float(lat), float(lon), ts
        ship.sog = _clean(body.get("Sog"), 102.3)
        ship.cog = _clean(body.get("Cog"), 360)
        ship.heading = _clean(body.get("TrueHeading"), 360)
        ship.ais_class = "A" if msg_type == "PositionReport" else "B"
        ship.nav_status = body.get("NavigationalStatus") if msg_type == "PositionReport" else None
        if not ship.name:
            ship.name = _text(meta.get("ShipName"))
        ship.track.append((round(ship.lon, 6), round(ship.lat, 6), ts))
        return ship

    def _apply_static_a(self, mmsi, body) -> Ship:
        ship = self._ship(mmsi)
        ship.ais_class = ship.ais_class or "A"
        ship.name = _text(body.get("Name")) or ship.name
        ship.ship_type = body.get("Type") or ship.ship_type
        dim = body.get("Dimension") or {}
        length = (dim.get("A") or 0) + (dim.get("B") or 0)
        ship.length_m = length or ship.length_m
        ship.destination = _text(body.get("Destination")) or ship.destination
        return ship

    def _apply_static_b(self, mmsi, body) -> Ship | None:
        ship = self._ship(mmsi)
        ship.ais_class = ship.ais_class or "B"
        ra, rb = body.get("ReportA") or {}, body.get("ReportB") or {}
        if ra.get("Valid"):
            ship.name = _text(ra.get("Name")) or ship.name
        if rb.get("Valid"):
            ship.ship_type = rb.get("ShipType") or ship.ship_type
            dim = rb.get("Dimension") or {}
            length = (dim.get("A") or 0) + (dim.get("B") or 0)
            ship.length_m = length or ship.length_m
        return ship

    def prune(self, now: float | None = None) -> list[int]:
        """Remove navios sem posição há mais de max_age_s. Devolve os MMSI removidos."""
        now = now or time.time()
        gone = [m for m, s in self.ships.items() if s.ts is not None and now - s.ts > self.max_age_s]
        for m in gone:
            del self.ships[m]
        return gone

    def snapshot(self) -> list[dict]:
        """Só navios com posição conhecida (os que dá para desenhar)."""
        return [s.to_dict() for s in self.ships.values() if s.lat is not None]
