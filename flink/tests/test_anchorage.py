"""
Testes das regras de fundeadouro.

    python -m pytest flink/tests            (só a lógica, rápido)
    RUN_FLINK=1 python -m pytest flink/tests  (inclui o job num mini-cluster PyFlink local)
"""

import json
import os
import sys
from pathlib import Path

import pytest

JOB_DIR = Path(__file__).resolve().parents[1] / "job"
sys.path.insert(0, str(JOB_DIR))

from anchorage_logic import AnchorageTracker, Position, Zones, parse_position  # noqa: E402

# Zonas fixas de teste: as de produção (job/zones/santos.geojson) podem mudar sem quebrar os testes
TEST_ZONES = Path(__file__).resolve().parent / "zonas_teste.geojson"
ZONES = Zones.load(str(TEST_ZONES))
MIN = 60 * 1000
T0 = 1_792_000_000_000  # horário base qualquer (ms)

ANCHOR = (-24.10, -46.30)  # dentro do fundeadouro provisório
SEA = (-24.01, -46.32)     # entre o fundeadouro e o porto (canal)
PORT = (-23.95, -46.33)    # dentro do porto


def pos(minute, where, sog=0.1, nav=1, mmsi=111):
    lat, lon = where
    return Position(mmsi, T0 + minute * MIN, lat, lon, sog, nav)


def run(positions, tracker=None):
    tracker = tracker or AnchorageTracker()
    state, events = None, []
    for p in positions:
        current = (state or {}).get("session")
        zone = ZONES.zone_of(p.lat, p.lon, current=current["zone"] if current else None)
        evs, state, _ = tracker.process(state, p, zone)
        events += evs
    return events, state


def types(events):
    return [e["event_type"] for e in events]


def test_zonas_basicas():
    assert ZONES.zone_of(*ANCHOR) == ("fundeadouro_provisorio", "anchorage")
    assert ZONES.zone_of(*PORT) == ("porto_santos", "port")
    assert ZONES.zone_of(*SEA) is None


def test_espera_completa_ate_o_porto():
    seq = [pos(0, ANCHOR, sog=3.0, nav=0)]                          # entra navegando
    seq += [pos(m, ANCHOR) for m in range(5, 6 * 60, 3)]           # fundeado ~6 h
    seq += [pos(6 * 60 + 10, SEA, sog=8, nav=0)]                    # sai pelo canal
    seq += [pos(6 * 60 + 40, PORT, sog=4, nav=0)]                   # chega ao porto
    events, state = run(seq)
    assert types(events) == ["anchorage_enter", "wait_start", "anchorage_exit", "port_arrival"]
    enter, wait, exit_, arrival = events
    assert wait["ts_ms"] == T0 + 5 * MIN                            # espera começa quando parou
    assert exit_["wait_s"] == (6 * 60 + 10 - 5) * 60
    assert arrival["from_zone"] == "fundeadouro_provisorio"
    # espera como intervalo: último ponto dentro aos 6h-1min (range(5, 360, 3) termina em 359)
    assert exit_["last_inside_ts_ms"] == T0 + 359 * MIN
    assert exit_["wait_min_s"] == (359 - 5) * 60
    assert exit_["exit_gap_s"] == (6 * 60 + 10 - 359) * 60
    assert arrival["wait_min_s"] == exit_["wait_min_s"] and arrival["exit_gap_s"] == exit_["exit_gap_s"]
    # entrou navegando: início observado, não censurado
    assert exit_["start_censored"] is False and arrival["start_censored"] is False
    assert arrival["total_s"] == (6 * 60 + 40 - 5) * 60
    assert state["session"] is None and state["pending"] is None


def test_passagem_sem_parar_nao_gera_espera():
    seq = [pos(m, ANCHOR, sog=10, nav=0) for m in range(0, 30, 2)] + [pos(32, SEA, sog=10, nav=0), pos(50, PORT, sog=6, nav=0)]
    events, _ = run(seq)
    assert types(events) == ["anchorage_enter", "anchorage_exit"]   # sem wait_start nem port_arrival
    assert "wait_s" not in events[1]


def test_parado_menos_de_15_min_nao_conta():
    seq = [pos(0, ANCHOR, sog=0.2, nav=0), pos(10, ANCHOR, sog=0.2, nav=0), pos(12, ANCHOR, sog=5, nav=0), pos(40, SEA, sog=9, nav=0)]
    assert "wait_start" not in types(run(seq)[0])


def test_mensagem_repetida_ou_atrasada_e_ignorada():
    p1, p2 = pos(0, ANCHOR), pos(20, ANCHOR)
    events, state = run([p1, p2, p1, p2])
    assert types(events) == ["anchorage_enter", "wait_start"]


def test_tolerancia_na_borda_nao_pisca():
    borda_dentro = (-24.0305, -46.30)   # dentro, perto da borda norte (lat -24.03)
    borda_fora = (-24.0290, -46.30)     # ~170 m fora: ainda dentro da tolerância
    seq = [pos(0, borda_dentro), pos(5, borda_fora), pos(10, borda_dentro)]
    assert types(run(seq)[0]) == ["anchorage_enter"]


def test_sinal_perdido_encerra_sessao():
    tracker = AnchorageTracker()
    events, state = run([pos(0, ANCHOR), pos(30, ANCHOR)], tracker)
    timer = tracker._next_timer(state)
    assert timer == T0 + 30 * MIN + 6 * 3600 * 1000
    evs, state, nxt = tracker.on_timer(state, 111, timer)
    assert types(evs) == ["signal_lost"]
    assert evs[0]["wait_s"] == 30 * 60
    assert state["session"] is None and nxt is None


def test_parse_posicao_real():
    raw = json.dumps({
        "MessageType": "PositionReport",
        "MetaData": {"MMSI": 212775000, "time_utc": "2026-10-14 15:28:07.506871542 +0000 UTC"},
        "Message": {"PositionReport": {"Latitude": -23.97, "Longitude": -46.29, "Sog": 102.3, "NavigationalStatus": 1}},
    })
    p = parse_position(raw)
    assert p.mmsi == 212775000 and p.sog is None and p.stopped
    assert parse_position('{"MessageType":"ShipStaticData"}') is None


@pytest.mark.skipif(not os.environ.get("RUN_FLINK"), reason="defina RUN_FLINK=1 para rodar no mini-cluster PyFlink")
def test_job_no_minicluster():
    from pyflink.common import Types
    from pyflink.datastream import StreamExecutionEnvironment

    from anchorage_job import build_pipeline

    def raw(minute, where, sog, nav, mmsi):
        lat, lon = where
        from datetime import datetime, timezone
        t = datetime.fromtimestamp((T0 + minute * MIN) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f") + "000 +0000 UTC"
        return json.dumps({"MessageType": "PositionReport", "MetaData": {"MMSI": mmsi, "time_utc": t},
                           "Message": {"PositionReport": {"Latitude": lat, "Longitude": lon, "Sog": sog, "NavigationalStatus": nav}}})

    lines = [raw(0, ANCHOR, 3.0, 0, 111)]
    lines += [raw(m, ANCHOR, 0.1, 1, 111) for m in range(5, 120, 5)]
    lines += [raw(125, SEA, 8, 0, 111), raw(150, PORT, 4, 0, 111)]
    lines += [raw(0, ANCHOR, 0.1, 1, 222), raw(20, ANCHOR, 0.1, 1, 222)]   # 222 some: deve gerar signal_lost
    lines += [raw(m, PORT, 5, 0, 333) for m in range(0, 7 * 60, 30)]        # 333 só no porto; avança o watermark
    lines.sort(key=lambda s: json.loads(s)["MetaData"]["time_utc"])

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_python_executable(sys.executable)
    env.add_python_file(str(JOB_DIR))
    stream = env.from_collection(lines, type_info=Types.STRING())
    out = [json.loads(e) for e in build_pipeline(stream, str(TEST_ZONES)).execute_and_collect()]
    by_ship = {}
    for e in out:
        by_ship.setdefault(e["mmsi"], []).append(e["event_type"])
    assert by_ship[111] == ["anchorage_enter", "wait_start", "anchorage_exit", "port_arrival"]
    assert by_ship[222] == ["anchorage_enter", "wait_start", "signal_lost"]
    assert 333 not in by_ship


def test_ja_parado_no_primeiro_avistamento_e_censurado():
    events, _ = run([pos(0, ANCHOR), pos(20, ANCHOR), pos(40, SEA, sog=8, nav=0)])
    assert types(events) == ["anchorage_enter", "wait_start", "anchorage_exit"]
    assert events[1]["start_censored"] is True and events[2]["start_censored"] is True


def test_buraco_de_cobertura_vira_intervalo():
    # visto parado até o minuto 60; some; reaparece fora 10 h depois
    seq = [pos(0, ANCHOR, sog=2, nav=0), pos(5, ANCHOR), pos(60, ANCHOR), pos(660, SEA, sog=9, nav=0)]
    events, _ = run(seq)
    exit_ = events[-1]
    assert exit_["event_type"] == "anchorage_exit"
    assert exit_["wait_min_s"] == 55 * 60            # certeza: esperou pelo menos 55 min
    assert exit_["wait_s"] == 655 * 60               # no máximo ~10,9 h
    assert exit_["exit_gap_s"] == 600 * 60           # 10 h sem saber onde estava

