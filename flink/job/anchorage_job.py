"""
Job PyFlink: ais.raw (Kafka) -> eventos de fundeadouro -> ais.events (Kafka).

    KafkaSource ais.raw
      -> parse (só posições válidas)
      -> watermark pelo horário do receptor (MetaData.time_utc), atraso tolerado de 5 min
      ├─> ZoneCoverageCounter (key_by zona): posições por hora em cada fundeadouro
      │     ├─> broadcast ─┐
      │     └─> JSON zone_coverage ───────────────────────┐
      └─> key_by(MMSI) ─> AnchorageFunction (estado por navio + timers de horário do evento,
                           lê a cobertura do broadcast state para decidir o signal_lost)
                                                          └─> KafkaSink ais.events (JSON, at-least-once)

A contagem de cobertura vai junto para ais.events (event_type = zone_coverage, mmsi = 0):
o ClickHouse a separa numa tabela própria e usa o mesmo número no critério de espera completa.

As regras estão em anchorage_logic.py (Python puro, testável sem cluster).

Variáveis de ambiente:
    KAFKA_BOOTSTRAP   padrão kafka:9092
    SOURCE_TOPIC      padrão ais.raw
    EVENTS_TOPIC      padrão ais.events
    ZONES_FILE        padrão /opt/job/zones/santos.geojson
    CHECKPOINT_MS     padrão 60000
"""

from __future__ import annotations

import json
import os

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import (
    FlatMapFunction,
    KeyedBroadcastProcessFunction,
    KeyedProcessFunction,
    RuntimeContext,
    StreamExecutionEnvironment,
)
from pyflink.datastream.state import MapStateDescriptor, StateTtlConfig, ValueStateDescriptor

from anchorage_logic import HOUR_MS, AnchorageTracker, Zones, hour_start, parse_position

# zona|início da hora (ms) -> posições na zona naquela hora; o mesmo mapa em todas as instâncias
COVERAGE_DESC = MapStateDescriptor("zone_coverage", Types.STRING(), Types.LONG())
COVERAGE_KEEP_MS = 4 * 24 * HOUR_MS  # guarda 4 dias (o teto do signal_lost é 48 h)
COVERAGE_TYPE = Types.TUPLE([Types.STRING(), Types.LONG(), Types.LONG()])  # (zona, hora, posições)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
SOURCE_TOPIC = os.environ.get("SOURCE_TOPIC", "ais.raw")
EVENTS_TOPIC = os.environ.get("EVENTS_TOPIC", "ais.events")
ZONES_FILE = os.environ.get("ZONES_FILE", os.path.join(os.path.dirname(os.path.abspath(__file__)), "zones", "santos.geojson"))
CHECKPOINT_MS = int(os.environ.get("CHECKPOINT_MS", "60000"))


class PositionTimestamp(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return value.ts_ms


class AnchorageZoneOf(FlatMapFunction):
    """Posição -> (fundeadouro, horário em ms); posições fora de fundeadouro somem.

    O horário vai no próprio registro: ctx.timestamp() não sobrevive a um operador Python no meio."""

    def __init__(self, zones_file: str):
        self.zones_file = zones_file

    def open(self, runtime_context: RuntimeContext):
        self.zones = Zones.load(self.zones_file)

    def flat_map(self, pos):
        zone = self.zones.zone_of(pos.lat, pos.lon)
        if zone and zone[1] == "anchorage":
            yield zone[0], pos.ts_ms


class ZoneCoverageCounter(KeyedProcessFunction):
    """Conta posições por hora (event time) em cada fundeadouro; emite (zona, hora, n) quando a hora fecha."""

    def open(self, runtime_context: RuntimeContext):
        self.counts = runtime_context.get_map_state(MapStateDescriptor("counts", Types.LONG(), Types.LONG()))

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        hour = hour_start(value[1])
        current = self.counts.get(hour) if self.counts.contains(hour) else 0
        if current == 0:
            # dispara quando o watermark passa do fim da hora: nenhuma posição dela chega depois
            ctx.timer_service().register_event_time_timer(hour + HOUR_MS - 1)
        self.counts.put(hour, current + 1)

    def on_timer(self, timestamp: int, ctx: "KeyedProcessFunction.OnTimerContext"):
        hour = timestamp + 1 - HOUR_MS
        if self.counts.contains(hour):
            yield ctx.get_current_key(), hour, self.counts.get(hour)
            self.counts.remove(hour)


def coverage_json(value) -> str:
    zone, hour, positions = value
    return json.dumps({"event_type": "zone_coverage", "mmsi": 0, "zone": zone, "ts_ms": hour,
                       "positions": positions}, separators=(",", ":"))


class AnchorageFunction(KeyedBroadcastProcessFunction):
    """Guarda o estado do navio como JSON e delega as decisões ao AnchorageTracker."""

    def __init__(self, zones_file: str):
        self.zones_file = zones_file

    def open(self, runtime_context: RuntimeContext):
        # Navio que some por 7 dias tem o estado descartado (evita crescer para sempre)
        ttl = StateTtlConfig.new_builder(Time.days(7)).build()
        state_desc = ValueStateDescriptor("tracker", Types.STRING())
        state_desc.enable_time_to_live(ttl)
        self.state = runtime_context.get_state(state_desc)
        self.timer = runtime_context.get_state(ValueStateDescriptor("timer", Types.LONG()))
        self.zones = Zones.load(self.zones_file)
        self.tracker = AnchorageTracker()

    def _load(self):
        raw = self.state.value()
        return json.loads(raw) if raw else None

    def _save(self, state, next_timer, ctx):
        self.state.update(json.dumps(state, separators=(",", ":")))
        current = self.timer.value()
        if current != next_timer:
            if current is not None:
                ctx.timer_service().delete_event_time_timer(current)
            if next_timer is not None:
                ctx.timer_service().register_event_time_timer(next_timer)
                self.timer.update(next_timer)
            else:
                self.timer.clear()

    def process_element(self, pos, ctx: "KeyedBroadcastProcessFunction.ReadOnlyContext"):
        state = self._load()
        session = (state or {}).get("session")
        zone = self.zones.zone_of(pos.lat, pos.lon, current=session["zone"] if session else None)
        events, new_state, next_timer = self.tracker.process(state, pos, zone)
        self._save(new_state, next_timer, ctx)
        for ev in events:
            yield json.dumps(ev, separators=(",", ":"))

    def process_broadcast_element(self, value, ctx: "KeyedBroadcastProcessFunction.Context"):
        zone, hour, positions = value
        coverage = ctx.get_broadcast_state(COVERAGE_DESC)
        coverage.put(f"{zone}|{hour}", positions)
        for key in list(coverage.keys()):
            if int(key.rsplit("|", 1)[1]) < hour - COVERAGE_KEEP_MS:
                coverage.remove(key)

    def on_timer(self, timestamp: int, ctx: "KeyedBroadcastProcessFunction.OnTimerContext"):
        self.timer.clear()
        coverage_state = ctx.get_broadcast_state(COVERAGE_DESC)

        def coverage(zone: str, hour: int) -> int:
            key = f"{zone}|{hour}"
            return coverage_state.get(key) if coverage_state.contains(key) else 0

        events, new_state, next_timer = self.tracker.on_timer(self._load(), ctx.get_current_key(), timestamp, coverage)
        self._save(new_state, next_timer, ctx)
        for ev in events:
            yield json.dumps(ev, separators=(",", ":"))


def build_pipeline(raw_stream, zones_file: str = ZONES_FILE):
    """raw (strings JSON do aisstream.io) -> eventos + cobertura (strings JSON). Separado para testar sem Kafka."""
    watermarks = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(5))
        .with_timestamp_assigner(PositionTimestamp())
        .with_idleness(Duration.of_minutes(2))
    )
    positions = (
        raw_stream
        .map(parse_position).name("parse").uid("parse")
        .filter(lambda p: p is not None).name("posicoes-validas").uid("posicoes-validas")
        .assign_timestamps_and_watermarks(watermarks).name("watermarks").uid("watermarks")
    )
    coverage = (
        positions
        .flat_map(AnchorageZoneOf(zones_file), output_type=Types.TUPLE([Types.STRING(), Types.LONG()])).name("zona-fundeadouro").uid("zona-fundeadouro")
        .key_by(lambda z: z[0], key_type=Types.STRING())
        .process(ZoneCoverageCounter(), output_type=COVERAGE_TYPE).name("cobertura-por-hora").uid("cobertura-por-hora")
    )
    events = (
        positions
        .key_by(lambda p: p.mmsi, key_type=Types.LONG())
        .connect(coverage.broadcast(COVERAGE_DESC))
        .process(AnchorageFunction(zones_file), output_type=Types.STRING())
        .name("anchorage-events").uid("anchorage-events")
    )
    return events.union(coverage.map(coverage_json, output_type=Types.STRING()).name("cobertura-json").uid("cobertura-json"))


def main():
    from pyflink.datastream.connectors.base import DeliveryGuarantee
    from pyflink.datastream.connectors.kafka import (
        KafkaOffsetResetStrategy,
        KafkaOffsetsInitializer,
        KafkaRecordSerializationSchema,
        KafkaSink,
        KafkaSource,
    )

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(CHECKPOINT_MS)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("flink-anchorage")
        # Primeira execução: relê tudo que o tópico ainda guarda (7 dias). Depois: retoma do checkpoint.
        .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST))
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    raw = env.from_source(source, WatermarkStrategy.no_watermarks(), "ais.raw").uid("source-ais-raw")

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(EVENTS_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        # At-least-once: em caso de falha, eventos podem repetir; a tabela no ClickHouse deduplica.
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    build_pipeline(raw).sink_to(sink).name("ais.events").uid("sink-ais-events")
    env.execute("ais-anchorage-events")


if __name__ == "__main__":
    main()
