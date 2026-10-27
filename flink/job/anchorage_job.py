"""
Job PyFlink: ais.raw (Kafka) -> eventos de fundeadouro -> ais.events (Kafka).

    KafkaSource ais.raw
      -> parse (só posições válidas)
      -> watermark pelo horário do receptor (MetaData.time_utc), atraso tolerado de 5 min
      -> key_by(MMSI)
      -> AnchorageFunction (estado por navio + timers de horário do evento)
      -> KafkaSink ais.events (JSON, at-least-once)

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
from pyflink.datastream import KeyedProcessFunction, RuntimeContext, StreamExecutionEnvironment
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

from anchorage_logic import AnchorageTracker, Zones, parse_position

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
SOURCE_TOPIC = os.environ.get("SOURCE_TOPIC", "ais.raw")
EVENTS_TOPIC = os.environ.get("EVENTS_TOPIC", "ais.events")
ZONES_FILE = os.environ.get("ZONES_FILE", os.path.join(os.path.dirname(os.path.abspath(__file__)), "zones", "santos.geojson"))
CHECKPOINT_MS = int(os.environ.get("CHECKPOINT_MS", "60000"))


class PositionTimestamp(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return value.ts_ms


class AnchorageFunction(KeyedProcessFunction):
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

    def process_element(self, pos, ctx: "KeyedProcessFunction.Context"):
        state = self._load()
        session = (state or {}).get("session")
        zone = self.zones.zone_of(pos.lat, pos.lon, current=session["zone"] if session else None)
        events, new_state, next_timer = self.tracker.process(state, pos, zone)
        self._save(new_state, next_timer, ctx)
        for ev in events:
            yield json.dumps(ev, separators=(",", ":"))

    def on_timer(self, timestamp: int, ctx: "KeyedProcessFunction.OnTimerContext"):
        self.timer.clear()
        events, new_state, next_timer = self.tracker.on_timer(self._load(), ctx.get_current_key(), timestamp)
        self._save(new_state, next_timer, ctx)
        for ev in events:
            yield json.dumps(ev, separators=(",", ":"))


def build_pipeline(raw_stream, zones_file: str = ZONES_FILE):
    """raw (strings JSON do aisstream.io) -> eventos (strings JSON). Separado para testar sem Kafka."""
    watermarks = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(5))
        .with_timestamp_assigner(PositionTimestamp())
        .with_idleness(Duration.of_minutes(2))
    )
    return (
        raw_stream
        .map(parse_position)
        .filter(lambda p: p is not None)
        .assign_timestamps_and_watermarks(watermarks)
        .key_by(lambda p: p.mmsi, key_type=Types.LONG())
        .process(AnchorageFunction(zones_file), output_type=Types.STRING())
        .name("anchorage-events")
    )


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
    raw = env.from_source(source, WatermarkStrategy.no_watermarks(), "ais.raw")

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
    build_pipeline(raw).sink_to(sink).name("ais.events")
    env.execute("ais-anchorage-events")


if __name__ == "__main__":
    main()
