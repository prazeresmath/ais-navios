-- =============================================================================
-- Fase 2 — Kafka -> bronze
-- Criado POR ÚLTIMO: a tabela Kafka só começa a consumir quando a MV que lê
-- dela existe, e nessa hora todas as tabelas de destino já precisam existir.
-- =============================================================================

-- Consumidor do tópico. Não guarda dados: é uma "torneira" para o Kafka.
-- RawBLOB entrega a mensagem inteira numa única coluna String, sem parse:
-- o JSON é guardado como veio e o parse acontece na camada silver.
CREATE TABLE IF NOT EXISTS ais.kafka_ais_raw
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list   = 'kafka:9092',
    kafka_topic_list    = 'ais.raw',
    kafka_group_name    = 'clickhouse-ais-raw',
    kafka_format        = 'RawBLOB',
    kafka_num_consumers = 1;

-- Move cada lote consumido para a bronze, junto com os metadados do Kafka
-- (colunas virtuais _timestamp_ms, _partition, _offset, _key).
CREATE MATERIALIZED VIEW IF NOT EXISTS ais.mv_kafka_to_raw
TO ais.ais_raw
AS
SELECT
    ifNull(_timestamp_ms, now64(3, 'UTC'))   AS received_at,
    _partition                               AS kafka_partition,
    _offset                                  AS kafka_offset,
    toUInt32OrZero(_key)                     AS mmsi,
    JSONExtractString(raw, 'MessageType')    AS message_type,
    raw
FROM ais.kafka_ais_raw;
