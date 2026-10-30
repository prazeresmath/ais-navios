-- =============================================================================
-- Fase 4 — eventos de fundeadouro (gerados pelo Flink no tópico ais.events)
-- Idempotente. A tabela Kafka vem depois da tabela de destino, e a MV por último.
-- =============================================================================

-- Um evento por linha. O Flink entrega at-least-once: se reprocessar depois de
-- uma falha, o mesmo evento chega de novo — o ReplacingMergeTree por
-- (event_type, mmsi, ts) remove a duplicata no merge (ler com FINAL).
CREATE TABLE IF NOT EXISTS ais.events
(
    event_type      LowCardinality(String)            COMMENT 'anchorage_enter | wait_start | anchorage_exit | port_arrival | signal_lost',
    mmsi            UInt32,
    zone            LowCardinality(String)            COMMENT 'Zona do evento (fundeadouro ou porto)',
    ts              DateTime64(3, 'UTC')              COMMENT 'Horário do evento (horário do receptor AIS)',
    lat             Nullable(Float64),
    lon             Nullable(Float64),
    from_zone       LowCardinality(Nullable(String))  COMMENT 'port_arrival: fundeadouro de onde veio',
    enter_ts        Nullable(DateTime64(3, 'UTC'))    COMMENT 'Entrada no fundeadouro',
    wait_start_ts   Nullable(DateTime64(3, 'UTC'))    COMMENT 'Início da espera (parado >= 15 min)',
    exit_ts         Nullable(DateTime64(3, 'UTC'))    COMMENT 'port_arrival: saída do fundeadouro',
    duration_s      Nullable(UInt32)                  COMMENT 'Tempo total dentro do fundeadouro',
    wait_s          Nullable(UInt32)                  COMMENT 'Tempo de espera (do início da espera até a saída)',
    total_s         Nullable(UInt32)                  COMMENT 'port_arrival: do início da espera até chegar ao porto',
    last_inside_ts  Nullable(DateTime64(3, 'UTC'))    COMMENT 'Última vez visto dentro do fundeadouro',
    wait_min_s      Nullable(UInt32)                  COMMENT 'Espera mínima (até a última vez visto dentro)',
    exit_gap_s      Nullable(UInt32)                  COMMENT 'Buraco entre a última vez dentro e a primeira vez fora',
    start_censored  Nullable(Bool)                    COMMENT 'Já parado no primeiro avistamento: espera subestimada',
    received_at     DateTime64(3, 'UTC')              COMMENT 'Quando o ClickHouse leu o evento do Kafka'
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (event_type, mmsi, ts)
COMMENT 'Eventos de fundeadouro gerados pelo Flink (ais.events)';

-- Colunas da v2 (espera como intervalo e censura) para quem já tinha a tabela
ALTER TABLE ais.events ADD COLUMN IF NOT EXISTS last_inside_ts Nullable(DateTime64(3, 'UTC')) AFTER total_s;
ALTER TABLE ais.events ADD COLUMN IF NOT EXISTS wait_min_s     Nullable(UInt32)               AFTER last_inside_ts;
ALTER TABLE ais.events ADD COLUMN IF NOT EXISTS exit_gap_s     Nullable(UInt32)               AFTER wait_min_s;
ALTER TABLE ais.events ADD COLUMN IF NOT EXISTS start_censored Nullable(Bool)                 AFTER exit_gap_s;

CREATE TABLE IF NOT EXISTS ais.kafka_ais_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list   = 'kafka:9092',
    kafka_topic_list    = 'ais.events',
    kafka_group_name    = 'clickhouse-ais-events',
    kafka_format        = 'RawBLOB',
    kafka_num_consumers = 1;

-- A MV é recriada a cada execução para refletir colunas novas. Enquanto ela não
-- existe a tabela Kafka não consome (nada se perde: o offset não avança).
DROP VIEW IF EXISTS ais.mv_kafka_to_events;
CREATE MATERIALIZED VIEW ais.mv_kafka_to_events
TO ais.events
AS
SELECT
    JSONExtractString(raw, 'event_type')                                                         AS event_type,
    toUInt32(JSONExtractUInt(raw, 'mmsi'))                                                       AS mmsi,
    JSONExtractString(raw, 'zone')                                                               AS zone,
    fromUnixTimestamp64Milli(JSONExtractInt(raw, 'ts_ms'), 'UTC')                                AS ts,
    if(JSONHas(raw, 'lat'), JSONExtractFloat(raw, 'lat'), NULL)                                  AS lat,
    if(JSONHas(raw, 'lon'), JSONExtractFloat(raw, 'lon'), NULL)                                  AS lon,
    if(JSONHas(raw, 'from_zone'), JSONExtractString(raw, 'from_zone'), NULL)                     AS from_zone,
    if(JSONHas(raw, 'enter_ts_ms'), fromUnixTimestamp64Milli(JSONExtractInt(raw, 'enter_ts_ms'), 'UTC'), NULL)           AS enter_ts,
    if(JSONHas(raw, 'wait_start_ts_ms'), fromUnixTimestamp64Milli(JSONExtractInt(raw, 'wait_start_ts_ms'), 'UTC'), NULL) AS wait_start_ts,
    if(JSONHas(raw, 'exit_ts_ms'), fromUnixTimestamp64Milli(JSONExtractInt(raw, 'exit_ts_ms'), 'UTC'), NULL)             AS exit_ts,
    if(JSONHas(raw, 'duration_s'), toUInt32(JSONExtractUInt(raw, 'duration_s')), NULL)           AS duration_s,
    if(JSONHas(raw, 'wait_s'), toUInt32(JSONExtractUInt(raw, 'wait_s')), NULL)                   AS wait_s,
    if(JSONHas(raw, 'total_s'), toUInt32(JSONExtractUInt(raw, 'total_s')), NULL)                 AS total_s,
    if(JSONHas(raw, 'last_inside_ts_ms'), fromUnixTimestamp64Milli(JSONExtractInt(raw, 'last_inside_ts_ms'), 'UTC'), NULL) AS last_inside_ts,
    if(JSONHas(raw, 'wait_min_s'), toUInt32(JSONExtractUInt(raw, 'wait_min_s')), NULL)           AS wait_min_s,
    if(JSONHas(raw, 'exit_gap_s'), toUInt32(JSONExtractUInt(raw, 'exit_gap_s')), NULL)           AS exit_gap_s,
    if(JSONHas(raw, 'start_censored'), JSONExtractBool(raw, 'start_censored') = 1, NULL)         AS start_censored,
    now64(3, 'UTC')                                                                              AS received_at
FROM ais.kafka_ais_events
WHERE JSONHas(raw, 'event_type') AND JSONHas(raw, 'ts_ms');


-- -----------------------------------------------------------------------------
-- Views de leitura (base para a Fase 5 / Grafana)
-- -----------------------------------------------------------------------------

-- Uma linha por espera concluída: quanto tempo o navio ficou parado no
-- fundeadouro e, se chegou ao porto, quanto tempo levou no total.
CREATE OR REPLACE VIEW ais.anchorage_waits AS
SELECT
    e.mmsi                                  AS mmsi,
    s.name                                  AS name,
    if(empty(s.category), 'Desconhecido', s.category) AS category,
    s.length_m                              AS length_m,
    e.zone                                  AS zone,
    e.enter_ts                              AS enter_ts,
    e.wait_start_ts                         AS wait_start_ts,
    e.last_inside_ts                        AS last_inside_ts,
    e.ts                                    AS exit_ts,
    e.wait_min_s                            AS wait_min_s,
    e.wait_s                                AS wait_max_s,
    round(e.wait_min_s / 3600, 2)           AS wait_min_h,
    round(e.wait_s / 3600, 2)               AS wait_max_h,
    round(e.exit_gap_s / 3600, 2)           AS exit_gap_h,
    ifNull(e.start_censored, false)         AS start_censored,
    -- Espera "boa" para métricas: início observado e saída vista com no máximo 1 h de buraco
    NOT ifNull(e.start_censored, false) AND ifNull(e.exit_gap_s, 0) <= 3600 AS complete,
    a.arrival_ts                            AS port_arrival_ts,
    a.total_s                               AS total_s
FROM
(
    SELECT mmsi, zone, enter_ts, wait_start_ts, last_inside_ts, ts, wait_s, wait_min_s, exit_gap_s, start_censored
    FROM ais.events FINAL
    WHERE event_type = 'anchorage_exit' AND wait_s IS NOT NULL
) AS e
LEFT JOIN
(
    SELECT mmsi, wait_start_ts, ts AS arrival_ts, total_s
    FROM ais.events FINAL
    WHERE event_type = 'port_arrival'
) AS a ON a.mmsi = e.mmsi AND a.wait_start_ts = e.wait_start_ts
LEFT JOIN ais.ships AS s ON s.mmsi = e.mmsi;

-- Navios esperando agora: o último evento de fundeadouro do navio é wait_start.
-- O rank desempata eventos com o mesmo horário (entrou e já estava parado).
CREATE OR REPLACE VIEW ais.waiting_now AS
SELECT
    w.mmsi                                                     AS mmsi,
    s.name                                                     AS name,
    if(empty(s.category), 'Desconhecido', s.category)          AS category,
    w.zone                                                     AS zone,
    w.wait_start_ts                                            AS wait_start_ts,
    dateDiff('minute', w.wait_start_ts, now64(3, 'UTC'))       AS waiting_min,
    w.start_censored                                           AS start_censored
FROM
(
    SELECT
        mmsi,
        argMax(event_type, (ts, multiIf(event_type = 'anchorage_enter', 1, event_type = 'wait_start', 2, 3))) AS last_event,
        argMax(zone, ts)                                       AS zone,
        maxIf(ts, event_type = 'wait_start')                   AS wait_start_ts,
        ifNull(argMaxIf(start_censored, ts, event_type = 'wait_start'), false) AS start_censored
    FROM ais.events FINAL
    WHERE event_type != 'port_arrival'
    GROUP BY mmsi
) AS w
LEFT JOIN ais.ships AS s ON s.mmsi = w.mmsi
WHERE w.last_event = 'wait_start';
