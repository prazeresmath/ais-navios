-- =============================================================================
-- Fase 2 — tabelas de destino (bronze e silver)
-- Idempotente: pode rodar a cada `docker compose up`.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS ais;

-- -----------------------------------------------------------------------------
-- BRONZE: cada mensagem do tópico ais.raw, exatamente como chegou.
-- Serve para auditoria e para reprocessar a camada silver se o parse mudar.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ais.ais_raw
(
    received_at     DateTime64(3, 'UTC')    COMMENT 'Timestamp do registro no Kafka',
    kafka_partition UInt16,
    kafka_offset    UInt64,
    mmsi            UInt32                  COMMENT 'Chave da mensagem no Kafka',
    message_type    LowCardinality(String),
    raw             String COMMENT 'JSON original do aisstream.io' CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(received_at)
ORDER BY (message_type, mmsi, received_at)
TTL toDateTime(received_at) + INTERVAL 30 DAY DELETE
COMMENT 'Bronze: mensagens AIS brutas (TTL 30 dias)';

-- -----------------------------------------------------------------------------
-- SILVER: posições tipadas (classe A e classe B).
-- ReplacingMergeTree por (mmsi, ts): se o Kafka entregar a mesma mensagem duas
-- vezes (entrega at-least-once), a duplicata some no merge. Para leitura exata
-- antes do merge, usar FINAL ou a view ais.positions_dedup.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ais.positions
(
    mmsi          UInt32,
    ts            DateTime64(6, 'UTC')     COMMENT 'Horário em que o receptor captou a mensagem (MetaData.time_utc)',
    received_at   DateTime64(3, 'UTC')     COMMENT 'Horário em que chegou ao Kafka',
    ais_class     LowCardinality(String)   COMMENT 'A ou B',
    lat           Float64,
    lon           Float64,
    sog           Nullable(Float32)        COMMENT 'Velocidade sobre o fundo, nós (NULL = indisponível)',
    cog           Nullable(Float32)        COMMENT 'Rumo sobre o fundo, graus (NULL = indisponível)',
    heading       Nullable(UInt16)         COMMENT 'Proa verdadeira, graus (NULL = indisponível)',
    nav_status    Nullable(UInt8)          COMMENT 'Status de navegação (só classe A: 1 = fundeado, 5 = atracado)',
    ship_name     String                   COMMENT 'Nome informado no MetaData (pode vir vazio)'
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (mmsi, ts)
COMMENT 'Silver: posições AIS tipadas, sem duplicatas após merge';

-- -----------------------------------------------------------------------------
-- SILVER: dados estáticos por navio (nome, tipo, dimensões, destino).
-- Chegam em partes (classe B manda nome e dimensões em mensagens separadas),
-- por isso AggregatingMergeTree com anyLast: ele ignora NULL, então uma mensagem
-- parcial não apaga o que já se sabia do navio.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ais.ship_static
(
    mmsi         UInt32,
    updated_at   SimpleAggregateFunction(max,     DateTime64(3, 'UTC')),
    ais_class    SimpleAggregateFunction(anyLast, Nullable(String)),
    name         SimpleAggregateFunction(anyLast, Nullable(String)),
    call_sign    SimpleAggregateFunction(anyLast, Nullable(String)),
    imo          SimpleAggregateFunction(anyLast, Nullable(UInt32)),
    ship_type    SimpleAggregateFunction(anyLast, Nullable(UInt8)),
    length_m     SimpleAggregateFunction(anyLast, Nullable(UInt16)),
    width_m      SimpleAggregateFunction(anyLast, Nullable(UInt16)),
    draught_m    SimpleAggregateFunction(anyLast, Nullable(Float32)),
    destination  SimpleAggregateFunction(anyLast, Nullable(String)),
    eta          SimpleAggregateFunction(anyLast, Nullable(String))
)
ENGINE = AggregatingMergeTree
ORDER BY mmsi
COMMENT 'Silver: atributos estáticos por navio (consolidados com anyLast)';
