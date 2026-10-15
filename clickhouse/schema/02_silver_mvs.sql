-- =============================================================================
-- Fase 2 — bronze -> silver
-- As MVs leem de ais.ais_raw (não do Kafka). Assim, para reprocessar depois de
-- mudar o parse basta: INSERT INTO ais.positions SELECT ... FROM ais.ais_raw.
-- =============================================================================

-- Posições: classe A (PositionReport) e classe B (Standard/Extended)
CREATE MATERIALIZED VIEW IF NOT EXISTS ais.mv_raw_to_positions
TO ais.positions
AS
WITH
    JSONExtractRaw(raw, 'Message', message_type)                AS body,
    JSONExtractString(raw, 'MetaData', 'time_utc')              AS time_utc,
    JSONExtractFloat(body, 'Latitude')                          AS lat_raw,
    JSONExtractFloat(body, 'Longitude')                         AS lon_raw,
    JSONExtractFloat(body, 'Sog')                               AS sog_raw,
    JSONExtractFloat(body, 'Cog')                               AS cog_raw,
    JSONExtractUInt(body, 'TrueHeading')                        AS heading_raw
SELECT
    mmsi,
    -- '2026-10-14 15:28:07.506871542 +0000 UTC' -> os 26 primeiros caracteres (microssegundos)
    ifNull(parseDateTime64BestEffortOrNull(substring(time_utc, 1, 26), 6, 'UTC'),
           toDateTime64(received_at, 6, 'UTC'))                 AS ts,
    received_at,
    if(message_type = 'PositionReport', 'A', 'B')               AS ais_class,
    lat_raw                                                     AS lat,
    lon_raw                                                     AS lon,
    -- Valores "não disponível" do padrão AIS viram NULL
    if(sog_raw >= 102.3, NULL, toFloat32(sog_raw))              AS sog,
    if(cog_raw >= 360,   NULL, toFloat32(cog_raw))              AS cog,
    if(heading_raw >= 360, NULL, toUInt16(heading_raw))         AS heading,
    if(message_type = 'PositionReport',
       toUInt8(JSONExtractUInt(body, 'NavigationalStatus')), NULL) AS nav_status,
    trimBoth(JSONExtractString(raw, 'MetaData', 'ShipName'))    AS ship_name
FROM ais.ais_raw
WHERE message_type IN ('PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport')
  -- 91 / 181 = posição indisponível no padrão AIS
  AND lat_raw BETWEEN -90 AND 90
  AND lon_raw BETWEEN -180 AND 180;


-- Dados estáticos classe A (ShipStaticData — mensagem 5)
CREATE MATERIALIZED VIEW IF NOT EXISTS ais.mv_raw_to_ship_static_a
TO ais.ship_static
AS
WITH
    JSONExtractRaw(raw, 'Message', 'ShipStaticData')            AS body,
    JSONExtractRaw(body, 'Dimension')                           AS dim,
    JSONExtractRaw(body, 'Eta')                                 AS eta_raw
SELECT
    mmsi,
    received_at                                                                  AS updated_at,
    'A'                                                                          AS ais_class,
    nullIf(trimBoth(JSONExtractString(body, 'Name')), '')                        AS name,
    nullIf(trimBoth(JSONExtractString(body, 'CallSign')), '')                    AS call_sign,
    nullIf(toUInt32(JSONExtractUInt(body, 'ImoNumber')), 0)                      AS imo,
    nullIf(toUInt8(JSONExtractUInt(body, 'Type')), 0)                            AS ship_type,
    nullIf(toUInt16(JSONExtractUInt(dim, 'A') + JSONExtractUInt(dim, 'B')), 0)   AS length_m,
    nullIf(toUInt16(JSONExtractUInt(dim, 'C') + JSONExtractUInt(dim, 'D')), 0)   AS width_m,
    nullIf(toFloat32(JSONExtractFloat(body, 'MaximumStaticDraught')), 0)         AS draught_m,
    nullIf(trimBoth(JSONExtractString(body, 'Destination')), '')                 AS destination,
    -- ETA no AIS não tem ano: guardamos como 'MM-DD HH:MM' (mês 0 = não informado)
    if(JSONExtractUInt(eta_raw, 'Month') = 0, NULL,
       format('{}-{} {}:{}',
              leftPad(toString(JSONExtractUInt(eta_raw, 'Month')), 2, '0'),
              leftPad(toString(JSONExtractUInt(eta_raw, 'Day')), 2, '0'),
              leftPad(toString(JSONExtractUInt(eta_raw, 'Hour')), 2, '0'),
              leftPad(toString(JSONExtractUInt(eta_raw, 'Minute')), 2, '0')))    AS eta
FROM ais.ais_raw
WHERE message_type = 'ShipStaticData';


-- Dados estáticos classe B (StaticDataReport — mensagem 24, em duas partes)
-- Parte A traz só o nome, parte B traz indicativo, tipo e dimensões.
CREATE MATERIALIZED VIEW IF NOT EXISTS ais.mv_raw_to_ship_static_b
TO ais.ship_static
AS
WITH
    JSONExtractRaw(raw, 'Message', 'StaticDataReport')          AS body,
    JSONExtractRaw(body, 'ReportA')                             AS ra,
    JSONExtractRaw(body, 'ReportB')                             AS rb,
    JSONExtractBool(ra, 'Valid')                                AS a_ok,
    JSONExtractBool(rb, 'Valid')                                AS b_ok,
    JSONExtractRaw(rb, 'Dimension')                             AS dim
SELECT
    mmsi,
    received_at                                                                          AS updated_at,
    'B'                                                                                  AS ais_class,
    if(a_ok, nullIf(trimBoth(JSONExtractString(ra, 'Name')), ''), NULL)                  AS name,
    if(b_ok, nullIf(trimBoth(JSONExtractString(rb, 'CallSign')), ''), NULL)              AS call_sign,
    CAST(NULL AS Nullable(UInt32))                                                       AS imo,
    if(b_ok, nullIf(toUInt8(JSONExtractUInt(rb, 'ShipType')), 0), NULL)                  AS ship_type,
    if(b_ok, nullIf(toUInt16(JSONExtractUInt(dim, 'A') + JSONExtractUInt(dim, 'B')), 0), NULL) AS length_m,
    if(b_ok, nullIf(toUInt16(JSONExtractUInt(dim, 'C') + JSONExtractUInt(dim, 'D')), 0), NULL) AS width_m,
    CAST(NULL AS Nullable(Float32))                                                      AS draught_m,
    CAST(NULL AS Nullable(String))                                                       AS destination,
    CAST(NULL AS Nullable(String))                                                       AS eta
FROM ais.ais_raw
WHERE message_type = 'StaticDataReport'
  AND (a_ok OR b_ok);
