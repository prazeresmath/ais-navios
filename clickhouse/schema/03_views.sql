-- =============================================================================
-- Fase 2 — views de leitura
-- =============================================================================

-- Um registro por navio, com os atributos estáticos consolidados
CREATE OR REPLACE VIEW ais.ships AS
SELECT
    mmsi,
    max(updated_at)        AS updated_at,
    anyLast(ais_class)     AS ais_class,
    anyLast(name)          AS name,
    anyLast(call_sign)     AS call_sign,
    anyLast(imo)           AS imo,
    anyLast(ship_type)     AS ship_type,
    -- Categoria a partir do código de tipo AIS (ITU-R M.1371)
    multiIf(
        ship_type IS NULL,                 'Desconhecido',
        ship_type = 30,                    'Pesca',
        ship_type IN (31, 32, 52),         'Rebocador',
        ship_type IN (33, 34, 35),         'Serviço',
        ship_type IN (36, 37),             'Recreio',
        ship_type BETWEEN 40 AND 49,       'Alta velocidade',
        ship_type = 50,                    'Prático',
        ship_type BETWEEN 51 AND 59,       'Serviço',
        ship_type BETWEEN 60 AND 69,       'Passageiros',
        ship_type BETWEEN 70 AND 79,       'Carga',
        ship_type BETWEEN 80 AND 89,       'Tanque',
                                           'Outros'
    )                      AS category,
    anyLast(length_m)      AS length_m,
    anyLast(width_m)       AS width_m,
    anyLast(draught_m)     AS draught_m,
    anyLast(destination)   AS destination,
    anyLast(eta)           AS eta
FROM ais.ship_static
GROUP BY mmsi;

-- Posições sem duplicatas mesmo antes do merge do ReplacingMergeTree
CREATE OR REPLACE VIEW ais.positions_dedup AS
SELECT * FROM ais.positions FINAL;

-- Última posição conhecida de cada navio, com os dados estáticos
CREATE OR REPLACE VIEW ais.latest_positions AS
SELECT
    p.mmsi                                   AS mmsi,
    coalesce(s.name, nullIf(p.last_ship_name, '')) AS name,
    if(empty(s.category), 'Desconhecido', s.category) AS category,
    s.length_m                               AS length_m,
    s.destination                            AS destination,
    p.last_ts                                AS ts,
    p.last_lat                               AS lat,
    p.last_lon                               AS lon,
    p.last_sog                               AS sog,
    p.last_cog                               AS cog,
    p.last_heading                           AS heading,
    p.last_nav_status                        AS nav_status,
    p.last_ais_class                         AS ais_class
FROM
(
    SELECT
        mmsi,
        max(ts)                     AS last_ts,
        argMax(lat, ts)             AS last_lat,
        argMax(lon, ts)             AS last_lon,
        argMax(sog, ts)             AS last_sog,
        argMax(cog, ts)             AS last_cog,
        argMax(heading, ts)         AS last_heading,
        argMax(nav_status, ts)      AS last_nav_status,
        argMax(ais_class, ts)       AS last_ais_class,
        argMax(ship_name, ts)       AS last_ship_name
    FROM ais.positions
    GROUP BY mmsi
) AS p
LEFT JOIN ais.ships AS s ON s.mmsi = p.mmsi;
