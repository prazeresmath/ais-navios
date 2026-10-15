-- =============================================================================
-- Consultas de validação da Fase 2
-- Rodar no console SQL: http://localhost:8123/play
-- =============================================================================

-- 1) Está chegando? Mensagens na bronze por tipo e a mais recente
SELECT message_type, count() AS mensagens, max(received_at) AS ultima
FROM ais.ais_raw
GROUP BY message_type
ORDER BY mensagens DESC;

-- 2) O consumidor do Kafka está saudável? (erros, último lote, mensagens lidas)
SELECT table, consumer_id, num_messages_read, last_poll_time, last_commit_time,
       exceptions.text AS erros
FROM system.kafka_consumers
WHERE database = 'ais';

-- 3) Nenhuma mensagem perdida entre bronze e silver?
--    (posições na bronze x posições na silver — devem bater, menos as inválidas)
SELECT
    (SELECT count() FROM ais.ais_raw
      WHERE message_type IN ('PositionReport','StandardClassBPositionReport','ExtendedClassBPositionReport')) AS bronze,
    (SELECT count() FROM ais.positions_dedup) AS silver_sem_duplicata,
    (SELECT count() FROM ais.positions)       AS silver_bruto;

-- 4) Mensagens por minuto na última hora
SELECT toStartOfMinute(received_at) AS minuto, count() AS mensagens, uniqExact(mmsi) AS navios
FROM ais.ais_raw
WHERE received_at > now() - INTERVAL 1 HOUR
GROUP BY minuto
ORDER BY minuto;

-- 5) Última posição de cada navio (o que o mapa vai mostrar)
SELECT mmsi, name, category, round(lat, 4) AS lat, round(lon, 4) AS lon,
       sog, heading, nav_status, ts
FROM ais.latest_positions
ORDER BY ts DESC;

-- 6) Navios conhecidos por categoria
SELECT category, count() AS navios
FROM ais.ships
GROUP BY category
ORDER BY navios DESC;

-- 7) Navios fundeados agora (nav_status 1 = fundeado) — prévia da métrica de espera
SELECT mmsi, name, category, length_m, destination, ts
FROM ais.latest_positions
WHERE nav_status = 1 OR (sog IS NOT NULL AND sog < 0.5)
ORDER BY ts DESC;

-- 8) Trajetória de um navio (troque o MMSI)
SELECT ts, lat, lon, sog, cog, heading
FROM ais.positions_dedup
WHERE mmsi = 352002805
ORDER BY ts;

-- 9) Espaço em disco por tabela
SELECT table, formatReadableSize(sum(bytes_on_disk)) AS disco, sum(rows) AS linhas
FROM system.parts
WHERE database = 'ais' AND active
GROUP BY table;
