# Recalcula todos os eventos de fundeadouro do zero (ex.: depois de trocar as zonas).
#
#   1. para o vigia e cancela o job do Flink
#   2. arquiva os checkpoints (o estado antigo usa nomes de zona antigos)
#   3. volta o consumer group do Flink para o início do tópico ais.raw
#   4. esvazia a tabela ais.events no ClickHouse
#   5. sobe o vigia: sem checkpoint, o job relê todo o histórico que o Kafka ainda guarda (7 dias)
#
# Uso (na pasta do repo):  powershell -ExecutionPolicy Bypass -File scripts\reprocessar-eventos.ps1

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

function Step($msg) { Write-Host "`n==> $msg" -ForegroundColor Cyan }

Step "Parando o vigia (flink-job)"
docker compose stop flink-job

Step "Cancelando jobs em execução no Flink"
# (sem aspas duplas dentro do comando: o PowerShell 5 as remove ao chamar programas externos)
docker compose exec -T flink-jobmanager bash -c 'for j in $(flink list -r 2>/dev/null | grep -oE [0-9a-f]{32}); do echo cancelando $j; flink cancel $j; done'

Step "Arquivando checkpoints"
docker compose exec -T flink-jobmanager bash -c 'ts=$(date +%Y%m%d-%H%M%S); if [ -d /flink-data/checkpoints ]; then mv /flink-data/checkpoints /flink-data/checkpoints-arquivo-$ts; fi; mkdir -p /flink-data/checkpoints && chown flink:flink /flink-data/checkpoints && echo arquivado em /flink-data/checkpoints-arquivo-$ts'

Step "Voltando o consumer group flink-anchorage para o início de ais.raw"
Start-Sleep -Seconds 5
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group flink-anchorage --topic ais.raw --reset-offsets --to-earliest --execute

Step "Esvaziando ais.events no ClickHouse"
"TRUNCATE TABLE ais.events" | docker compose exec -T clickhouse bash -c 'clickhouse-client --user $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD'

Step "Subindo o vigia (primeira execução, sem checkpoint)"
docker compose up -d flink-job

Write-Host "`nPronto. Acompanhe com: docker compose logs -f flink-job" -ForegroundColor Green
