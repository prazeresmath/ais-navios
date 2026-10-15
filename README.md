# AIS Navios — streaming de navios em tempo real

Pipeline de streaming de ponta a ponta com posições AIS de navios no **Porto de Santos**:
os dados chegam por WebSocket, passam pelo **Kafka**, são processados com **Flink**,
guardados no **ClickHouse** e exibidos num **mapa ao vivo** com os navios se movendo.

> Projeto de portfólio em construção. Status das fases logo abaixo.

## Arquitetura

```
aisstream.io ─WebSocket─▶ Producer Python ─▶ Kafka (KRaft) ── tópico ais.raw
                                               ├─▶ FastAPI (última posição) ─WebSocket─▶ Mapa MapLibre
                                               ├─▶ ClickHouse (Kafka Engine + MV) ─▶ Grafana
                                               └─▶ Flink (eventos: fundeadouro, espera) ─▶ Kafka ─▶ ClickHouse
```

| Fase | Entrega | Status |
|---|---|---|
| 1 | Kafka + ClickHouse no Docker, producer publicando no tópico `ais.raw` | ✅ |
| 2 | Histórico no ClickHouse via Kafka Engine + Materialized View (bronze → silver) | ✅ |
| 3 | Mapa ao vivo: FastAPI (WebSocket) + MapLibre com interpolação | ✅ |
| 4 | Eventos no Flink: entrada/saída do fundeadouro, tempo de espera | ⏳ |
| 5 | Métricas no Grafana | ⏳ |

## Decisões

- **Fonte push (WebSocket), não polling** — streaming de verdade de ponta a ponta.
- **Recorte:** Porto de Santos + fundeadouro (`-24.20,-46.50` a `-23.85,-46.05`), para capturar a fila de navios esperando para atracar.
- **Kafka em KRaft** (sem ZooKeeper), nó único. O volume é baixo (~13 msg/min no recorte), então o Kafka está superdimensionado de propósito: o objetivo é demonstrar o padrão.
- **Tópico bruto imutável:** o producer publica o JSON original sem transformar. Chave = MMSI, o que garante ordem por navio dentro da partição.
- **Producer idempotente** (`acks=all`, `enable.idempotence=true`): retry não gera duplicata no tópico.
- **Cobertura AIS esparsa** em Santos: navios em movimento chegam a cada 60–90 s. Por isso o mapa vai interpolar a posição entre mensagens (dead reckoning).

## Modelo de dados (ClickHouse)

```
Kafka ais.raw ─▶ kafka_ais_raw (Kafka Engine, RawBLOB)
                   └─MV─▶ ais_raw            BRONZE  JSON original + partição/offset (TTL 30 dias)
                            ├─MV─▶ positions     SILVER  posições tipadas (ReplacingMergeTree por mmsi, ts)
                            └─MV─▶ ship_static   SILVER  nome, tipo, dimensões, destino (AggregatingMergeTree)
Views: ships · positions_dedup · latest_positions
```

- **Bronze imutável:** o JSON é guardado como chegou. Se o parse mudar, a silver é reprocessada a partir da bronze (`INSERT INTO ais.positions SELECT ... FROM ais.ais_raw`), sem depender da retenção do Kafka.
- **Deduplicação:** o Kafka Engine entrega *at-least-once*; a `positions` é `ReplacingMergeTree` com chave `(mmsi, ts)`, então uma mensagem reentregue some no merge (`positions_dedup` usa `FINAL` para leitura exata).
- **Dados estáticos em partes:** a classe B manda nome e dimensões em mensagens separadas. `ship_static` usa `anyLast`, que ignora `NULL` — uma mensagem parcial não apaga o que já se sabia do navio.
- **Valores "indisponível" do AIS viram `NULL`:** velocidade 102,3, rumo 360, proa 511, posição 91/181.
- Consultas de validação em [`clickhouse/queries/validacao.sql`](clickhouse/queries/validacao.sql).

## Mapa ao vivo (live-api)

```
Kafka ais.raw ─▶ live-api (FastAPI) ── estado em memória: última posição + rastro por navio
                    │  ao ligar: offsets_for_times(agora − 60 min) → relê a última hora
                    └─ WebSocket /ws ─▶ navegador (MapLibre): snapshot ao conectar + 1 msg por atualização
```

- **Sem banco no caminho ao vivo:** o mapa é alimentado só pelo stream; o ClickHouse fica para histórico e análise.
- **Warmup por tempo, não por commit:** cada execução usa um consumer group descartável e posiciona os offsets em *agora − 60 min*. Reiniciar o serviço nunca deixa o mapa vazio, e não há offset comprometido para gerenciar.
- **Dead reckoning no navegador:** entre mensagens (60–90 s em Santos), a posição é estimada por velocidade e rumo; quando a posição real chega, o ícone desliza até ela em 2 s em vez de pular. Extrapolação limitada a 3 min.
- **Mensagens fora de ordem:** posição com horário anterior à já conhecida é ignorada.
- Ícones coloridos por categoria com contorno branco (destacam das boias); filtro por categoria clicando na legenda.
- Mapa base: [OpenFreeMap](https://openfreemap.org) + camada náutica do [OpenSeaMap](https://www.openseamap.org) (boias, canais, fundeadouros).

## Como rodar

Pré-requisitos: Docker Desktop e uma API key gratuita do [aisstream.io](https://aisstream.io).

```bash
cp .env.example .env        # Windows: copy .env.example .env
# preencha AISSTREAM_API_KEY e CLICKHOUSE_PASSWORD no .env

docker compose up -d --build
docker compose logs -f producer
```

| Serviço | Endereço |
|---|---|
| **Mapa ao vivo** | http://localhost:8000 |
| Kafbat UI (tópicos e mensagens) | http://localhost:8080 |
| ClickHouse (console SQL) | http://localhost:8123/play |
| Kafka para clientes no host | `localhost:9094` |

Parar: `docker compose down` (mantém os dados) · `docker compose down -v` (apaga os dados).

## Estrutura

```
producer/            WebSocket aisstream.io -> Kafka
clickhouse/schema/   DDL aplicado pelo clickhouse-init a cada `up` (idempotente)
clickhouse/queries/  consultas de validação
live-api/            FastAPI: Kafka -> estado em memória -> WebSocket; serve o mapa (static/)
docker-compose.yml   Kafka (KRaft), kafka-init, Kafbat UI, ClickHouse, clickhouse-init, producer, live-api
.env.example         variáveis necessárias (sem valores)
```

## Formato da mensagem no tópico `ais.raw`

- **Chave:** MMSI do navio (string)
- **Valor:** JSON original do aisstream.io (`MessageType`, `MetaData`, `Message`)
- **Headers:** `message_type`, `source`, `ingested_at` (UTC)
