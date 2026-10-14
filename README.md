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
| 2 | Histórico no ClickHouse via Kafka Engine + Materialized View | ⏳ |
| 3 | Mapa ao vivo: FastAPI (WebSocket) + MapLibre com interpolação | ⏳ |
| 4 | Eventos no Flink: entrada/saída do fundeadouro, tempo de espera | ⏳ |
| 5 | Métricas no Grafana | ⏳ |

## Decisões

- **Fonte push (WebSocket), não polling** — streaming de verdade de ponta a ponta.
- **Recorte:** Porto de Santos + fundeadouro (`-24.20,-46.50` a `-23.85,-46.05`), para capturar a fila de navios esperando para atracar.
- **Kafka em KRaft** (sem ZooKeeper), nó único. O volume é baixo (~13 msg/min no recorte), então o Kafka está superdimensionado de propósito: o objetivo é demonstrar o padrão.
- **Tópico bruto imutável:** o producer publica o JSON original sem transformar. Chave = MMSI, o que garante ordem por navio dentro da partição.
- **Producer idempotente** (`acks=all`, `enable.idempotence=true`): retry não gera duplicata no tópico.
- **Cobertura AIS esparsa** em Santos: navios em movimento chegam a cada 60–90 s. Por isso o mapa vai interpolar a posição entre mensagens (dead reckoning).

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
| Kafbat UI (tópicos e mensagens) | http://localhost:8080 |
| ClickHouse (console SQL) | http://localhost:8123/play |
| Kafka para clientes no host | `localhost:9094` |

Parar: `docker compose down` (mantém os dados) · `docker compose down -v` (apaga os dados).

## Estrutura

```
producer/            WebSocket aisstream.io -> Kafka
docker-compose.yml   Kafka (KRaft), kafka-init (cria tópicos), Kafbat UI, ClickHouse, producer
.env.example         variáveis necessárias (sem valores)
```

## Formato da mensagem no tópico `ais.raw`

- **Chave:** MMSI do navio (string)
- **Valor:** JSON original do aisstream.io (`MessageType`, `MetaData`, `Message`)
- **Headers:** `message_type`, `source`, `ingested_at` (UTC)
