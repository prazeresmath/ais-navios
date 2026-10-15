"""
live-api: Kafka (ais.raw) -> estado em memória -> WebSocket -> mapa no navegador.

Ao ligar, volta os offsets para WARMUP_MINUTES atrás (offsets_for_times) e relê o
tópico para montar o estado; depois segue consumindo ao vivo. Não usa o ClickHouse:
o mapa é alimentado só pelo stream.

Endpoints:
    GET  /            mapa (static/index.html)
    GET  /api/ships   foto atual (JSON)
    GET  /health      status do consumidor
    WS   /ws          snapshot ao conectar + uma mensagem por atualização
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from confluent_kafka import OFFSET_END, Consumer, KafkaError, KafkaException, TopicPartition
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from ais_state import ShipState

log = logging.getLogger("live-api")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s | %(message)s")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "ais.raw")
WARMUP_MINUTES = float(os.environ.get("WARMUP_MINUTES", "60"))
SHIP_MAX_AGE_H = float(os.environ.get("SHIP_MAX_AGE_H", "6"))
STATIC_DIR = Path(__file__).parent / "static"


class Hub:
    """Clientes WebSocket conectados."""

    def __init__(self):
        self.clients: set[WebSocket] = set()

    async def broadcast(self, payload: dict) -> None:
        if not self.clients:
            return
        text = json.dumps(payload, separators=(",", ":"))
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_text(text)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)


state = ShipState(max_age_s=SHIP_MAX_AGE_H * 3600)
hub = Hub()
status = {"warmup_done": False, "connected": False, "last_message_at": None, "error": None}


def build_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            # Grupo único por execução: o offset é decidido pelo warmup, não por commit
            "group.id": f"live-api-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
            "client.id": "live-api",
        }
    )


def assign_from_warmup(consumer: Consumer) -> dict[int, int]:
    """
    Posiciona cada partição em (agora - WARMUP_MINUTES).
    Devolve {partição: último offset a reler} só das partições que têm o que reler.
    """
    meta = consumer.list_topics(KAFKA_TOPIC, timeout=10)
    topic = meta.topics.get(KAFKA_TOPIC)
    if topic is None or topic.error is not None or not topic.partitions:
        raise KafkaException(KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART, f"tópico {KAFKA_TOPIC} indisponível"))
    since_ms = int((time.time() - WARMUP_MINUTES * 60) * 1000)
    wanted = [TopicPartition(KAFKA_TOPIC, p, since_ms) for p in topic.partitions]
    found = consumer.offsets_for_times(wanted, timeout=10)
    assigned, to_replay = [], {}
    for tp in found:
        _, high = consumer.get_watermark_offsets(TopicPartition(KAFKA_TOPIC, tp.partition), timeout=10)
        if 0 <= tp.offset < high:
            to_replay[tp.partition] = high - 1
            assigned.append(TopicPartition(KAFKA_TOPIC, tp.partition, tp.offset))
        else:
            # -1 = nenhuma mensagem depois do instante pedido -> começa do fim
            assigned.append(TopicPartition(KAFKA_TOPIC, tp.partition, OFFSET_END))
    consumer.assign(assigned)
    log.info("Warmup: relendo %.0f min do tópico %s (%d partições)", WARMUP_MINUTES, KAFKA_TOPIC, len(assigned))
    return to_replay


async def finish_warmup() -> None:
    status["warmup_done"] = True
    log.info("Warmup concluído: %d navios no estado.", len(state.snapshot()))
    await hub.broadcast({"type": "snapshot", "ships": state.snapshot(), "warmup_done": True})


async def consume_loop() -> None:
    loop = asyncio.get_running_loop()
    while True:
        consumer = build_consumer()
        try:
            pending = await loop.run_in_executor(None, assign_from_warmup, consumer)
            status.update(connected=True, error=None, warmup_done=False)
            last_prune = time.time()
            while True:
                msg = await loop.run_in_executor(None, consumer.poll, 1.0)
                if time.time() - last_prune > 60:
                    gone = state.prune()
                    last_prune = time.time()
                    if gone:
                        await hub.broadcast({"type": "remove", "mmsi": gone})
                if msg is None:
                    # Poll vazio = alcançou o fim do que havia para reler
                    if not status["warmup_done"]:
                        await finish_warmup()
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.warning("Erro do Kafka: %s", msg.error())
                    continue
                status["last_message_at"] = time.time()
                ship = state.apply(msg.value())
                if not status["warmup_done"]:
                    p = msg.partition()
                    if p in pending and msg.offset() >= pending[p]:
                        del pending[p]
                    if not pending:
                        await finish_warmup()
                    continue  # durante o warmup não transmite mensagem a mensagem
                if ship is not None and ship.lat is not None:
                    await hub.broadcast({"type": "update", "ship": ship.to_dict()})
        except asyncio.CancelledError:
            consumer.close()
            raise
        except Exception as e:  # Kafka fora do ar, tópico ainda não criado etc.
            status.update(connected=False, error=str(e))
            log.warning("Consumidor parou (%s). Tentando de novo em 5 s...", e)
            try:
                consumer.close()
            except Exception:
                pass
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(consume_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="AIS Navios — live-api", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/ships")
async def ships():
    return JSONResponse({"ships": state.snapshot(), "warmup_done": status["warmup_done"]})


@app.get("/health")
async def health():
    return {
        **status,
        "ships": len(state.snapshot()),
        "messages": state.messages,
        "clients": len(hub.clients),
        "topic": KAFKA_TOPIC,
    }


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    hub.clients.add(ws)
    try:
        await ws.send_text(json.dumps({"type": "snapshot", "ships": state.snapshot(),
                                       "warmup_done": status["warmup_done"]}, separators=(",", ":")))
        while True:
            await ws.receive_text()  # o cliente não manda nada; isso só detecta desconexão
    except WebSocketDisconnect:
        pass
    finally:
        hub.clients.discard(ws)
