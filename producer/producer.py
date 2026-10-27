"""
Producer AIS: aisstream.io (WebSocket) -> Kafka.

Lê as mensagens AIS do recorte configurado e publica cada uma, sem alteração,
no tópico Kafka (camada bruta / bronze).

- Valor:   o JSON original do aisstream.io, byte a byte (nada é transformado aqui)
- Chave:   MMSI do navio -> todas as mensagens de um navio caem na mesma partição,
           o que garante a ordem por navio para quem consome
- Headers: message_type, source, ingested_at (metadados da ingestão)

Configuração por variável de ambiente (ou .env ao lado do docker-compose):
    AISSTREAM_API_KEY   obrigatória
    AIS_BBOX            lat_min,lon_min,lat_max,lon_max  (padrão: Santos + fundeadouro)
    KAFKA_BOOTSTRAP     padrão kafka:9092 (dentro do Docker); localhost:9094 fora dele
    KAFKA_TOPIC         padrão ais.raw
    AISSTREAM_URL       padrão wss://stream.aisstream.io/v0/stream
    STATS_INTERVAL_S    padrão 60
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
from collections import Counter
from datetime import datetime, timezone

import websockets
from confluent_kafka import KafkaException, Producer

try:
    from dotenv import load_dotenv

    load_dotenv()  # útil ao rodar fora do Docker; no Docker as variáveis já vêm do compose
except ImportError:
    pass

log = logging.getLogger("ais-producer")

DEFAULT_BBOX = "-24.30,-46.50,-23.85,-46.05"
# Tipos que não descrevem navio nenhum e não devem ir para o tópico
IGNORED_TYPES = {"SubscriptionConfirmation"}


# ---------------------------------------------------------------- configuração


def parse_bbox(raw: str) -> list[list[float]]:
    """'lat_min,lon_min,lat_max,lon_max' -> [[lat_min, lon_min], [lat_max, lon_max]]."""
    parts = [float(p) for p in raw.split(",")]
    if len(parts) != 4:
        raise ValueError(f"AIS_BBOX precisa de 4 números, veio: {raw!r}")
    lat_min, lon_min, lat_max, lon_max = parts
    if not (-90 <= lat_min < lat_max <= 90 and -180 <= lon_min < lon_max <= 180):
        raise ValueError(f"AIS_BBOX fora de faixa ou invertido: {raw!r}")
    return [[lat_min, lon_min], [lat_max, lon_max]]


def load_config() -> dict:
    api_key = os.environ.get("AISSTREAM_API_KEY", "").strip()
    if not api_key:
        sys.exit("AISSTREAM_API_KEY não definida (preencha o .env).")
    return {
        "api_key": api_key,
        "bbox": parse_bbox(os.environ.get("AIS_BBOX", DEFAULT_BBOX)),
        "bootstrap": os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
        "topic": os.environ.get("KAFKA_TOPIC", "ais.raw"),
        "url": os.environ.get("AISSTREAM_URL", "wss://stream.aisstream.io/v0/stream"),
        "stats_interval": float(os.environ.get("STATS_INTERVAL_S", "60")),
    }


# ---------------------------------------------------------------- mensagem


def to_record(raw: str | bytes) -> tuple[bytes, bytes, list[tuple[str, bytes]]] | None:
    """
    Converte uma mensagem do WebSocket em (chave, valor, headers) para o Kafka.
    Retorna None para mensagens que não devem ser publicadas.
    Levanta RuntimeError se o aisstream.io devolver erro (ex.: key inválida).
    """
    value = raw.encode("utf-8") if isinstance(raw, str) else raw
    msg = json.loads(value)

    if "error" in msg:
        raise RuntimeError(f"aisstream.io respondeu erro: {msg['error']}")

    msg_type = msg.get("MessageType", "")
    if msg_type in IGNORED_TYPES:
        return None

    mmsi = msg.get("MetaData", {}).get("MMSI")
    if mmsi is None:
        return None

    headers = [
        ("message_type", msg_type.encode()),
        ("source", b"aisstream.io"),
        ("ingested_at", datetime.now(timezone.utc).isoformat(timespec="milliseconds").encode()),
    ]
    return str(mmsi).encode(), value, headers


# ---------------------------------------------------------------- producer


class AisProducer:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.stats = Counter()
        self.by_type = Counter()
        self.producer = Producer(
            {
                "bootstrap.servers": cfg["bootstrap"],
                "client.id": "ais-producer",
                # Durabilidade: só confirma quando o broker gravou; idempotência evita duplicata em retry
                "acks": "all",
                "enable.idempotence": True,
                # Agrupa por até 100 ms antes de enviar e comprime o lote
                "linger.ms": 100,
                "compression.type": "lz4",
            }
        )

    def _on_delivery(self, err, kmsg):
        if err is not None:
            self.stats["falhas"] += 1
            log.error("Falha ao entregar mensagem (chave=%s): %s", kmsg.key(), err)
        else:
            self.stats["entregues"] += 1

    def publish(self, raw: str | bytes) -> None:
        self.stats["recebidas"] += 1
        record = to_record(raw)
        if record is None:
            self.stats["ignoradas"] += 1
            return
        key, value, headers = record
        self.by_type[dict(headers)["message_type"].decode()] += 1
        while True:
            try:
                self.producer.produce(
                    self.cfg["topic"],
                    key=key,
                    value=value,
                    headers=headers,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                # Fila local cheia (broker fora do ar): espera esvaziar um pouco e tenta de novo
                log.warning("Fila local do producer cheia; aguardando o broker...")
                self.producer.poll(1)
        # Dispara callbacks de entrega pendentes sem bloquear
        self.producer.poll(0)

    def log_stats(self) -> None:
        log.info(
            "recebidas=%d publicadas=%d entregues=%d falhas=%d ignoradas=%d pendentes=%d | %s",
            self.stats["recebidas"],
            self.stats["recebidas"] - self.stats["ignoradas"],
            self.stats["entregues"],
            self.stats["falhas"],
            self.stats["ignoradas"],
            len(self.producer),
            dict(self.by_type),
        )

    def close(self, timeout: float = 10) -> None:
        pendentes = self.producer.flush(timeout)
        if pendentes:
            log.warning("%d mensagens não foram entregues antes de encerrar.", pendentes)
        self.log_stats()


# ---------------------------------------------------------------- loop principal


async def stream(prod: AisProducer, stop: asyncio.Event) -> None:
    cfg = prod.cfg
    subscription = json.dumps({"APIKey": cfg["api_key"], "BoundingBoxes": [cfg["bbox"]]})
    backoff = 1

    while not stop.is_set():
        try:
            async with websockets.connect(cfg["url"], ping_interval=20, ping_timeout=20) as ws:
                # O aisstream.io exige a assinatura em até 3 s após conectar
                await ws.send(subscription)
                log.info("Conectado ao aisstream.io; recorte %s -> tópico %s", cfg["bbox"], cfg["topic"])
                backoff = 1
                while not stop.is_set():
                    recv = asyncio.create_task(ws.recv())
                    halt = asyncio.create_task(stop.wait())
                    done, _ = await asyncio.wait({recv, halt}, return_when=asyncio.FIRST_COMPLETED)
                    if halt in done:
                        recv.cancel()
                        return
                    halt.cancel()
                    prod.publish(recv.result())
        except RuntimeError as e:
            # Erro de negócio (key inválida, assinatura malformada): reconectar não resolve
            log.critical("%s", e)
            raise
        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.warning("Conexão perdida (%s). Reconectando em %ds...", e, backoff)
        try:
            await asyncio.wait_for(stop.wait(), timeout=backoff)
        except asyncio.TimeoutError:
            pass
        backoff = min(backoff * 2, 60)


async def periodic_stats(prod: AisProducer, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=prod.cfg["stats_interval"])
        except asyncio.TimeoutError:
            prod.producer.poll(0)
            prod.log_stats()


async def main() -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    cfg = load_config()
    try:
        prod = AisProducer(cfg)
    except KafkaException as e:
        log.critical("Não foi possível criar o producer Kafka: %s", e)
        return 1

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows: Ctrl+C cai no KeyboardInterrupt
            pass

    stats_task = asyncio.create_task(periodic_stats(prod, stop))
    code = 0
    try:
        await stream(prod, stop)
    except RuntimeError:
        code = 2
    finally:
        stop.set()
        await stats_task
        log.info("Encerrando: enviando mensagens pendentes ao Kafka...")
        prod.close()
    return code


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        pass
