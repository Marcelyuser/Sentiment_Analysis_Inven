from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class KafkaProducerConfig:
    """
    Kafka producer configuration for sending analyzed documents.

    Environment variables (recommended):
      - KAFKA_BOOTSTRAP_SERVERS: "host1:9092,host2:9092"
      - KAFKA_TOPIC: "inven.sentiment"
      - KAFKA_CLIENT_ID: optional
      - KAFKA_ACKS: "all" | "1" | "0" (default: "all")
      - KAFKA_RETRIES: int (default: 10)
      - KAFKA_LINGER_MS: int (default: 20)
      - KAFKA_BATCH_SIZE: int (default: 32768)
      - KAFKA_COMPRESSION_TYPE: "gzip" | "snappy" | "lz4" | "zstd" | "" (default: "gzip")

    Optional security (set only if needed):
      - KAFKA_SECURITY_PROTOCOL: "PLAINTEXT" | "SASL_PLAINTEXT" | "SASL_SSL" | "SSL"
      - KAFKA_SASL_MECHANISM: "SCRAM-SHA-512" | "SCRAM-SHA-256" | "PLAIN" ...
      - KAFKA_SASL_USERNAME
      - KAFKA_SASL_PASSWORD
      - KAFKA_SSL_CAFILE
      - KAFKA_SSL_CERTFILE
      - KAFKA_SSL_KEYFILE
    """

    bootstrap_servers: str
    topic: str
    client_id: str = "inven-sentiment-producer"

    acks: str = "all"
    retries: int = 10
    linger_ms: int = 20
    batch_size: int = 32768
    compression_type: str = "gzip"

    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_plain_username: Optional[str] = None
    sasl_plain_password: Optional[str] = None

    ssl_cafile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None

    @staticmethod
    def from_env() -> "KafkaProducerConfig":
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
        topic = os.getenv("KAFKA_TOPIC", "").strip()
        if not bootstrap or not topic:
            raise ValueError(
                "Missing Kafka env vars. Required: KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC"
            )

        return KafkaProducerConfig(
            bootstrap_servers=bootstrap,
            topic=topic,
            client_id=os.getenv("KAFKA_CLIENT_ID", "inven-sentiment-producer").strip() or "inven-sentiment-producer",
            acks=os.getenv("KAFKA_ACKS", "all").strip() or "all",
            retries=int(os.getenv("KAFKA_RETRIES", "10")),
            linger_ms=int(os.getenv("KAFKA_LINGER_MS", "20")),
            batch_size=int(os.getenv("KAFKA_BATCH_SIZE", "32768")),
            compression_type=os.getenv("KAFKA_COMPRESSION_TYPE", "gzip").strip() or None,
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").strip() or "PLAINTEXT",
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "").strip() or None,
            sasl_plain_username=os.getenv("KAFKA_SASL_USERNAME", "").strip() or None,
            sasl_plain_password=os.getenv("KAFKA_SASL_PASSWORD", "").strip() or None,
            ssl_cafile=os.getenv("KAFKA_SSL_CAFILE", "").strip() or None,
            ssl_certfile=os.getenv("KAFKA_SSL_CERTFILE", "").strip() or None,
            ssl_keyfile=os.getenv("KAFKA_SSL_KEYFILE", "").strip() or None,
        )


class InvenKafkaProducer:
    """
    Thin wrapper over kafka-python's KafkaProducer.

    - Key: doc_id (bytes) for stable partitioning and compaction-friendly streams.
    - Value: JSON (UTF-8)
    """

    def __init__(self, cfg: KafkaProducerConfig):
        self.cfg = cfg
        self._producer = self._build_producer(cfg)

    def close(self) -> None:
        try:
            self._producer.flush(timeout=30)
        finally:
            self._producer.close(timeout=30)

    def send_many(self, items: Iterable[dict[str, Any]]) -> int:
        """
        Send multiple items to Kafka.

        Returns:
            Number of messages successfully queued (send invoked). Delivery errors raise.
        """
        count = 0
        for item in items:
            doc_id = str(item.get("doc_id", "")).strip()
            if not doc_id:
                raise ValueError("Missing doc_id in item (used as Kafka key).")

            future = self._producer.send(
                self.cfg.topic,
                key=doc_id.encode("utf-8"),
                value=item,
            )
            try:
                metadata = future.get(timeout=30)
                logger.debug(
                    "Produced: topic=%s partition=%s offset=%s key=%s",
                    metadata.topic, metadata.partition, metadata.offset, doc_id
                )
            except KafkaError as e:
                logger.error("Kafka produce failed: key=%s err=%s", doc_id, e)
                raise

            count += 1

        # ensure delivery
        self._producer.flush(timeout=30)
        return count

    def _build_producer(self, cfg: KafkaProducerConfig) -> KafkaProducer:
        kwargs: dict[str, Any] = {
            "bootstrap_servers": [s.strip() for s in cfg.bootstrap_servers.split(",") if s.strip()],
            "client_id": cfg.client_id,
            "acks": cfg.acks,
            "retries": cfg.retries,
            "linger_ms": cfg.linger_ms,
            "batch_size": cfg.batch_size,
            "compression_type": cfg.compression_type,
            "key_serializer": lambda k: k,  # already bytes
            "value_serializer": lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            "max_in_flight_requests_per_connection": 5,
            "request_timeout_ms": 30000,
        }

        # Security options (only applied if provided)
        sec = cfg.security_protocol.upper()
        kwargs["security_protocol"] = sec

        if sec in ("SASL_PLAINTEXT", "SASL_SSL"):
            if not (cfg.sasl_mechanism and cfg.sasl_plain_username and cfg.sasl_plain_password):
                raise ValueError(
                    "SASL selected but missing one of: KAFKA_SASL_MECHANISM, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD"
                )
            kwargs["sasl_mechanism"] = cfg.sasl_mechanism
            kwargs["sasl_plain_username"] = cfg.sasl_plain_username
            kwargs["sasl_plain_password"] = cfg.sasl_plain_password

        if sec in ("SSL", "SASL_SSL"):
            # cafile is strongly recommended to verify broker cert
            if cfg.ssl_cafile:
                kwargs["ssl_cafile"] = cfg.ssl_cafile
            if cfg.ssl_certfile:
                kwargs["ssl_certfile"] = cfg.ssl_certfile
            if cfg.ssl_keyfile:
                kwargs["ssl_keyfile"] = cfg.ssl_keyfile

        logger.info(
            "Kafka producer ready: bootstrap=%s topic=%s security=%s client_id=%s",
            cfg.bootstrap_servers, cfg.topic, sec, cfg.client_id
        )
        return KafkaProducer(**kwargs)
