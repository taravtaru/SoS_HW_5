import json
import logging
import os
from contextlib import asynccontextmanager
from io import BytesIO
from pathlib import Path
from struct import pack
from threading import Event, Thread
from time import monotonic

import httpx
from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from fastapi import FastAPI, HTTPException, Request
from fastavro import parse_schema, schemaless_writer
from fastavro.validation import ValidationError, validate

from app.events import MovieEvent
from app.generator import generate

logger = logging.getLogger("uvicorn.error")
TOPIC = "movie-events"
SUBJECT = f"{TOPIC}-value"
SCHEMA = (Path(__file__).resolve().parent.parent / "schemas" / "movie-event.v1.avsc").read_text()
AVRO_SCHEMA = parse_schema(json.loads(SCHEMA))


def register_schema() -> int:
    schema = {"schema": SCHEMA, "schemaType": "AVRO"}
    with httpx.Client(base_url=os.environ["SCHEMA_REGISTRY_URL"], timeout=10) as registry:
        registry.put(f"/config/{SUBJECT}", json={"compatibility": "BACKWARD_TRANSITIVE"}).raise_for_status()
        registry.post(f"/subjects/{SUBJECT}/versions", json=schema).raise_for_status()
        result = registry.post(f"/subjects/{SUBJECT}", json=schema).raise_for_status().json()

    logger.info("schema_registered subject=%s version=%s id=%s", SUBJECT, result["version"], result["id"])
    return result["id"]


def create_topic(brokers: str) -> None:
    admin = AdminClient({"bootstrap.servers": brokers})
    topic = NewTopic(TOPIC, num_partitions=3, replication_factor=2, config={"min.insync.replicas": "1"})
    try:
        admin.create_topics([topic], operation_timeout=10, request_timeout=15)[TOPIC].result()
    except KafkaException as error:
        if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise


class Publisher:
    def __init__(self, brokers: str, schema_id: int) -> None:
        self.header = pack(">BI", 0, schema_id)
        self.producer = Producer({
            "bootstrap.servers": brokers,
            "acks": "all",
            "enable.idempotence": True,
            "partitioner": "murmur2_random",
            "retry.backoff.ms": 100,
            "retry.backoff.max.ms": 2000,
            "delivery.timeout.ms": 15000,
        })

    def publish(self, event: MovieEvent) -> None:
        record = event.model_dump(mode="json")
        record["timestamp"] = event.timestamp
        validate(record, AVRO_SCHEMA, strict=True)
        payload = BytesIO(self.header)
        payload.seek(len(self.header))
        schemaless_writer(payload, AVRO_SCHEMA, record, strict=True)

        delivered = Event()
        delivery_error = None

        def on_delivery(error, _message) -> None:
            nonlocal delivery_error
            delivery_error = error
            delivered.set()

        self.producer.produce(
            TOPIC,
            key=event.user_id.encode(),
            value=payload.getvalue(),
            on_delivery=on_delivery,
        )
        deadline = monotonic() + 17
        while not delivered.is_set():
            if monotonic() >= deadline:
                raise TimeoutError("Kafka delivery confirmation timed out")
            self.producer.poll(0.1)
        if delivery_error is not None:
            raise KafkaException(delivery_error)

        logger.info(
            "event_published event_id=%s event_type=%s timestamp=%s",
            event.event_id, event.event_type, event.timestamp.isoformat(),
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    brokers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    create_topic(brokers)
    publisher = app.state.publisher = Publisher(brokers, register_schema())
    interval = float(os.getenv("GENERATOR_INTERVAL_SECONDS", "0"))
    stop = Event()
    worker = None
    if interval > 0:
        worker = Thread(target=generate, args=(publisher.publish, interval, stop), daemon=True)
        worker.start()
    try:
        yield
    finally:
        stop.set()
        if worker:
            worker.join(timeout=2)


app = FastAPI(title="Movie Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/events", status_code=201)
def publish_event(event: MovieEvent, request: Request):
    try:
        request.app.state.publisher.publish(event)
    except ValidationError as error:
        raise HTTPException(422, "Event does not match the Avro schema") from error
    except (KafkaException, BufferError, TimeoutError) as error:
        logger.warning("event_publish_failed event_id=%s error=%s", event.event_id, error)
        raise HTTPException(503, "Kafka did not confirm the event") from error
    return {"event_id": event.event_id}
