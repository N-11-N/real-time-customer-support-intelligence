import json
from pathlib import Path
from pydantic import ValidationError
from .contracts import SupportEvent

TOPIC = "support_events_v1"
DLQ_TOPIC = "support_events_dlq"


def validate_payload(payload: dict, partition: int = 0, offset: int = 0):
    """Pure validation boundary, independently testable without a live broker."""
    try:
        model = SupportEvent.model_validate(payload)
        return model.model_dump(mode="json"), None
    except ValidationError as exc:
        return None, {
            **payload,
            "kafka_partition": partition,
            "kafka_offset": offset,
            # exc.json() converts nested exception objects into JSON-safe strings.
            "rejection_reason": json.loads(exc.json(include_url=False)),
        }


def produce_jsonl(path: str, bootstrap_servers: str = "localhost:9092") -> int:
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        enable_idempotence=True,
    )
    count = 0
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        payload.setdefault("schema_version", "1.0")
        producer.send(TOPIC, key=payload.get("ticket_id", "unknown").encode(), value=payload)
        count += 1
    producer.flush()
    producer.close()
    return count


def consume_validated(expected: int, bootstrap_servers: str = "localhost:9092"):
    """Return accepted and quarantined records; commit only after validation."""
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="support-silver-writer-v1",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=15000,
    )
    accepted, rejected = [], []
    for record in consumer:
        valid, invalid = validate_payload(record.value, record.partition, record.offset)
        if valid is not None:
            accepted.append(valid)
        else:
            rejected.append(invalid)
        if len(accepted) + len(rejected) >= expected:
            break
    consumer.commit()
    consumer.close()
    return accepted, rejected
