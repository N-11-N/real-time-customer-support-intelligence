import pytest
from pydantic import ValidationError
from src.contracts import SupportEvent
from src.governance import mask_pii
from src.streaming import validate_payload


def test_valid_event():
    event = SupportEvent.model_validate({
        "schema_version":"1.0", "event_id":"evt-1", "ticket_id":"T-1001",
        "customer_id":"C-901", "channel":"chat", "subject":"Need help",
        "message":"Please help with my order", "created_at":"2026-07-16T08:00:00Z",
        "priority":"high"})
    assert event.ticket_id == "T-1001"


def test_invalid_channel_rejected():
    with pytest.raises(ValidationError):
        SupportEvent.model_validate({"schema_version":"1.0", "event_id":"evt-1",
            "ticket_id":"T-1001", "customer_id":"C-901", "channel":"fax",
            "subject":"Need help", "message":"Please help with my order",
            "created_at":"2026-07-16T08:00:00Z", "priority":"high"})


def test_pii_masking():
    masked, detected = mask_pii("Email me at user@example.com or 0551234567")
    assert "user@example.com" not in masked
    assert set(detected) == {"email", "phone"}


def test_kafka_boundary_adds_coordinates_to_quarantine():
    payload = {"schema_version":"1.0", "event_id":"evt-bad", "ticket_id":"",
        "customer_id":"C-901", "channel":"fax", "subject":"x", "message":"short",
        "created_at":"not-a-date", "priority":"urgent"}
    valid, invalid = validate_payload(payload, partition=2, offset=41)
    assert valid is None
    assert invalid["kafka_partition"] == 2
    assert invalid["kafka_offset"] == 41
    assert invalid["rejection_reason"]
