from datetime import datetime
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, field_validator


class SupportEvent(BaseModel):
    """Versioned contract enforced at the Kafka consumer boundary."""

    model_config = ConfigDict(extra="forbid", strict=True)
    schema_version: str = "1.0"
    event_id: str = Field(min_length=3)
    ticket_id: str = Field(pattern=r"^T-\d{4,}$")
    customer_id: str = Field(pattern=r"^C-\d{3,}$")
    channel: Literal["chat", "email", "web"]
    subject: str = Field(min_length=3, max_length=160)
    message: str = Field(min_length=10, max_length=5000)
    created_at: str
    priority: Literal["low", "medium", "high"]

    @field_validator("message")
    @classmethod
    def message_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("message cannot be blank")
        return value.strip()

    @field_validator("created_at")
    @classmethod
    def valid_iso_timestamp(cls, value: str) -> str:
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("created_at must be an ISO-8601 timestamp") from exc
        return value
