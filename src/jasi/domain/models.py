from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


def utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class InboundMessage:
    channel: str
    external_update_id: str
    external_chat_id: str
    external_user_id: str
    text: str
    received_at: datetime = field(default_factory=utcnow)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OutboundPart:
    text: str


@dataclass(frozen=True)
class OutboundMessage:
    channel: str
    external_chat_id: str
    text: str
    outbox_id: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DeliveryResult:
    success: bool
    external_message_id: str | None = None
    error: str | None = None
    retryable: bool = True


@dataclass(frozen=True)
class ConversationRecord:
    id: int
    channel: str
    external_chat_id: str
    created_at: datetime
    updated_at: datetime
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MessageRecord:
    id: int
    conversation_id: int
    role: str
    origin: str
    sequence: int
    content: str
    delivery_status: str
    turn_id: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class OutboxRecord:
    id: int
    conversation_id: int
    message_id: int
    channel: str
    external_chat_id: str
    segment_index: int
    segment_count: int
    text: str
    status: str
    attempts: int
    next_attempt_at: datetime
    last_error: str | None = None
    external_message_id: str | None = None
