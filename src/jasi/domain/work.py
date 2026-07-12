from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

from jasi.domain.models import MessageRecord, OutboxRecord, utcnow

WorkKind = Literal["passive", "scheduled", "proactive", "drift"]
WorkAction = Literal["agent", "direct"]
WorkStatus = Literal["pending", "running", "succeeded", "failed", "cancelled"]


@dataclass(frozen=True)
class WorkSpec:
    kind: WorkKind
    action: WorkAction
    dedupe_key: str
    session_id: str
    input_text: str
    priority: int
    conversation_id: int | None = None
    inbound_event_id: int | None = None
    profile: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    available_at: datetime = field(default_factory=utcnow)
    max_attempts: int = 5


@dataclass(frozen=True)
class WorkRecord:
    id: int
    kind: WorkKind
    action: WorkAction
    dedupe_key: str
    session_id: str
    input_text: str
    priority: int
    status: WorkStatus
    attempts: int
    max_attempts: int
    available_at: datetime
    conversation_id: int | None = None
    inbound_event_id: int | None = None
    profile: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    lease_token: str | None = None
    lease_until: datetime | None = None
    output_message_id: int | None = None
    last_error: str | None = None
    created_at: datetime = field(default_factory=utcnow)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    updated_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class WorkEnqueueResult:
    work: WorkRecord
    created: bool


@dataclass(frozen=True)
class OutboundDraft:
    channel: str
    external_chat_id: str
    text: str
    origin: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class WorkExecutionResult:
    turn_id: int | None = None
    outbound: OutboundDraft | None = None


@dataclass(frozen=True)
class WorkCompletion:
    message: MessageRecord | None = None
    outbox: tuple[OutboxRecord, ...] = ()


class WorkLeaseLost(RuntimeError):
    pass
