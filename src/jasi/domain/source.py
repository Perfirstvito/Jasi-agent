from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

from jasi.domain.models import utcnow

InitiativeKind = Literal["proactive", "drift"]


class SourceLeaseLost(RuntimeError):
    pass


@dataclass(frozen=True)
class SourceSubscriptionSpec:
    source: str
    dedupe_key: str
    session_id: str
    conversation_id: int
    profile: str = "proactive"
    config: dict[str, Any] = field(default_factory=dict)
    poll_interval_seconds: int = 300
    item_ttl_seconds: int = 86_400
    cooldown_seconds: int = 1_800
    priority: int = 40
    next_poll_at: datetime = field(default_factory=utcnow)

    def __post_init__(self) -> None:
        for name, value in (
            ("source", self.source),
            ("dedupe key", self.dedupe_key),
            ("session ID", self.session_id),
            ("profile", self.profile),
        ):
            if not value.strip():
                raise ValueError(f"source {name} cannot be empty")
        if self.conversation_id <= 0:
            raise ValueError("source conversation ID must be positive")
        if self.poll_interval_seconds <= 0:
            raise ValueError("source poll interval must be positive")
        if self.item_ttl_seconds <= 0:
            raise ValueError("source item TTL must be positive")
        if self.cooldown_seconds < 0:
            raise ValueError("source cooldown cannot be negative")
        if self.next_poll_at.tzinfo is None:
            raise ValueError("source next poll must be timezone-aware")


@dataclass(frozen=True)
class SourceSubscriptionRecord:
    id: int
    source: str
    dedupe_key: str
    session_id: str
    conversation_id: int
    profile: str
    config: dict[str, Any]
    cursor: dict[str, Any]
    poll_interval_seconds: int
    item_ttl_seconds: int
    cooldown_seconds: int
    priority: int
    next_poll_at: datetime
    enabled: bool
    poll_attempts: int
    lease_token: str | None = None
    lease_until: datetime | None = None
    last_error: str | None = None
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class SourceCreateResult:
    subscription: SourceSubscriptionRecord
    created: bool


@dataclass(frozen=True)
class SourceItemDraft:
    external_id: str
    text: str
    occurred_at: datetime
    expires_at: datetime | None = None
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.external_id.strip():
            raise ValueError("source item external ID cannot be empty")
        if not self.text.strip():
            raise ValueError("source item text cannot be empty")
        if self.occurred_at.tzinfo is None:
            raise ValueError("source item occurrence time must be timezone-aware")
        if self.expires_at is not None and self.expires_at.tzinfo is None:
            raise ValueError("source item expiry must be timezone-aware")


@dataclass(frozen=True)
class SourceBatch:
    items: tuple[SourceItemDraft, ...]
    next_cursor: dict[str, Any] = field(default_factory=dict)
