from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from jasi.domain.models import utcnow


@dataclass(frozen=True)
class DriftOpportunitySpec:
    dedupe_key: str
    session_id: str
    conversation_id: int
    input_text: str
    available_at: datetime = field(default_factory=utcnow)
    expires_at: datetime | None = None
    profile: str = "drift"
    min_idle_seconds: int = 3_600
    cooldown_seconds: int = 21_600
    priority: int = 20
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name, value in (
            ("dedupe key", self.dedupe_key),
            ("session ID", self.session_id),
            ("input", self.input_text),
            ("profile", self.profile),
        ):
            if not value.strip():
                raise ValueError(f"drift {name} cannot be empty")
        if self.conversation_id <= 0:
            raise ValueError("drift conversation ID must be positive")
        if self.available_at.tzinfo is None:
            raise ValueError("drift availability must be timezone-aware")
        if self.expires_at is not None:
            if self.expires_at.tzinfo is None:
                raise ValueError("drift expiry must be timezone-aware")
            if self.expires_at <= self.available_at:
                raise ValueError("drift expiry must be after availability")
        if self.min_idle_seconds < 0:
            raise ValueError("drift idle threshold cannot be negative")
        if self.cooldown_seconds < 0:
            raise ValueError("drift cooldown cannot be negative")


@dataclass(frozen=True)
class DriftOpportunityRecord:
    id: int
    dedupe_key: str
    session_id: str
    conversation_id: int
    input_text: str
    available_at: datetime
    expires_at: datetime
    profile: str
    min_idle_seconds: int
    cooldown_seconds: int
    priority: int
    payload: dict[str, Any]
    status: str
    work_item_id: int | None = None
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class DriftOfferResult:
    opportunity: DriftOpportunityRecord
    created: bool
