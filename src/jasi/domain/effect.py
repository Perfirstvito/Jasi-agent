from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from jasi.domain.models import utcnow


@dataclass(frozen=True)
class EffectDraft:
    adapter: str
    operation: str
    dedupe_key: str
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name, value in (
            ("adapter", self.adapter),
            ("operation", self.operation),
            ("dedupe key", self.dedupe_key),
        ):
            if not value.strip():
                raise ValueError(f"effect {name} cannot be empty")


@dataclass(frozen=True)
class EffectRecord:
    id: int
    adapter: str
    operation: str
    dedupe_key: str
    payload: dict[str, Any]
    status: str
    attempts: int
    next_attempt_at: datetime
    result: dict[str, Any] = field(default_factory=dict)
    last_error: str | None = None
    created_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class EffectResult:
    success: bool
    result: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    retryable: bool = True
