from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from jasi.domain.models import utcnow


@dataclass(frozen=True)
class OperationsSnapshot:
    work: dict[str, int]
    outbox: dict[str, int]
    effects: dict[str, int]
    source_items: dict[str, int]
    drift_opportunities: dict[str, int]
    due_schedules: int
    due_sources: int
    expired_work_leases: int
    generated_at: datetime = field(default_factory=utcnow)
