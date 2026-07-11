from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from zoneinfo import ZoneInfo

from croniter import croniter

from jasi.domain.models import utcnow
from jasi.domain.work import WorkAction

ScheduleKind = Literal["at", "interval", "cron"]


@dataclass(frozen=True)
class ScheduleSpec:
    dedupe_key: str
    session_id: str
    conversation_id: int
    action: WorkAction
    schedule_kind: ScheduleKind
    timezone: str
    next_run_at: datetime
    input_text: str
    profile: str | None = None
    interval_seconds: int | None = None
    cron_expression: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    priority: int | None = None
    max_attempts: int = 5

    def __post_init__(self) -> None:
        if not self.dedupe_key.strip():
            raise ValueError("schedule dedupe key cannot be empty")
        if not self.session_id.strip():
            raise ValueError("schedule session ID cannot be empty")
        if self.conversation_id <= 0:
            raise ValueError("schedule conversation ID must be positive")
        if not self.input_text.strip():
            raise ValueError("schedule input cannot be empty")
        if self.next_run_at.tzinfo is None:
            raise ValueError("schedule next run must be timezone-aware")
        if self.max_attempts <= 0:
            raise ValueError("schedule max attempts must be positive")
        ZoneInfo(self.timezone)

        if self.action not in {"agent", "direct"}:
            raise ValueError(f"unsupported schedule action: {self.action}")
        if self.action == "agent" and not (self.profile or "").strip():
            raise ValueError("agent schedule requires a profile")
        if self.schedule_kind == "at":
            if self.interval_seconds is not None or self.cron_expression is not None:
                raise ValueError("one-time schedule cannot define recurrence")
        elif self.schedule_kind == "interval":
            if self.interval_seconds is None or self.interval_seconds <= 0:
                raise ValueError("interval schedule requires positive seconds")
            if self.cron_expression is not None:
                raise ValueError("interval schedule cannot define cron")
        elif self.schedule_kind == "cron":
            if not (self.cron_expression or "").strip():
                raise ValueError("cron schedule requires an expression")
            if self.interval_seconds is not None:
                raise ValueError("cron schedule cannot define an interval")
            if not croniter.is_valid(self.cron_expression):
                raise ValueError("invalid cron expression")
        else:
            raise ValueError(f"unsupported schedule kind: {self.schedule_kind}")


@dataclass(frozen=True)
class ScheduleJobRecord:
    id: int
    dedupe_key: str
    session_id: str
    conversation_id: int
    action: WorkAction
    schedule_kind: ScheduleKind
    timezone: str
    next_run_at: datetime
    input_text: str
    profile: str | None
    interval_seconds: int | None
    cron_expression: str | None
    payload: dict[str, Any]
    priority: int
    max_attempts: int
    enabled: bool
    version: int
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class ScheduleCreateResult:
    job: ScheduleJobRecord
    created: bool


def schedule_priority(action: WorkAction) -> int:
    return 90 if action == "direct" else 70


def next_run_after(job: ScheduleJobRecord, now: datetime) -> datetime | None:
    if now.tzinfo is None:
        raise ValueError("schedule clock must be timezone-aware")
    if job.schedule_kind == "at":
        return None
    if job.schedule_kind == "interval":
        seconds = job.interval_seconds
        if seconds is None or seconds <= 0:
            raise ValueError("invalid persisted interval schedule")
        elapsed = max(0.0, (now - job.next_run_at).total_seconds())
        steps = int(elapsed // seconds) + 1
        return (job.next_run_at + timedelta(seconds=steps * seconds)).astimezone(UTC)

    expression = job.cron_expression
    if not expression:
        raise ValueError("invalid persisted cron schedule")
    timezone = ZoneInfo(job.timezone)
    next_local = croniter(expression, now.astimezone(timezone)).get_next(datetime)
    if next_local.tzinfo is None:
        next_local = next_local.replace(tzinfo=timezone)
    return next_local.astimezone(UTC)
