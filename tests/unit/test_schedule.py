from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from jasi.application.direct_work import DirectWorkHandler
from jasi.application.schedule import ScheduleService, ScheduleWorker
from jasi.domain.schedule import (
    ScheduleCreateResult,
    ScheduleJobRecord,
    ScheduleSpec,
    next_run_after,
)
from jasi.domain.work import WorkSpec
from tests.unit.fakes import FakeRepository, FakeWorkRepository


def job(
    *,
    schedule_kind: str,
    next_run_at: datetime,
    interval_seconds: int | None = None,
    cron_expression: str | None = None,
    timezone: str = "UTC",
) -> ScheduleJobRecord:
    return ScheduleJobRecord(
        id=1,
        dedupe_key="job:1",
        session_id="telegram:1",
        conversation_id=1,
        action="direct",
        schedule_kind=schedule_kind,
        timezone=timezone,
        next_run_at=next_run_at,
        input_text="reminder",
        profile=None,
        interval_seconds=interval_seconds,
        cron_expression=cron_expression,
        payload={},
        priority=90,
        max_attempts=5,
        enabled=True,
        version=1,
    )


def schedule_spec(**overrides) -> ScheduleSpec:
    values = {
        "dedupe_key": "job:1",
        "session_id": "telegram:1",
        "conversation_id": 1,
        "action": "direct",
        "schedule_kind": "at",
        "timezone": "Asia/Shanghai",
        "next_run_at": datetime(2026, 7, 11, 12, tzinfo=UTC),
        "input_text": "reminder",
    }
    values.update(overrides)
    return ScheduleSpec(**values)


def test_schedule_spec_rejects_ambiguous_or_invalid_recurrence() -> None:
    with pytest.raises(ValueError, match="cannot define recurrence"):
        schedule_spec(interval_seconds=60)
    with pytest.raises(ValueError, match="positive seconds"):
        schedule_spec(schedule_kind="interval", interval_seconds=0)
    with pytest.raises(ValueError, match="invalid cron"):
        schedule_spec(schedule_kind="cron", cron_expression="not a cron")
    with pytest.raises(ValueError, match="requires a profile"):
        schedule_spec(action="agent")


def test_interval_misfire_coalesces_to_first_future_time() -> None:
    current = datetime(2026, 7, 11, 10, 5, 30, tzinfo=UTC)
    record = job(
        schedule_kind="interval",
        next_run_at=datetime(2026, 7, 11, 10, 0, tzinfo=UTC),
        interval_seconds=60,
    )

    assert next_run_after(record, current) == datetime(2026, 7, 11, 10, 6, tzinfo=UTC)


def test_cron_uses_job_timezone_and_one_time_schedule_stops() -> None:
    current = datetime(2026, 7, 11, 2, 3, tzinfo=UTC)
    cron_job = job(
        schedule_kind="cron",
        next_run_at=current,
        cron_expression="*/5 * * * *",
        timezone="Asia/Shanghai",
    )
    one_time = job(schedule_kind="at", next_run_at=current)

    assert next_run_after(cron_job, current) == datetime(2026, 7, 11, 2, 5, tzinfo=UTC)
    assert next_run_after(one_time, current) is None


@pytest.mark.asyncio
async def test_direct_handler_builds_outbound_without_runtime() -> None:
    work_repo = FakeWorkRepository()
    queued = await work_repo.enqueue_work(
        WorkSpec(
            kind="scheduled",
            action="direct",
            dedupe_key="schedule:1:time",
            session_id="telegram:1",
            conversation_id=1,
            input_text="stand up",
            priority=90,
        )
    )
    conversations = FakeRepository()
    conversations.add_conversation()

    result = await DirectWorkHandler(conversations).execute(queued.work)

    assert result.turn_id is None
    assert result.outbound is not None
    assert result.outbound.channel == "telegram"
    assert result.outbound.text == "stand up"
    assert result.outbound.origin == "scheduled"


@pytest.mark.asyncio
async def test_schedule_service_and_worker_wake_the_next_stage() -> None:
    work_repo = FakeWorkRepository()
    queued = await work_repo.enqueue_work(
        WorkSpec(
            kind="scheduled",
            action="direct",
            dedupe_key="schedule:1:time",
            session_id="telegram:1",
            conversation_id=1,
            input_text="stand up",
            priority=90,
        )
    )

    class Repository:
        def __init__(self) -> None:
            self.materialized = False

        async def create_schedule(self, spec: ScheduleSpec) -> ScheduleCreateResult:
            return ScheduleCreateResult(
                job=job(schedule_kind="at", next_run_at=spec.next_run_at),
                created=True,
            )

        async def cancel_schedule(self, _job_id: int) -> bool:
            return True

        async def materialize_due(self, _now: datetime, _limit: int):
            if self.materialized:
                return []
            self.materialized = True
            return [queued.work]

    repository = Repository()
    schedule_wakeup = asyncio.Event()
    work_wakeup = asyncio.Event()
    service = ScheduleService(
        repository=repository,
        schedule_wakeup=schedule_wakeup,
    )
    worker = ScheduleWorker(
        repository=repository,
        batch_size=10,
        schedule_wakeup=schedule_wakeup,
        work_wakeup=work_wakeup,
    )

    await service.create(schedule_spec())
    assert schedule_wakeup.is_set()
    assert await worker.drain_once() == 1
    assert work_wakeup.is_set()
