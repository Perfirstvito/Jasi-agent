from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jasi.adapters.persistence.postgres.db import (
    ScheduledJob,
    ScheduleOccurrence,
    WorkItem,
)
from jasi.adapters.persistence.postgres.records import work_record
from jasi.domain.schedule import (
    ScheduleCreateResult,
    ScheduleJobRecord,
    ScheduleSpec,
    next_run_after,
    schedule_priority,
)
from jasi.domain.work import WorkRecord


class SQLAlchemyScheduleRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def create_schedule(self, spec: ScheduleSpec) -> ScheduleCreateResult:
        async with self._session_factory.begin() as session:
            statement = (
                pg_insert(ScheduledJob)
                .values(
                    dedupe_key=spec.dedupe_key,
                    session_id=spec.session_id,
                    conversation_id=spec.conversation_id,
                    action=spec.action,
                    schedule_kind=spec.schedule_kind,
                    timezone=spec.timezone,
                    next_run_at=spec.next_run_at.astimezone(UTC),
                    input_text=spec.input_text,
                    profile=spec.profile,
                    interval_seconds=spec.interval_seconds,
                    cron_expression=spec.cron_expression,
                    payload=spec.payload,
                    priority=spec.priority
                    if spec.priority is not None
                    else schedule_priority(spec.action),
                    max_attempts=spec.max_attempts,
                    enabled=True,
                    version=1,
                )
                .on_conflict_do_nothing(index_elements=[ScheduledJob.dedupe_key])
                .returning(ScheduledJob.id)
            )
            job_id = (await session.execute(statement)).scalar_one_or_none()
            created = job_id is not None
            if job_id is None:
                job_id = await session.scalar(
                    select(ScheduledJob.id).where(ScheduledJob.dedupe_key == spec.dedupe_key)
                )
            if job_id is None:
                raise RuntimeError("schedule disappeared after enqueue conflict")
            row = await session.get(ScheduledJob, job_id)
            if row is None:
                raise RuntimeError("schedule disappeared after creation")
            return ScheduleCreateResult(job=_job_record(row), created=created)

    async def get_schedule(self, job_id: int) -> ScheduleJobRecord | None:
        async with self._session_factory() as session:
            row = await session.get(ScheduledJob, job_id)
            return _job_record(row) if row is not None else None

    async def cancel_schedule(self, job_id: int) -> bool:
        async with self._session_factory.begin() as session:
            row = (
                await session.scalars(
                    select(ScheduledJob)
                    .where(ScheduledJob.id == job_id)
                    .with_for_update()
                )
            ).one_or_none()
            if row is None or not row.enabled:
                return False
            row.enabled = False
            row.version += 1
            row.updated_at = datetime.now(UTC)
            return True

    async def materialize_due(self, now: datetime, limit: int) -> list[WorkRecord]:
        if now.tzinfo is None:
            raise ValueError("schedule clock must be timezone-aware")
        if limit <= 0:
            return []
        now = now.astimezone(UTC)

        async with self._session_factory.begin() as session:
            jobs = list(
                (
                    await session.scalars(
                        select(ScheduledJob)
                        .where(
                            ScheduledJob.enabled.is_(True),
                            ScheduledJob.next_run_at <= now,
                        )
                        .order_by(ScheduledJob.next_run_at, ScheduledJob.id)
                        .limit(limit)
                        .with_for_update(skip_locked=True)
                    )
                ).all()
            )
            created: list[WorkItem] = []
            for job in jobs:
                scheduled_for = job.next_run_at.astimezone(UTC)
                occurrence_exists = await session.scalar(
                    select(ScheduleOccurrence.id).where(
                        ScheduleOccurrence.job_id == job.id,
                        ScheduleOccurrence.scheduled_for == scheduled_for,
                    )
                )

                if occurrence_exists is None:
                    work = WorkItem(
                        kind="scheduled",
                        action=job.action,
                        dedupe_key=f"schedule:{job.id}:{scheduled_for.isoformat()}",
                        session_id=job.session_id,
                        conversation_id=job.conversation_id,
                        profile=job.profile,
                        input_text=job.input_text,
                        payload={
                            **dict(job.payload or {}),
                            "schedule_job_id": job.id,
                            "scheduled_for": scheduled_for.isoformat(),
                        },
                        priority=job.priority,
                        status="pending",
                        available_at=now,
                        max_attempts=job.max_attempts,
                    )
                    session.add(work)
                    await session.flush()
                    session.add(
                        ScheduleOccurrence(
                            job_id=job.id,
                            scheduled_for=scheduled_for,
                            work_item_id=work.id,
                        )
                    )
                    created.append(work)

                next_run = next_run_after(_job_record(job), now)
                job.enabled = next_run is not None
                if next_run is not None:
                    job.next_run_at = next_run
                job.version += 1
                job.updated_at = now

            await session.flush()
            return [work_record(row) for row in created]


def _job_record(row: ScheduledJob) -> ScheduleJobRecord:
    return ScheduleJobRecord(
        id=row.id,
        dedupe_key=row.dedupe_key,
        session_id=row.session_id,
        conversation_id=row.conversation_id,
        action=row.action,
        schedule_kind=row.schedule_kind,
        timezone=row.timezone,
        next_run_at=row.next_run_at,
        input_text=row.input_text,
        profile=row.profile,
        interval_seconds=row.interval_seconds,
        cron_expression=row.cron_expression,
        payload=dict(row.payload or {}),
        priority=row.priority,
        max_attempts=row.max_attempts,
        enabled=row.enabled,
        version=row.version,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )
