from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jasi.adapters.persistence.postgres.db import (
    DriftOpportunity,
    EffectOutbox,
    OutboxMessage,
    ScheduledJob,
    SourceItem,
    SourceSubscription,
    WorkItem,
)
from jasi.domain.operations import OperationsSnapshot


class SQLAlchemyOperationsRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def snapshot(self, now: datetime) -> OperationsSnapshot:
        if now.tzinfo is None:
            raise ValueError("operations clock must be timezone-aware")
        now = now.astimezone(UTC)
        async with self._session_factory() as session:
            return OperationsSnapshot(
                work=await _status_counts(session, WorkItem, WorkItem.status),
                outbox=await _status_counts(session, OutboxMessage, OutboxMessage.status),
                effects=await _status_counts(session, EffectOutbox, EffectOutbox.status),
                source_items=await _status_counts(session, SourceItem, SourceItem.status),
                drift_opportunities=await _status_counts(
                    session,
                    DriftOpportunity,
                    DriftOpportunity.status,
                ),
                due_schedules=int(
                    await session.scalar(
                        select(func.count())
                        .select_from(ScheduledJob)
                        .where(
                            ScheduledJob.enabled.is_(True),
                            ScheduledJob.next_run_at <= now,
                        )
                    )
                    or 0
                ),
                due_sources=int(
                    await session.scalar(
                        select(func.count())
                        .select_from(SourceSubscription)
                        .where(
                            SourceSubscription.enabled.is_(True),
                            SourceSubscription.next_poll_at <= now,
                        )
                    )
                    or 0
                ),
                expired_work_leases=int(
                    await session.scalar(
                        select(func.count())
                        .select_from(WorkItem)
                        .where(
                            WorkItem.status == "running",
                            WorkItem.lease_until <= now,
                        )
                    )
                    or 0
                ),
                generated_at=now,
            )


async def _status_counts(
    session: AsyncSession,
    model,
    status_column,
) -> dict[str, int]:
    rows = (
        await session.execute(
            select(status_column, func.count()).select_from(model).group_by(status_column)
        )
    ).all()
    return {str(status): int(count) for status, count in rows}
