from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jasi.adapters.persistence.postgres.db import DriftOpportunity
from jasi.domain.drift import DriftOfferResult, DriftOpportunityRecord, DriftOpportunitySpec


class SQLAlchemyDriftRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def offer_drift(self, spec: DriftOpportunitySpec) -> DriftOfferResult:
        expires_at = spec.expires_at or (spec.available_at + timedelta(days=1))
        async with self._session_factory.begin() as session:
            statement = (
                pg_insert(DriftOpportunity)
                .values(
                    dedupe_key=spec.dedupe_key,
                    session_id=spec.session_id,
                    conversation_id=spec.conversation_id,
                    input_text=spec.input_text,
                    available_at=spec.available_at.astimezone(UTC),
                    expires_at=expires_at.astimezone(UTC),
                    profile=spec.profile,
                    min_idle_seconds=spec.min_idle_seconds,
                    cooldown_seconds=spec.cooldown_seconds,
                    priority=spec.priority,
                    payload=spec.payload,
                    status="new",
                )
                .on_conflict_do_nothing(index_elements=[DriftOpportunity.dedupe_key])
                .returning(DriftOpportunity.id)
            )
            opportunity_id = (await session.execute(statement)).scalar_one_or_none()
            created = opportunity_id is not None
            if opportunity_id is None:
                opportunity_id = await session.scalar(
                    select(DriftOpportunity.id).where(
                        DriftOpportunity.dedupe_key == spec.dedupe_key
                    )
                )
            if opportunity_id is None:
                raise RuntimeError("drift opportunity disappeared after conflict")
            row = await session.get(DriftOpportunity, opportunity_id)
            if row is None:
                raise RuntimeError("drift opportunity disappeared after creation")
            return DriftOfferResult(opportunity=_opportunity_record(row), created=created)

    async def dismiss_drift(self, opportunity_id: int) -> bool:
        async with self._session_factory.begin() as session:
            row = (
                await session.scalars(
                    select(DriftOpportunity)
                    .where(DriftOpportunity.id == opportunity_id)
                    .with_for_update()
                )
            ).one_or_none()
            if row is None or row.status != "new":
                return False
            row.status = "dismissed"
            row.updated_at = datetime.now(UTC)
            return True


def _opportunity_record(row: DriftOpportunity) -> DriftOpportunityRecord:
    return DriftOpportunityRecord(
        id=row.id,
        dedupe_key=row.dedupe_key,
        session_id=row.session_id,
        conversation_id=row.conversation_id,
        input_text=row.input_text,
        available_at=row.available_at,
        expires_at=row.expires_at,
        profile=row.profile,
        min_idle_seconds=row.min_idle_seconds,
        cooldown_seconds=row.cooldown_seconds,
        priority=row.priority,
        payload=dict(row.payload or {}),
        status=row.status,
        work_item_id=row.work_item_id,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )
