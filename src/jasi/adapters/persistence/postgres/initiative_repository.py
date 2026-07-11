from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jasi.adapters.persistence.postgres.db import (
    DriftOpportunity,
    InitiativeState,
    SourceItem,
    SourceSubscription,
    WorkItem,
)
from jasi.adapters.persistence.postgres.records import work_record
from jasi.domain.source import InitiativeKind
from jasi.domain.work import WorkRecord


class SQLAlchemyInitiativeRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def materialize_initiatives(
        self,
        kind: InitiativeKind,
        now: datetime,
        limit: int,
    ) -> list[WorkRecord]:
        if kind not in {"proactive", "drift"}:
            raise ValueError(f"unsupported initiative kind: {kind}")
        if now.tzinfo is None:
            raise ValueError("initiative clock must be timezone-aware")
        if limit <= 0:
            return []
        now = now.astimezone(UTC)
        if kind == "drift":
            return await self._materialize_drift(now, limit)

        async with self._session_factory.begin() as session:
            await session.execute(
                update(SourceItem)
                .where(
                    SourceItem.status == "new",
                    SourceItem.expires_at <= now,
                )
                .values(status="expired", updated_at=now)
            )

            rank = func.row_number().over(
                partition_by=SourceSubscription.session_id,
                order_by=(SourceItem.occurred_at, SourceItem.id),
            )
            ranked = (
                select(
                    SourceItem.id.label("item_id"),
                    rank.label("session_rank"),
                )
                .join(
                    SourceSubscription,
                    SourceSubscription.id == SourceItem.subscription_id,
                )
                .outerjoin(
                    InitiativeState,
                    InitiativeState.session_id == SourceSubscription.session_id,
                )
                .where(
                    SourceItem.status == "new",
                    SourceItem.expires_at > now,
                    SourceSubscription.enabled.is_(True),
                    or_(
                        InitiativeState.next_proactive_at.is_(None),
                        InitiativeState.next_proactive_at <= now,
                    ),
                )
                .subquery()
            )
            items = list(
                (
                    await session.scalars(
                        select(SourceItem)
                        .join(ranked, ranked.c.item_id == SourceItem.id)
                        .where(ranked.c.session_rank == 1)
                        .order_by(SourceItem.occurred_at, SourceItem.id)
                        .limit(limit)
                        .with_for_update(skip_locked=True, of=SourceItem)
                    )
                ).all()
            )

            created: list[WorkItem] = []
            for item in items:
                subscription = await session.get(SourceSubscription, item.subscription_id)
                if subscription is None:
                    raise RuntimeError("source item references a missing subscription")
                state = await self._lock_state(
                    session,
                    subscription.session_id,
                    subscription.conversation_id,
                    now,
                )
                if state.next_proactive_at is not None and state.next_proactive_at > now:
                    continue

                work = WorkItem(
                    kind="proactive",
                    action="agent",
                    dedupe_key=f"source:{subscription.id}:{item.external_id}",
                    session_id=subscription.session_id,
                    conversation_id=subscription.conversation_id,
                    profile=subscription.profile,
                    input_text=item.text,
                    payload={
                        **dict(item.payload or {}),
                        "source": subscription.source,
                        "source_item_id": item.id,
                        "source_subscription_id": subscription.id,
                        "source_external_id": item.external_id,
                        "occurred_at": item.occurred_at.isoformat(),
                    },
                    priority=subscription.priority,
                    status="pending",
                    available_at=now,
                    max_attempts=5,
                )
                session.add(work)
                await session.flush()
                item.status = "enqueued"
                item.work_item_id = work.id
                item.updated_at = now
                state.next_proactive_at = now + timedelta(
                    seconds=subscription.cooldown_seconds
                )
                state.updated_at = now
                created.append(work)

            await session.flush()
            return [work_record(row) for row in created]

    async def _materialize_drift(
        self,
        now: datetime,
        limit: int,
    ) -> list[WorkRecord]:
        async with self._session_factory.begin() as session:
            await session.execute(
                update(DriftOpportunity)
                .where(
                    DriftOpportunity.status == "new",
                    DriftOpportunity.expires_at <= now,
                )
                .values(status="expired", updated_at=now)
            )

            rank = func.row_number().over(
                partition_by=DriftOpportunity.session_id,
                order_by=(DriftOpportunity.available_at, DriftOpportunity.id),
            )
            ranked = (
                select(
                    DriftOpportunity.id.label("opportunity_id"),
                    rank.label("session_rank"),
                )
                .outerjoin(
                    InitiativeState,
                    InitiativeState.session_id == DriftOpportunity.session_id,
                )
                .where(
                    DriftOpportunity.status == "new",
                    DriftOpportunity.available_at <= now,
                    DriftOpportunity.expires_at > now,
                    or_(
                        InitiativeState.next_drift_at.is_(None),
                        InitiativeState.next_drift_at <= now,
                    ),
                )
                .subquery()
            )
            opportunities = list(
                (
                    await session.scalars(
                        select(DriftOpportunity)
                        .join(
                            ranked,
                            ranked.c.opportunity_id == DriftOpportunity.id,
                        )
                        .where(ranked.c.session_rank == 1)
                        .order_by(DriftOpportunity.available_at, DriftOpportunity.id)
                        .limit(limit)
                        .with_for_update(skip_locked=True, of=DriftOpportunity)
                    )
                ).all()
            )

            created: list[WorkItem] = []
            for opportunity in opportunities:
                state = await self._lock_state(
                    session,
                    opportunity.session_id,
                    opportunity.conversation_id,
                    now,
                )
                if state.next_drift_at is not None and state.next_drift_at > now:
                    continue
                activity = [
                    value
                    for value in (state.last_user_at, state.last_delivery_at)
                    if value is not None
                ]
                if activity and max(activity) > now - timedelta(
                    seconds=opportunity.min_idle_seconds
                ):
                    continue

                work = WorkItem(
                    kind="drift",
                    action="agent",
                    dedupe_key=f"drift:{opportunity.id}",
                    session_id=opportunity.session_id,
                    conversation_id=opportunity.conversation_id,
                    profile=opportunity.profile,
                    input_text=opportunity.input_text,
                    payload={
                        **dict(opportunity.payload or {}),
                        "drift_opportunity_id": opportunity.id,
                    },
                    priority=opportunity.priority,
                    status="pending",
                    available_at=now,
                    max_attempts=5,
                )
                session.add(work)
                await session.flush()
                opportunity.status = "enqueued"
                opportunity.work_item_id = work.id
                opportunity.updated_at = now
                state.next_drift_at = now + timedelta(
                    seconds=opportunity.cooldown_seconds
                )
                state.updated_at = now
                created.append(work)

            await session.flush()
            return [work_record(row) for row in created]

    async def _lock_state(
        self,
        session: AsyncSession,
        session_id: str,
        conversation_id: int,
        now: datetime,
    ) -> InitiativeState:
        await session.execute(
            pg_insert(InitiativeState)
            .values(
                session_id=session_id,
                conversation_id=conversation_id,
                updated_at=now,
            )
            .on_conflict_do_nothing(index_elements=[InitiativeState.session_id])
        )
        state = (
            await session.scalars(
                select(InitiativeState)
                .where(InitiativeState.session_id == session_id)
                .with_for_update()
            )
        ).one()
        if state.conversation_id != conversation_id:
            raise ValueError("initiative session maps to multiple conversations")
        return state
