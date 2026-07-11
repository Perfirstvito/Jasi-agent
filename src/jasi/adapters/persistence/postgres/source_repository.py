from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jasi.adapters.persistence.postgres.db import SourceItem, SourceSubscription
from jasi.domain.source import (
    SourceBatch,
    SourceCreateResult,
    SourceLeaseLost,
    SourceSubscriptionRecord,
    SourceSubscriptionSpec,
)


class SQLAlchemySourceRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def create_subscription(self, spec: SourceSubscriptionSpec) -> SourceCreateResult:
        async with self._session_factory.begin() as session:
            statement = (
                pg_insert(SourceSubscription)
                .values(
                    source=spec.source,
                    dedupe_key=spec.dedupe_key,
                    session_id=spec.session_id,
                    conversation_id=spec.conversation_id,
                    profile=spec.profile,
                    config=spec.config,
                    cursor={},
                    poll_interval_seconds=spec.poll_interval_seconds,
                    item_ttl_seconds=spec.item_ttl_seconds,
                    cooldown_seconds=spec.cooldown_seconds,
                    priority=spec.priority,
                    next_poll_at=spec.next_poll_at.astimezone(UTC),
                    enabled=True,
                )
                .on_conflict_do_nothing(index_elements=[SourceSubscription.dedupe_key])
                .returning(SourceSubscription.id)
            )
            subscription_id = (await session.execute(statement)).scalar_one_or_none()
            created = subscription_id is not None
            if subscription_id is None:
                subscription_id = await session.scalar(
                    select(SourceSubscription.id).where(
                        SourceSubscription.dedupe_key == spec.dedupe_key
                    )
                )
            if subscription_id is None:
                raise RuntimeError("source subscription disappeared after conflict")
            row = await session.get(SourceSubscription, subscription_id)
            if row is None:
                raise RuntimeError("source subscription disappeared after creation")
            return SourceCreateResult(subscription=_subscription_record(row), created=created)

    async def disable_subscription(self, subscription_id: int) -> bool:
        async with self._session_factory.begin() as session:
            row = (
                await session.scalars(
                    select(SourceSubscription)
                    .where(SourceSubscription.id == subscription_id)
                    .with_for_update()
                )
            ).one_or_none()
            if row is None or not row.enabled:
                return False
            row.enabled = False
            row.lease_token = None
            row.lease_until = None
            row.updated_at = datetime.now(UTC)
            return True

    async def claim_due_subscriptions(
        self,
        now: datetime,
        limit: int,
        lease_seconds: float,
    ) -> list[SourceSubscriptionRecord]:
        if now.tzinfo is None:
            raise ValueError("source clock must be timezone-aware")
        if limit <= 0:
            return []
        if lease_seconds <= 0:
            raise ValueError("source lease must be positive")
        now = now.astimezone(UTC)

        async with self._session_factory.begin() as session:
            rows = list(
                (
                    await session.scalars(
                        select(SourceSubscription)
                        .where(
                            SourceSubscription.enabled.is_(True),
                            SourceSubscription.next_poll_at <= now,
                            (
                                SourceSubscription.lease_until.is_(None)
                                | (SourceSubscription.lease_until <= now)
                            ),
                        )
                        .order_by(SourceSubscription.next_poll_at, SourceSubscription.id)
                        .limit(limit)
                        .with_for_update(skip_locked=True)
                    )
                ).all()
            )
            lease_until = now + timedelta(seconds=lease_seconds)
            for row in rows:
                row.lease_token = uuid4().hex
                row.lease_until = lease_until
                row.poll_attempts += 1
                row.last_error = None
                row.updated_at = now
            await session.flush()
            return [_subscription_record(row) for row in rows]

    async def complete_source_poll(
        self,
        subscription_id: int,
        lease_token: str,
        batch: SourceBatch,
        now: datetime,
    ) -> int:
        if now.tzinfo is None:
            raise ValueError("source clock must be timezone-aware")
        now = now.astimezone(UTC)
        async with self._session_factory.begin() as session:
            row = await self._get_for_update(session, subscription_id)
            self._verify_lease(row, lease_token, now)

            inserted = 0
            for item in batch.items:
                expires_at = item.expires_at or (
                    item.occurred_at + timedelta(seconds=row.item_ttl_seconds)
                )
                statement = (
                    pg_insert(SourceItem)
                    .values(
                        subscription_id=row.id,
                        external_id=item.external_id,
                        text=item.text,
                        occurred_at=item.occurred_at.astimezone(UTC),
                        expires_at=expires_at.astimezone(UTC),
                        payload=item.payload,
                        status="new",
                    )
                    .on_conflict_do_nothing(
                        index_elements=[SourceItem.subscription_id, SourceItem.external_id]
                    )
                    .returning(SourceItem.id)
                )
                if (await session.execute(statement)).scalar_one_or_none() is not None:
                    inserted += 1

            row.cursor = batch.next_cursor
            row.next_poll_at = now + timedelta(seconds=row.poll_interval_seconds)
            row.lease_token = None
            row.lease_until = None
            row.poll_attempts = 0
            row.last_error = None
            row.updated_at = now
            return inserted

    async def mark_source_poll_failed(
        self,
        subscription_id: int,
        lease_token: str,
        error: str,
        now: datetime,
    ) -> None:
        if now.tzinfo is None:
            raise ValueError("source clock must be timezone-aware")
        now = now.astimezone(UTC)
        async with self._session_factory.begin() as session:
            row = await self._get_for_update(session, subscription_id)
            self._verify_lease(row, lease_token, now)
            backoff = min(300, 5 * (2 ** min(row.poll_attempts - 1, 6)))
            row.next_poll_at = now + timedelta(seconds=backoff)
            row.lease_token = None
            row.lease_until = None
            row.last_error = _safe_error(error)
            row.updated_at = now

    async def _get_for_update(
        self,
        session: AsyncSession,
        subscription_id: int,
    ) -> SourceSubscription:
        row = (
            await session.scalars(
                select(SourceSubscription)
                .where(SourceSubscription.id == subscription_id)
                .with_for_update()
            )
        ).one_or_none()
        if row is None:
            raise KeyError(f"source subscription not found: {subscription_id}")
        return row

    @staticmethod
    def _verify_lease(
        row: SourceSubscription,
        lease_token: str,
        now: datetime,
    ) -> None:
        if (
            not lease_token
            or row.lease_token != lease_token
            or row.lease_until is None
            or row.lease_until <= now
        ):
            raise SourceLeaseLost(f"source lease is no longer owned: {row.id}")


def _subscription_record(row: SourceSubscription) -> SourceSubscriptionRecord:
    return SourceSubscriptionRecord(
        id=row.id,
        source=row.source,
        dedupe_key=row.dedupe_key,
        session_id=row.session_id,
        conversation_id=row.conversation_id,
        profile=row.profile,
        config=dict(row.config or {}),
        cursor=dict(row.cursor or {}),
        poll_interval_seconds=row.poll_interval_seconds,
        item_ttl_seconds=row.item_ttl_seconds,
        cooldown_seconds=row.cooldown_seconds,
        priority=row.priority,
        next_poll_at=row.next_poll_at,
        enabled=row.enabled,
        poll_attempts=row.poll_attempts,
        lease_token=row.lease_token,
        lease_until=row.lease_until,
        last_error=row.last_error,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _safe_error(error: str) -> str:
    return error.replace("\x00", "")[:500]
