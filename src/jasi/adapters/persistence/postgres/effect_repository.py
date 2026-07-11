from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jasi.adapters.persistence.postgres.db import EffectOutbox
from jasi.domain.effect import EffectRecord

EFFECT_BACKOFF_SECONDS = [2, 10, 30, 120, 300]
EFFECT_MAX_ATTEMPTS = 5


class SQLAlchemyEffectRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get_effect(self, effect_id: int) -> EffectRecord | None:
        async with self._session_factory() as session:
            row = await session.get(EffectOutbox, effect_id)
            return _effect_record(row) if row is not None else None

    async def claim_effect_batch(self, limit: int) -> list[EffectRecord]:
        if limit <= 0:
            return []
        async with self._session_factory.begin() as session:
            now = datetime.now(UTC)
            stale_before = now - timedelta(minutes=5)
            rows = list(
                (
                    await session.scalars(
                        select(EffectOutbox)
                        .where(
                            or_(
                                and_(
                                    EffectOutbox.status == "pending",
                                    EffectOutbox.next_attempt_at <= now,
                                ),
                                and_(
                                    EffectOutbox.status == "executing",
                                    EffectOutbox.locked_at <= stale_before,
                                ),
                            )
                        )
                        .order_by(EffectOutbox.created_at, EffectOutbox.id)
                        .limit(limit)
                        .with_for_update(skip_locked=True)
                    )
                ).all()
            )
            for row in rows:
                row.status = "executing"
                row.locked_at = now
                row.updated_at = now
            return [_effect_record(row) for row in rows]

    async def mark_effect_succeeded(self, effect_id: int, result: dict) -> None:
        async with self._session_factory.begin() as session:
            row = await self._get_for_update(session, effect_id)
            if row.status == "succeeded":
                return
            if row.status != "executing":
                raise ValueError(f"effect is not executing: {effect_id}")
            now = datetime.now(UTC)
            row.status = "succeeded"
            row.result = result
            row.last_error = None
            row.locked_at = None
            row.completed_at = now
            row.updated_at = now

    async def mark_effect_failed_attempt(
        self,
        effect_id: int,
        error: str,
        retryable: bool,
    ) -> None:
        async with self._session_factory.begin() as session:
            row = await self._get_for_update(session, effect_id)
            if row.status != "executing":
                return
            now = datetime.now(UTC)
            attempts = row.attempts + 1
            exhausted = not retryable or attempts >= EFFECT_MAX_ATTEMPTS
            row.attempts = attempts
            row.last_error = _safe_error(error)
            row.locked_at = None
            row.updated_at = now
            if exhausted:
                row.status = "failed"
                row.completed_at = now
                return
            row.status = "pending"
            row.next_attempt_at = now + timedelta(
                seconds=EFFECT_BACKOFF_SECONDS[min(attempts - 1, 4)]
            )

    async def _get_for_update(self, session: AsyncSession, effect_id: int) -> EffectOutbox:
        row = (
            await session.scalars(
                select(EffectOutbox)
                .where(EffectOutbox.id == effect_id)
                .with_for_update()
            )
        ).one_or_none()
        if row is None:
            raise KeyError(f"effect not found: {effect_id}")
        return row


def _effect_record(row: EffectOutbox) -> EffectRecord:
    return EffectRecord(
        id=row.id,
        adapter=row.adapter,
        operation=row.operation,
        dedupe_key=row.dedupe_key,
        payload=dict(row.payload or {}),
        status=row.status,
        attempts=row.attempts,
        next_attempt_at=row.next_attempt_at,
        result=dict(row.result or {}),
        last_error=row.last_error,
        created_at=row.created_at,
    )


def _safe_error(error: str) -> str:
    return error.replace("\x00", "")[:500]
