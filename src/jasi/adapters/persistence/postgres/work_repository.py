from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import exists, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased

from jasi.adapters.persistence.postgres.db import (
    InboundEvent,
    Message,
    OutboxMessage,
    WorkItem,
)
from jasi.domain.models import MessageRecord, OutboundPart, OutboxRecord
from jasi.domain.work import (
    WorkCompletion,
    WorkEnqueueResult,
    WorkExecutionResult,
    WorkLeaseLost,
    WorkRecord,
    WorkSpec,
)

WORK_BACKOFF_SECONDS = [2, 10, 30, 120, 300]


class SQLAlchemyWorkRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def enqueue_work(self, spec: WorkSpec) -> WorkEnqueueResult:
        _validate_spec(spec)
        async with self._session_factory.begin() as session:
            statement = (
                pg_insert(WorkItem)
                .values(
                    kind=spec.kind,
                    action=spec.action,
                    dedupe_key=spec.dedupe_key,
                    session_id=spec.session_id,
                    conversation_id=spec.conversation_id,
                    inbound_event_id=spec.inbound_event_id,
                    profile=spec.profile,
                    input_text=spec.input_text,
                    payload=spec.payload,
                    priority=spec.priority,
                    status="pending",
                    available_at=spec.available_at,
                    max_attempts=spec.max_attempts,
                )
                .on_conflict_do_nothing(index_elements=[WorkItem.dedupe_key])
                .returning(WorkItem.id)
            )
            work_id = (await session.execute(statement)).scalar_one_or_none()
            created = work_id is not None
            if work_id is None:
                work_id = await session.scalar(
                    select(WorkItem.id).where(WorkItem.dedupe_key == spec.dedupe_key)
                )
            if work_id is None:
                raise RuntimeError("work disappeared after enqueue conflict")
            row = await session.get(WorkItem, work_id)
            if row is None:
                raise RuntimeError("work disappeared after enqueue")
            return WorkEnqueueResult(work=_work_record(row), created=created)

    async def get_work(self, work_id: int) -> WorkRecord | None:
        async with self._session_factory() as session:
            row = await session.get(WorkItem, work_id)
            return _work_record(row) if row is not None else None

    async def claim_work_batch(
        self,
        limit: int,
        lease_seconds: float,
    ) -> list[WorkRecord]:
        if limit <= 0:
            return []
        if lease_seconds <= 0:
            raise ValueError("work lease must be positive")

        async with self._session_factory.begin() as session:
            now = datetime.now(UTC)
            await session.execute(
                update(WorkItem)
                .where(
                    WorkItem.status == "running",
                    WorkItem.lease_until <= now,
                )
                .values(
                    status="pending",
                    lease_token=None,
                    lease_until=None,
                    available_at=now,
                    updated_at=now,
                )
            )

            rank = func.row_number().over(
                partition_by=WorkItem.session_id,
                order_by=(
                    WorkItem.priority.desc(),
                    WorkItem.available_at,
                    WorkItem.created_at,
                    WorkItem.id,
                ),
            )
            ranked = (
                select(WorkItem.id.label("work_id"), rank.label("session_rank"))
                .where(
                    WorkItem.status == "pending",
                    WorkItem.available_at <= now,
                )
                .subquery()
            )
            running = aliased(WorkItem)
            statement = (
                select(WorkItem)
                .join(ranked, ranked.c.work_id == WorkItem.id)
                .where(
                    ranked.c.session_rank == 1,
                    ~exists(
                        select(running.id).where(
                            running.session_id == WorkItem.session_id,
                            running.status == "running",
                        )
                    ),
                )
                .order_by(
                    WorkItem.priority.desc(),
                    WorkItem.available_at,
                    WorkItem.created_at,
                    WorkItem.id,
                )
                .limit(limit)
                .with_for_update(skip_locked=True, of=WorkItem)
            )
            rows = list((await session.scalars(statement)).all())
            lease_until = now + timedelta(seconds=lease_seconds)
            for row in rows:
                row.status = "running"
                row.attempts += 1
                row.lease_token = uuid4().hex
                row.lease_until = lease_until
                row.started_at = row.started_at or now
                row.last_error = None
                row.updated_at = now
            await session.flush()
            return [_work_record(row) for row in rows]

    async def complete_work(
        self,
        work_id: int,
        lease_token: str,
        result: WorkExecutionResult,
        parts: tuple[OutboundPart, ...],
    ) -> WorkCompletion:
        async with self._session_factory.begin() as session:
            work = await self._get_work_for_update(session, work_id)
            if work.status == "succeeded":
                return await self._load_completion(session, work)
            _verify_lease(work, lease_token)

            message: Message | None = None
            outbox_rows: list[OutboxMessage] = []
            if result.outbound is not None:
                if work.conversation_id is None:
                    raise ValueError("outbound work requires a conversation")
                if not parts:
                    raise ValueError("outbound work requires at least one part")

                sequence = await self._next_message_sequence(session, work.conversation_id)
                message = Message(
                    conversation_id=work.conversation_id,
                    role="assistant",
                    origin=result.outbound.origin,
                    sequence=sequence,
                    content=result.outbound.text,
                    delivery_status="pending",
                    turn_id=result.turn_id,
                    meta={
                        **result.outbound.metadata,
                        "work_id": work.id,
                        "work_kind": work.kind,
                    },
                )
                session.add(message)
                await session.flush()

                now = datetime.now(UTC)
                for index, part in enumerate(parts):
                    outbox = OutboxMessage(
                        conversation_id=work.conversation_id,
                        message_id=message.id,
                        channel=result.outbound.channel,
                        external_chat_id=result.outbound.external_chat_id,
                        segment_index=index,
                        segment_count=len(parts),
                        text=part.text,
                        status="pending",
                        attempts=0,
                        next_attempt_at=now,
                    )
                    session.add(outbox)
                    outbox_rows.append(outbox)
                await session.flush()
                work.output_message_id = message.id
            elif parts:
                raise ValueError("work without outbound cannot have parts")

            now = datetime.now(UTC)
            work.status = "succeeded"
            work.lease_token = None
            work.lease_until = None
            work.completed_at = now
            work.updated_at = now
            work.last_error = None
            if work.inbound_event_id is not None:
                await session.execute(
                    update(InboundEvent)
                    .where(InboundEvent.id == work.inbound_event_id)
                    .values(
                        status="completed",
                        last_error=None,
                        completed_at=now,
                        updated_at=now,
                    )
                )

            return WorkCompletion(
                message=_message_record(message) if message is not None else None,
                outbox=tuple(_outbox_record(row) for row in outbox_rows),
            )

    async def mark_work_failed_attempt(
        self,
        work_id: int,
        lease_token: str,
        error: str,
    ) -> None:
        async with self._session_factory.begin() as session:
            work = await self._get_work_for_update(session, work_id)
            _verify_lease(work, lease_token)
            now = datetime.now(UTC)
            work.last_error = _safe_error(error)
            work.lease_token = None
            work.lease_until = None
            work.updated_at = now
            if work.attempts >= work.max_attempts:
                work.status = "failed"
                work.completed_at = now
                return

            backoff_index = min(work.attempts - 1, len(WORK_BACKOFF_SECONDS) - 1)
            work.status = "pending"
            work.available_at = now + timedelta(seconds=WORK_BACKOFF_SECONDS[backoff_index])

    async def _get_work_for_update(self, session: AsyncSession, work_id: int) -> WorkItem:
        row = (
            await session.scalars(
                select(WorkItem).where(WorkItem.id == work_id).with_for_update()
            )
        ).one_or_none()
        if row is None:
            raise KeyError(f"work item not found: {work_id}")
        return row

    async def _load_completion(
        self,
        session: AsyncSession,
        work: WorkItem,
    ) -> WorkCompletion:
        if work.output_message_id is None:
            return WorkCompletion()
        message = await session.get(Message, work.output_message_id)
        if message is None:
            raise RuntimeError("completed work references a missing message")
        rows = list(
            (
                await session.scalars(
                    select(OutboxMessage)
                    .where(OutboxMessage.message_id == message.id)
                    .order_by(OutboxMessage.segment_index)
                )
            ).all()
        )
        return WorkCompletion(
            message=_message_record(message),
            outbox=tuple(_outbox_record(row) for row in rows),
        )

    async def _next_message_sequence(self, session: AsyncSession, conversation_id: int) -> int:
        current = await session.scalar(
            select(func.max(Message.sequence)).where(Message.conversation_id == conversation_id)
        )
        return int(current or 0) + 1


def _validate_spec(spec: WorkSpec) -> None:
    if not spec.dedupe_key.strip():
        raise ValueError("work dedupe key cannot be empty")
    if not spec.session_id.strip():
        raise ValueError("work session ID cannot be empty")
    if spec.max_attempts <= 0:
        raise ValueError("work max attempts must be positive")
    if spec.action == "agent" and not (spec.profile or "").strip():
        raise ValueError("agent work requires a profile")


def _verify_lease(work: WorkItem, lease_token: str) -> None:
    if work.status != "running" or not lease_token or work.lease_token != lease_token:
        raise WorkLeaseLost(f"work lease is no longer owned: {work.id}")


def _work_record(row: WorkItem) -> WorkRecord:
    return WorkRecord(
        id=row.id,
        kind=row.kind,
        action=row.action,
        dedupe_key=row.dedupe_key,
        session_id=row.session_id,
        conversation_id=row.conversation_id,
        inbound_event_id=row.inbound_event_id,
        profile=row.profile,
        input_text=row.input_text,
        payload=dict(row.payload or {}),
        priority=row.priority,
        status=row.status,
        attempts=row.attempts,
        max_attempts=row.max_attempts,
        available_at=row.available_at,
        lease_token=row.lease_token,
        lease_until=row.lease_until,
        output_message_id=row.output_message_id,
        last_error=row.last_error,
        created_at=row.created_at,
        started_at=row.started_at,
        completed_at=row.completed_at,
        updated_at=row.updated_at,
    )


def _message_record(row: Message) -> MessageRecord:
    return MessageRecord(
        id=row.id,
        conversation_id=row.conversation_id,
        role=row.role,
        origin=row.origin,
        sequence=row.sequence,
        content=row.content,
        delivery_status=row.delivery_status,
        turn_id=row.turn_id,
        metadata=dict(row.meta or {}),
        created_at=row.created_at,
    )


def _outbox_record(row: OutboxMessage) -> OutboxRecord:
    return OutboxRecord(
        id=row.id,
        conversation_id=row.conversation_id,
        message_id=row.message_id,
        channel=row.channel,
        external_chat_id=row.external_chat_id,
        segment_index=row.segment_index,
        segment_count=row.segment_count,
        text=row.text,
        status=row.status,
        attempts=row.attempts,
        next_attempt_at=row.next_attempt_at,
        last_error=row.last_error,
        external_message_id=row.external_message_id,
    )


def _safe_error(error: str) -> str:
    return error.replace("\x00", "")[:500]
