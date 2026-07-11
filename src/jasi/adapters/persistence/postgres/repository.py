from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, delete, exists, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased

from jasi.adapters.persistence.postgres.db import (
    Conversation,
    InitiativeState,
    Message,
    OutboxMessage,
    ToolExecution,
    Turn,
    WorkItem,
)
from jasi.domain.models import ConversationRecord, MessageRecord, OutboxRecord
from jasi.runtime.models import ToolExecutionRecord, TurnResult, TurnStart, Usage

OUTBOX_BACKOFF_SECONDS = [2, 10, 30, 120, 300]
OUTBOX_MAX_ATTEMPTS = 5


class SQLAlchemyRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get_conversation(self, conversation_id: int) -> ConversationRecord | None:
        async with self._session_factory() as session:
            row = await session.get(Conversation, conversation_id)
            return _conversation_record(row) if row is not None else None

    async def load_history(
        self,
        conversation_id: int,
        before_sequence: int | None,
        limit: int,
    ) -> list[MessageRecord]:
        async with self._session_factory() as session:
            filters = [
                Message.conversation_id == conversation_id,
                (
                    (Message.role == "user")
                    | (
                        (Message.role == "assistant")
                        & (Message.delivery_status == "sent")
                        & (Message.origin != "system_error")
                    )
                ),
            ]
            if before_sequence is not None:
                filters.append(Message.sequence < before_sequence)
            stmt = (
                select(Message)
                .where(*filters)
                .order_by(Message.sequence.desc())
                .limit(limit)
            )
            rows = list((await session.scalars(stmt)).all())
            return [_message_record(row) for row in reversed(rows)]

    async def start_turn(
        self,
        work_id: int,
        conversation_id: int,
        profile: str,
        model: str,
        metadata: dict,
    ) -> TurnStart:
        async with self._session_factory.begin() as session:
            row = (
                await session.scalars(
                    select(Turn)
                    .where(Turn.work_item_id == work_id)
                    .with_for_update()
                )
            ).one_or_none()
            if row is not None and row.status in {"succeeded", "failed"}:
                if row.final_text is not None:
                    tool_rows = list(
                        (
                            await session.scalars(
                                select(ToolExecution)
                                .where(ToolExecution.turn_id == row.id)
                                .order_by(ToolExecution.id)
                            )
                        ).all()
                    )
                    return TurnStart(
                        turn_id=row.id,
                        cached_result=_turn_result(row, tool_rows),
                    )

            if row is not None:
                await session.execute(delete(ToolExecution).where(ToolExecution.turn_id == row.id))
                row.conversation_id = conversation_id
                row.profile = profile
                row.model = model
                row.status = "running"
                row.step_count = 0
                row.usage = {}
                row.error_code = None
                row.error_message = None
                row.final_text = None
                row.meta = metadata
                row.started_at = datetime.now(UTC)
                row.completed_at = None
                return TurnStart(turn_id=row.id)

            row = Turn(
                conversation_id=conversation_id,
                work_item_id=work_id,
                profile=profile,
                model=model,
                status="running",
                meta=metadata,
            )
            session.add(row)
            await session.flush()
            return TurnStart(turn_id=row.id)

    async def finish_turn(
        self,
        turn_id: int,
        status: str,
        final_text: str,
        step_count: int,
        usage: Usage,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        async with self._session_factory.begin() as session:
            await session.execute(
                update(Turn)
                .where(Turn.id == turn_id)
                .values(
                    status=status,
                    final_text=final_text,
                    step_count=step_count,
                    usage=usage.to_dict(),
                    error_code=error_code,
                    error_message=_safe_error(error_message),
                    completed_at=datetime.now(UTC),
                )
            )

    async def record_tool_execution(self, turn_id: int, record: ToolExecutionRecord) -> None:
        async with self._session_factory.begin() as session:
            session.add(
                ToolExecution(
                    turn_id=turn_id,
                    tool_name=record.name,
                    arguments=record.arguments,
                    result=record.result,
                    risk=record.risk,
                    status=record.status,
                    duration_ms=record.duration_ms,
                    error_message=_safe_error(record.error_message),
                )
            )

    async def get_outbox(self, outbox_id: int) -> OutboxRecord | None:
        async with self._session_factory() as session:
            row = await session.get(OutboxMessage, outbox_id)
            return _outbox_record(row) if row is not None else None

    async def claim_outbox_batch(self, limit: int) -> list[OutboxRecord]:
        async with self._session_factory.begin() as session:
            now = datetime.now(UTC)
            stale_lock_before = now - timedelta(minutes=5)
            prior = aliased(OutboxMessage)
            stmt = (
                select(OutboxMessage)
                .where(
                    or_(
                        and_(
                            OutboxMessage.status == "pending",
                            OutboxMessage.next_attempt_at <= now,
                        ),
                        and_(
                            OutboxMessage.status == "delivering",
                            OutboxMessage.locked_at <= stale_lock_before,
                        ),
                    ),
                    ~exists(
                        select(prior.id).where(
                            prior.message_id == OutboxMessage.message_id,
                            prior.segment_index < OutboxMessage.segment_index,
                            prior.status != "sent",
                        )
                    ),
                )
                .order_by(OutboxMessage.created_at, OutboxMessage.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            rows = list((await session.scalars(stmt)).all())
            for row in rows:
                row.status = "delivering"
                row.locked_at = now
                row.updated_at = now
            return [_outbox_record(row) for row in rows]

    async def mark_outbox_sent(self, outbox_id: int, external_message_id: str | None) -> None:
        async with self._session_factory.begin() as session:
            row = await self._get_outbox_for_update(session, outbox_id)
            now = datetime.now(UTC)
            row.status = "sent"
            row.sent_at = now
            row.external_message_id = external_message_id
            row.last_error = None
            row.updated_at = now

            remaining = await session.scalar(
                select(func.count())
                .select_from(OutboxMessage)
                .where(
                    OutboxMessage.message_id == row.message_id,
                    OutboxMessage.status != "sent",
                )
            )
            if remaining == 0:
                await session.execute(
                    update(Message)
                    .where(Message.id == row.message_id)
                    .values(delivery_status="sent")
                )
                work = (
                    await session.scalars(
                        select(WorkItem).where(WorkItem.output_message_id == row.message_id)
                    )
                ).one_or_none()
                if work is not None and work.conversation_id is not None:
                    await session.execute(
                        pg_insert(InitiativeState)
                        .values(
                            session_id=work.session_id,
                            conversation_id=work.conversation_id,
                            last_delivery_at=now,
                            updated_at=now,
                        )
                        .on_conflict_do_update(
                            index_elements=[InitiativeState.session_id],
                            set_={
                                "last_delivery_at": func.greatest(
                                    InitiativeState.last_delivery_at,
                                    now,
                                ),
                                "updated_at": now,
                            },
                        )
                    )

    async def mark_outbox_failed_attempt(self, outbox_id: int, error: str, retryable: bool) -> None:
        async with self._session_factory.begin() as session:
            row = await self._get_outbox_for_update(session, outbox_id)
            next_attempts = row.attempts + 1
            now = datetime.now(UTC)
            exhausted = (not retryable) or next_attempts >= OUTBOX_MAX_ATTEMPTS
            row.attempts = next_attempts
            row.last_error = _safe_error(error)
            row.updated_at = now
            row.locked_at = None
            if exhausted:
                row.status = "failed"
                row.next_attempt_at = now
                await session.execute(
                    update(OutboxMessage)
                    .where(
                        OutboxMessage.message_id == row.message_id,
                        OutboxMessage.segment_index > row.segment_index,
                        OutboxMessage.status.in_(("pending", "delivering")),
                    )
                    .values(
                        status="failed",
                        locked_at=None,
                        last_error="previous segment failed",
                        updated_at=now,
                    )
                )
                await session.execute(
                    update(Message)
                    .where(Message.id == row.message_id)
                    .values(delivery_status="failed")
                )
                return

            backoff_index = min(next_attempts - 1, len(OUTBOX_BACKOFF_SECONDS) - 1)
            backoff = OUTBOX_BACKOFF_SECONDS[backoff_index]
            row.status = "pending"
            row.next_attempt_at = now + timedelta(seconds=backoff)

    async def _get_outbox_for_update(self, session: AsyncSession, outbox_id: int) -> OutboxMessage:
        row = (
            await session.scalars(
                select(OutboxMessage).where(OutboxMessage.id == outbox_id).with_for_update()
            )
        ).one_or_none()
        if row is None:
            raise KeyError(f"outbox message not found: {outbox_id}")
        return row

def _conversation_record(row: Conversation) -> ConversationRecord:
    return ConversationRecord(
        id=row.id,
        channel=row.channel,
        external_chat_id=row.external_chat_id,
        metadata=dict(row.meta or {}),
        created_at=row.created_at,
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


def _tool_record(row: ToolExecution) -> ToolExecutionRecord:
    return ToolExecutionRecord(
        name=row.tool_name,
        arguments=dict(row.arguments or {}),
        result=dict(row.result or {}),
        risk=row.risk,
        status=row.status,
        duration_ms=row.duration_ms,
        error_message=row.error_message,
    )


def _turn_result(row: Turn, tool_rows: list[ToolExecution]) -> TurnResult:
    usage = dict(row.usage or {})
    return TurnResult(
        turn_id=row.id,
        status=row.status,
        final_text=row.final_text or "",
        tool_records=[_tool_record(tool_row) for tool_row in tool_rows],
        usage=Usage(
            prompt_tokens=int(usage.get("prompt_tokens") or 0),
            completion_tokens=int(usage.get("completion_tokens") or 0),
            total_tokens=int(usage.get("total_tokens") or 0),
        ),
        error_code=row.error_code,
        error_message=row.error_message,
    )


def _safe_error(error: str | None) -> str | None:
    if error is None:
        return None
    return error.replace("\x00", "")[:500]
