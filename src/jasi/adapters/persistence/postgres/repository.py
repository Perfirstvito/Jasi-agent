from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import and_, delete, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jasi.adapters.persistence.postgres.db import (
    Conversation,
    InboundEvent,
    Message,
    OutboxMessage,
    ToolExecution,
    Turn,
)
from jasi.domain.models import (
    ConversationRecord,
    InboundClaim,
    InboundMessage,
    MessageRecord,
    OutboundPart,
    OutboxRecord,
)
from jasi.runtime.models import ToolExecutionRecord, TurnResult, TurnStart, Usage

OUTBOX_BACKOFF_SECONDS = [2, 10, 30, 120, 300]
OUTBOX_MAX_ATTEMPTS = 5


class SQLAlchemyRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def claim_inbound_message(self, message: InboundMessage) -> InboundClaim | None:
        async with self._session_factory.begin() as session:
            conversation_id = await self._upsert_conversation(
                session,
                channel=message.channel,
                external_chat_id=message.external_chat_id,
                metadata={"last_external_user_id": message.external_user_id},
            )

            event_stmt = (
                pg_insert(InboundEvent)
                .values(
                    channel=message.channel,
                    external_update_id=message.external_update_id,
                    conversation_id=conversation_id,
                    external_user_id=message.external_user_id,
                    status="processing",
                    attempts=1,
                    payload=message.metadata,
                )
                .on_conflict_do_nothing(
                    index_elements=[InboundEvent.channel, InboundEvent.external_update_id]
                )
                .returning(InboundEvent.id)
            )
            event_id = (await session.execute(event_stmt)).scalar_one_or_none()
            if event_id is None:
                event = (
                    await session.scalars(
                        select(InboundEvent)
                        .where(
                            InboundEvent.channel == message.channel,
                            InboundEvent.external_update_id == message.external_update_id,
                        )
                        .with_for_update()
                    )
                ).one()
                if event.status == "completed":
                    return None
                if event.conversation_id is None or event.message_id is None:
                    raise RuntimeError("incomplete inbound event cannot be resumed")
                event.status = "processing"
                event.attempts += 1
                event.last_error = None
                event.updated_at = datetime.now(UTC)
                conversation = await session.get(Conversation, event.conversation_id)
                row = await session.get(Message, event.message_id)
                if conversation is None or row is None:
                    raise RuntimeError("inbound event references missing records")
                return InboundClaim(
                    event_id=event.id,
                    conversation=_conversation_record(conversation),
                    message=_message_record(row),
                )

            sequence = await self._next_message_sequence(session, conversation_id)
            row = Message(
                conversation_id=conversation_id,
                role="user",
                origin=message.channel,
                sequence=sequence,
                content=message.text,
                delivery_status="sent",
                meta=message.metadata,
            )
            session.add(row)
            await session.flush()
            await session.execute(
                update(InboundEvent).where(InboundEvent.id == event_id).values(message_id=row.id)
            )
            conversation = await session.get(Conversation, conversation_id)
            if conversation is None:
                raise RuntimeError("conversation disappeared during inbound registration")
            return InboundClaim(
                event_id=event_id,
                conversation=_conversation_record(conversation),
                message=_message_record(row),
            )

    async def release_inbound(self, event_id: int, error: str) -> None:
        async with self._session_factory.begin() as session:
            event = await self._get_inbound_for_update(session, event_id)
            if event.status == "completed":
                return
            event.status = "pending"
            event.last_error = _safe_error(error)
            event.updated_at = datetime.now(UTC)

    async def load_history_before(
        self, conversation_id: int, before_sequence: int, limit: int
    ) -> list[MessageRecord]:
        async with self._session_factory() as session:
            stmt = (
                select(Message)
                .where(
                    Message.conversation_id == conversation_id,
                    Message.sequence < before_sequence,
                    (
                        (Message.role == "user")
                        | (
                            (Message.role == "assistant")
                            & (Message.delivery_status == "sent")
                            & (Message.origin == "model")
                        )
                    ),
                )
                .order_by(Message.sequence.desc())
                .limit(limit)
            )
            rows = list((await session.scalars(stmt)).all())
            return [_message_record(row) for row in reversed(rows)]

    async def start_turn(
        self,
        conversation_id: int,
        inbound_message_id: int,
        profile: str,
        model: str,
        metadata: dict,
    ) -> TurnStart:
        async with self._session_factory.begin() as session:
            row = (
                await session.scalars(
                    select(Turn)
                    .where(Turn.inbound_message_id == inbound_message_id)
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
                inbound_message_id=inbound_message_id,
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

    async def complete_inbound_response(
        self,
        inbound_event_id: int,
        conversation_id: int,
        channel: str,
        external_chat_id: str,
        turn_id: int,
        text: str,
        parts: tuple[OutboundPart, ...],
        origin: str,
        metadata: dict,
    ) -> tuple[MessageRecord, list[OutboxRecord]]:
        if not parts:
            raise ValueError("assistant response requires at least one outbound part")
        async with self._session_factory.begin() as session:
            event = await self._get_inbound_for_update(session, inbound_event_id)
            if event.status == "completed":
                return await self._load_response(session, turn_id)
            if event.conversation_id != conversation_id:
                raise ValueError("inbound event and response conversation do not match")

            sequence = await self._next_message_sequence(session, conversation_id)
            message = Message(
                conversation_id=conversation_id,
                role="assistant",
                origin=origin,
                sequence=sequence,
                content=text,
                delivery_status="pending",
                turn_id=turn_id,
                meta=metadata,
            )
            session.add(message)
            await session.flush()

            outbox_rows: list[OutboxMessage] = []
            now = datetime.now(UTC)
            for index, part in enumerate(parts):
                outbox = OutboxMessage(
                    conversation_id=conversation_id,
                    message_id=message.id,
                    channel=channel,
                    external_chat_id=external_chat_id,
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
            now = datetime.now(UTC)
            event.status = "completed"
            event.last_error = None
            event.updated_at = now
            event.completed_at = now
            return _message_record(message), [_outbox_record(row) for row in outbox_rows]

    async def get_outbox(self, outbox_id: int) -> OutboxRecord | None:
        async with self._session_factory() as session:
            row = await session.get(OutboxMessage, outbox_id)
            return _outbox_record(row) if row is not None else None

    async def claim_outbox_batch(self, limit: int) -> list[OutboxRecord]:
        async with self._session_factory.begin() as session:
            now = datetime.now(UTC)
            stale_lock_before = now - timedelta(minutes=5)
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
                    )
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
            row.status = "sent"
            row.sent_at = datetime.now(UTC)
            row.external_message_id = external_message_id
            row.last_error = None
            row.updated_at = datetime.now(UTC)

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
                    update(Message)
                    .where(Message.id == row.message_id)
                    .values(delivery_status="failed")
                )
                return

            backoff_index = min(next_attempts - 1, len(OUTBOX_BACKOFF_SECONDS) - 1)
            backoff = OUTBOX_BACKOFF_SECONDS[backoff_index]
            row.status = "pending"
            row.next_attempt_at = now + timedelta(seconds=backoff)

    async def _upsert_conversation(
        self, session: AsyncSession, channel: str, external_chat_id: str, metadata: dict[str, Any]
    ) -> int:
        stmt = (
            pg_insert(Conversation)
            .values(
                channel=channel,
                external_chat_id=external_chat_id,
                meta=metadata,
            )
            .on_conflict_do_update(
                index_elements=[Conversation.channel, Conversation.external_chat_id],
                set_={"updated_at": datetime.now(UTC)},
            )
            .returning(Conversation.id)
        )
        return int((await session.execute(stmt)).scalar_one())

    async def _next_message_sequence(self, session: AsyncSession, conversation_id: int) -> int:
        current = await session.scalar(
            select(func.max(Message.sequence)).where(Message.conversation_id == conversation_id)
        )
        return int(current or 0) + 1

    async def _get_outbox_for_update(self, session: AsyncSession, outbox_id: int) -> OutboxMessage:
        row = (
            await session.scalars(
                select(OutboxMessage).where(OutboxMessage.id == outbox_id).with_for_update()
            )
        ).one_or_none()
        if row is None:
            raise KeyError(f"outbox message not found: {outbox_id}")
        return row

    async def _get_inbound_for_update(self, session: AsyncSession, event_id: int) -> InboundEvent:
        row = (
            await session.scalars(
                select(InboundEvent).where(InboundEvent.id == event_id).with_for_update()
            )
        ).one_or_none()
        if row is None:
            raise KeyError(f"inbound event not found: {event_id}")
        return row

    async def _load_response(
        self, session: AsyncSession, turn_id: int
    ) -> tuple[MessageRecord, list[OutboxRecord]]:
        message = (
            await session.scalars(
                select(Message).where(
                    Message.turn_id == turn_id,
                    Message.role == "assistant",
                )
            )
        ).one_or_none()
        if message is None:
            raise RuntimeError("completed inbound event has no assistant response")
        outbox_rows = list(
            (
                await session.scalars(
                    select(OutboxMessage)
                    .where(OutboxMessage.message_id == message.id)
                    .order_by(OutboxMessage.segment_index)
                )
            ).all()
        )
        return _message_record(message), [_outbox_record(row) for row in outbox_rows]


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
