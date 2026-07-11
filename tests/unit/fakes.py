from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

from jasi.domain.models import (
    ConversationRecord,
    DeliveryResult,
    MessageRecord,
    OutboundMessage,
    OutboundPart,
    OutboxRecord,
)
from jasi.domain.work import (
    WorkCompletion,
    WorkEnqueueResult,
    WorkExecutionResult,
    WorkLeaseLost,
    WorkRecord,
    WorkSpec,
)
from jasi.runtime.models import (
    ModelRequest,
    ModelResponse,
    ToolExecutionRecord,
    TurnResult,
    TurnStart,
    Usage,
)


class FakeModel:
    def __init__(self, responses: list[ModelResponse | Exception]) -> None:
        self.responses = responses
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class FakeChannel:
    def __init__(self, results: list[DeliveryResult] | None = None) -> None:
        self.results = results or []
        self.sent: list[OutboundMessage] = []
        self.sent_event = asyncio.Event()

    async def send(self, message: OutboundMessage) -> DeliveryResult:
        self.sent.append(message)
        self.sent_event.set()
        if self.results:
            return self.results.pop(0)
        return DeliveryResult(success=True, external_message_id=f"sent-{len(self.sent)}")


class FakeOutboundPolicy:
    def prepare(self, text: str) -> tuple[OutboundPart, ...]:
        return (OutboundPart(text=text),)


class FakeRepository:
    def __init__(self) -> None:
        self._next_conversation_id = 1
        self._next_message_id = 1
        self._next_turn_id = 1
        self._next_outbox_id = 1
        self.conversations: dict[tuple[str, str], ConversationRecord] = {}
        self.messages: list[MessageRecord] = []
        self.turns: dict[int, dict[str, Any]] = {}
        self.tool_records: list[tuple[int, ToolExecutionRecord]] = []
        self.outbox: dict[int, OutboxRecord] = {}

    def add_conversation(
        self,
        *,
        conversation_id: int = 1,
        channel: str = "telegram",
        external_chat_id: str = "1",
    ) -> ConversationRecord:
        now = datetime.now(UTC)
        row = ConversationRecord(
            id=conversation_id,
            channel=channel,
            external_chat_id=external_chat_id,
            created_at=now,
            updated_at=now,
        )
        self.conversations[(channel, external_chat_id)] = row
        self._next_conversation_id = max(self._next_conversation_id, conversation_id + 1)
        return row

    async def get_conversation(self, conversation_id: int) -> ConversationRecord | None:
        return next(
            (row for row in self.conversations.values() if row.id == conversation_id),
            None,
        )

    def add_message(
        self,
        *,
        conversation_id: int = 1,
        role: str,
        content: str,
        sequence: int,
        origin: str = "model",
        delivery_status: str = "sent",
        turn_id: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> MessageRecord:
        row = MessageRecord(
            id=self._next_message_id,
            conversation_id=conversation_id,
            role=role,
            origin=origin,
            sequence=sequence,
            content=content,
            delivery_status=delivery_status,
            turn_id=turn_id,
            metadata=metadata or {},
            created_at=datetime.now(UTC),
        )
        self._next_message_id += 1
        self.messages.append(row)
        return row

    async def load_history(
        self,
        conversation_id: int,
        before_sequence: int | None,
        limit: int,
    ) -> list[MessageRecord]:
        rows = [
            row
            for row in self.messages
            if row.conversation_id == conversation_id
            and (before_sequence is None or row.sequence < before_sequence)
            and (
                row.role == "user"
                or (
                    row.role == "assistant"
                    and row.origin != "system_error"
                    and row.delivery_status == "sent"
                )
            )
        ]
        return sorted(rows, key=lambda row: row.sequence)[-limit:]

    async def start_turn(
        self,
        work_id: int,
        conversation_id: int,
        profile: str,
        model: str,
        metadata: dict,
    ) -> TurnStart:
        for turn_id, turn in self.turns.items():
            if turn["work_id"] != work_id:
                continue
            if turn["status"] in {"succeeded", "failed"} and turn.get("final_text") is not None:
                records = [
                    record
                    for record_turn_id, record in self.tool_records
                    if record_turn_id == turn_id
                ]
                return TurnStart(
                    turn_id=turn_id,
                    cached_result=TurnResult(
                        turn_id=turn_id,
                        status=turn["status"],
                        final_text=turn["final_text"],
                        tool_records=records,
                        usage=turn["usage"],
                        error_code=turn.get("error_code"),
                        error_message=turn.get("error_message"),
                    ),
                )
            self.tool_records = [item for item in self.tool_records if item[0] != turn_id]
            turn.update(
                {
                    "conversation_id": conversation_id,
                    "profile": profile,
                    "model": model,
                    "metadata": metadata,
                    "status": "running",
                }
            )
            return TurnStart(turn_id=turn_id)

        turn_id = self._next_turn_id
        self._next_turn_id += 1
        self.turns[turn_id] = {
            "work_id": work_id,
            "conversation_id": conversation_id,
            "profile": profile,
            "model": model,
            "metadata": metadata,
            "status": "running",
        }
        return TurnStart(turn_id=turn_id)

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
        self.turns[turn_id].update(
            {
                "status": status,
                "final_text": final_text,
                "step_count": step_count,
                "usage": usage,
                "error_code": error_code,
                "error_message": error_message,
            }
        )

    async def record_tool_execution(self, turn_id: int, record: ToolExecutionRecord) -> None:
        self.tool_records.append((turn_id, record))

    def create_outbox_response(
        self,
        *,
        channel: str = "telegram",
        external_chat_id: str = "1",
        text: str = "reply",
        parts: tuple[OutboundPart, ...] | None = None,
    ) -> tuple[MessageRecord, list[OutboxRecord]]:
        conversation = self.conversations.get((channel, external_chat_id))
        if conversation is None:
            conversation = self.add_conversation(
                channel=channel,
                external_chat_id=external_chat_id,
            )
        sequence = 1 + max(
            [row.sequence for row in self.messages if row.conversation_id == conversation.id],
            default=0,
        )
        message = self.add_message(
            conversation_id=conversation.id,
            role="assistant",
            origin="model",
            sequence=sequence,
            content=text,
            delivery_status="pending",
        )
        records: list[OutboxRecord] = []
        outbound_parts = parts or (OutboundPart(text=text),)
        for index, part in enumerate(outbound_parts):
            row = OutboxRecord(
                id=self._next_outbox_id,
                conversation_id=conversation.id,
                message_id=message.id,
                channel=channel,
                external_chat_id=external_chat_id,
                segment_index=index,
                segment_count=len(outbound_parts),
                text=part.text,
                status="pending",
                attempts=0,
                next_attempt_at=datetime.now(UTC),
            )
            self._next_outbox_id += 1
            self.outbox[row.id] = row
            records.append(row)
        return message, records

    async def get_outbox(self, outbox_id: int) -> OutboxRecord | None:
        return self.outbox.get(outbox_id)

    async def claim_outbox_batch(self, limit: int) -> list[OutboxRecord]:
        now = datetime.now(UTC)
        ready = [
            row
            for row in self.outbox.values()
            if row.status == "pending"
            and row.next_attempt_at <= now
            and all(
                prior.status == "sent"
                for prior in self.outbox.values()
                if prior.message_id == row.message_id
                and prior.segment_index < row.segment_index
            )
        ]
        ready.sort(key=lambda row: row.id)
        claimed: list[OutboxRecord] = []
        for row in ready[:limit]:
            claimed_row = replace(row, status="delivering")
            self.outbox[row.id] = claimed_row
            claimed.append(claimed_row)
        return claimed

    async def mark_outbox_sent(self, outbox_id: int, external_message_id: str | None) -> None:
        row = self.outbox[outbox_id]
        self.outbox[outbox_id] = replace(
            row,
            status="sent",
            external_message_id=external_message_id,
            last_error=None,
        )
        if all(
            item.status == "sent"
            for item in self.outbox.values()
            if item.message_id == row.message_id
        ):
            self._mark_message_status(row.message_id, "sent")

    async def mark_outbox_failed_attempt(self, outbox_id: int, error: str, retryable: bool) -> None:
        row = self.outbox[outbox_id]
        attempts = row.attempts + 1
        status = "pending" if retryable and attempts < 5 else "failed"
        self.outbox[outbox_id] = replace(
            row,
            attempts=attempts,
            status=status,
            last_error=error,
            next_attempt_at=datetime.now(UTC) + timedelta(seconds=2),
        )
        if status == "failed":
            for item_id, item in list(self.outbox.items()):
                if item.message_id == row.message_id and item.segment_index > row.segment_index:
                    self.outbox[item_id] = replace(
                        item,
                        status="failed",
                        last_error="previous segment failed",
                    )
            self._mark_message_status(row.message_id, "failed")

    def _mark_message_status(self, message_id: int, status: str) -> None:
        self.messages = [
            replace(row, delivery_status=status) if row.id == message_id else row
            for row in self.messages
        ]

class FakeWorkRepository:
    def __init__(self) -> None:
        self._next_work_id = 1
        self._next_lease_id = 1
        self._next_message_id = 1
        self._next_outbox_id = 1
        self.work: dict[int, WorkRecord] = {}
        self.completions: dict[int, WorkCompletion] = {}
        self.complete_calls: list[
            tuple[int, str, WorkExecutionResult, tuple[OutboundPart, ...]]
        ] = []
        self.fail_next_completion = False

    async def enqueue_work(self, spec: WorkSpec) -> WorkEnqueueResult:
        existing = next(
            (row for row in self.work.values() if row.dedupe_key == spec.dedupe_key),
            None,
        )
        if existing is not None:
            return WorkEnqueueResult(work=existing, created=False)

        now = datetime.now(UTC)
        row = WorkRecord(
            id=self._next_work_id,
            kind=spec.kind,
            action=spec.action,
            dedupe_key=spec.dedupe_key,
            session_id=spec.session_id,
            conversation_id=spec.conversation_id,
            inbound_event_id=spec.inbound_event_id,
            profile=spec.profile,
            input_text=spec.input_text,
            payload=dict(spec.payload),
            priority=spec.priority,
            status="pending",
            attempts=0,
            max_attempts=spec.max_attempts,
            available_at=spec.available_at,
            created_at=now,
            updated_at=now,
        )
        self._next_work_id += 1
        self.work[row.id] = row
        return WorkEnqueueResult(work=row, created=True)

    async def get_work(self, work_id: int) -> WorkRecord | None:
        return self.work.get(work_id)

    async def claim_work_batch(
        self,
        limit: int,
        lease_seconds: float,
    ) -> list[WorkRecord]:
        now = datetime.now(UTC)
        for work_id, row in list(self.work.items()):
            if row.status == "running" and row.lease_until is not None and row.lease_until <= now:
                self.work[work_id] = replace(
                    row,
                    status="pending",
                    lease_token=None,
                    lease_until=None,
                    available_at=now,
                    updated_at=now,
                )

        running_sessions = {
            row.session_id for row in self.work.values() if row.status == "running"
        }
        candidates = sorted(
            (
                row
                for row in self.work.values()
                if row.status == "pending" and row.available_at <= now
            ),
            key=lambda row: (-row.priority, row.available_at, row.created_at, row.id),
        )
        claimed: list[WorkRecord] = []
        for row in candidates:
            if len(claimed) >= limit:
                break
            if row.session_id in running_sessions:
                continue
            token = f"lease-{self._next_lease_id}"
            self._next_lease_id += 1
            claimed_row = replace(
                row,
                status="running",
                attempts=row.attempts + 1,
                lease_token=token,
                lease_until=now + timedelta(seconds=lease_seconds),
                started_at=row.started_at or now,
                last_error=None,
                updated_at=now,
            )
            self.work[row.id] = claimed_row
            claimed.append(claimed_row)
            running_sessions.add(row.session_id)
        return claimed

    async def complete_work(
        self,
        work_id: int,
        lease_token: str,
        result: WorkExecutionResult,
        parts: tuple[OutboundPart, ...],
    ) -> WorkCompletion:
        row = self.work[work_id]
        if row.status == "succeeded":
            return self.completions[work_id]
        self._verify_lease(row, lease_token)
        if self.fail_next_completion:
            self.fail_next_completion = False
            raise RuntimeError("simulated work completion failure")

        self.complete_calls.append((work_id, lease_token, result, parts))
        message = None
        outbox: list[OutboxRecord] = []
        output_message_id = None
        if result.outbound is not None:
            if row.conversation_id is None or not parts:
                raise ValueError("outbound work requires a conversation and parts")
            output_message_id = self._next_message_id
            self._next_message_id += 1
            message = MessageRecord(
                id=output_message_id,
                conversation_id=row.conversation_id,
                role="assistant",
                origin=result.outbound.origin,
                sequence=output_message_id,
                content=result.outbound.text,
                delivery_status="pending",
                turn_id=result.turn_id,
                metadata={
                    **result.outbound.metadata,
                    "work_id": row.id,
                    "work_kind": row.kind,
                },
            )
            for index, part in enumerate(parts):
                outbox.append(
                    OutboxRecord(
                        id=self._next_outbox_id,
                        conversation_id=row.conversation_id,
                        message_id=message.id,
                        channel=result.outbound.channel,
                        external_chat_id=result.outbound.external_chat_id,
                        segment_index=index,
                        segment_count=len(parts),
                        text=part.text,
                        status="pending",
                        attempts=0,
                        next_attempt_at=datetime.now(UTC),
                    )
                )
                self._next_outbox_id += 1

        now = datetime.now(UTC)
        self.work[work_id] = replace(
            row,
            status="succeeded",
            lease_token=None,
            lease_until=None,
            output_message_id=output_message_id,
            completed_at=now,
            updated_at=now,
        )
        completion = WorkCompletion(message=message, outbox=tuple(outbox))
        self.completions[work_id] = completion
        return completion

    async def mark_work_failed_attempt(
        self,
        work_id: int,
        lease_token: str,
        error: str,
    ) -> None:
        row = self.work[work_id]
        self._verify_lease(row, lease_token)
        now = datetime.now(UTC)
        exhausted = row.attempts >= row.max_attempts
        self.work[work_id] = replace(
            row,
            status="failed" if exhausted else "pending",
            lease_token=None,
            lease_until=None,
            available_at=now if exhausted else now + timedelta(seconds=2),
            completed_at=now if exhausted else None,
            last_error=error[:500],
            updated_at=now,
        )

    def make_ready(self, work_id: int) -> None:
        row = self.work[work_id]
        self.work[work_id] = replace(row, available_at=datetime.now(UTC))

    @staticmethod
    def _verify_lease(row: WorkRecord, lease_token: str) -> None:
        if row.status != "running" or row.lease_token != lease_token:
            raise WorkLeaseLost(f"work lease is no longer owned: {row.id}")
