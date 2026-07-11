from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

from jasi.domain.models import (
    ConversationRecord,
    DeliveryResult,
    InboundClaim,
    InboundMessage,
    MessageRecord,
    OutboundMessage,
    OutboundPart,
    OutboxRecord,
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
        self._next_event_id = 1
        self._next_message_id = 1
        self._next_turn_id = 1
        self._next_outbox_id = 1
        self.inbound_events: dict[tuple[str, str], dict[str, Any]] = {}
        self.conversations: dict[tuple[str, str], ConversationRecord] = {}
        self.messages: list[MessageRecord] = []
        self.turns: dict[int, dict[str, Any]] = {}
        self.tool_records: list[tuple[int, ToolExecutionRecord]] = []
        self.outbox: dict[int, OutboxRecord] = {}
        self.fail_next_completion = False

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

    async def claim_inbound_message(self, message: InboundMessage) -> InboundClaim | None:
        event_key = (message.channel, message.external_update_id)
        event = self.inbound_events.get(event_key)
        if event is not None:
            if event["status"] == "completed":
                return None
            event["status"] = "processing"
            event["attempts"] += 1
            event["last_error"] = None
            return InboundClaim(
                event_id=event["id"],
                conversation=event["conversation"],
                message=event["message"],
            )

        conv_key = (message.channel, message.external_chat_id)
        conversation = self.conversations.get(conv_key)
        if conversation is None:
            now = datetime.now(UTC)
            conversation = ConversationRecord(
                id=self._next_conversation_id,
                channel=message.channel,
                external_chat_id=message.external_chat_id,
                created_at=now,
                updated_at=now,
            )
            self._next_conversation_id += 1
            self.conversations[conv_key] = conversation

        sequence = 1 + max(
            [row.sequence for row in self.messages if row.conversation_id == conversation.id],
            default=0,
        )
        row = self.add_message(
            conversation_id=conversation.id,
            role="user",
            origin=message.channel,
            sequence=sequence,
            content=message.text,
            delivery_status="sent",
        )
        event = {
            "id": self._next_event_id,
            "status": "processing",
            "attempts": 1,
            "last_error": None,
            "conversation": conversation,
            "message": row,
        }
        self._next_event_id += 1
        self.inbound_events[event_key] = event
        return InboundClaim(
            event_id=event["id"],
            conversation=conversation,
            message=row,
        )

    async def release_inbound(self, event_id: int, error: str) -> None:
        event = self._inbound_event(event_id)
        if event["status"] == "completed":
            return
        event["status"] = "pending"
        event["last_error"] = error

    async def load_history_before(
        self, conversation_id: int, before_sequence: int, limit: int
    ) -> list[MessageRecord]:
        rows = [
            row
            for row in self.messages
            if row.conversation_id == conversation_id
            and row.sequence < before_sequence
            and (
                row.role == "user"
                or (
                    row.role == "assistant"
                    and row.origin == "model"
                    and row.delivery_status == "sent"
                )
            )
        ]
        return sorted(rows, key=lambda row: row.sequence)[-limit:]

    async def start_turn(
        self,
        conversation_id: int,
        inbound_message_id: int,
        profile: str,
        model: str,
        metadata: dict,
    ) -> TurnStart:
        for turn_id, turn in self.turns.items():
            if turn["inbound_message_id"] != inbound_message_id:
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
            "conversation_id": conversation_id,
            "inbound_message_id": inbound_message_id,
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
        event = self._inbound_event(inbound_event_id)
        if event["status"] == "completed":
            message = next(row for row in self.messages if row.turn_id == turn_id)
            records = [row for row in self.outbox.values() if row.message_id == message.id]
            return message, sorted(records, key=lambda row: row.segment_index)
        if self.fail_next_completion:
            self.fail_next_completion = False
            raise RuntimeError("simulated response transaction failure")

        sequence = 1 + max(
            [row.sequence for row in self.messages if row.conversation_id == conversation_id],
            default=0,
        )
        message = self.add_message(
            conversation_id=conversation_id,
            role="assistant",
            origin=origin,
            sequence=sequence,
            content=text,
            delivery_status="pending",
            turn_id=turn_id,
            metadata=metadata,
        )
        records: list[OutboxRecord] = []
        for index, part in enumerate(parts):
            outbox = OutboxRecord(
                id=self._next_outbox_id,
                conversation_id=conversation_id,
                message_id=message.id,
                channel=channel,
                external_chat_id=external_chat_id,
                segment_index=index,
                segment_count=len(parts),
                text=part.text,
                status="pending",
                attempts=0,
                next_attempt_at=datetime.now(UTC),
            )
            self._next_outbox_id += 1
            self.outbox[outbox.id] = outbox
            records.append(outbox)
        event["status"] = "completed"
        event["last_error"] = None
        return message, records

    async def get_outbox(self, outbox_id: int) -> OutboxRecord | None:
        return self.outbox.get(outbox_id)

    async def claim_outbox_batch(self, limit: int) -> list[OutboxRecord]:
        now = datetime.now(UTC)
        ready = [
            row
            for row in self.outbox.values()
            if row.status == "pending" and row.next_attempt_at <= now
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
            self._mark_message_status(row.message_id, "failed")

    def _mark_message_status(self, message_id: int, status: str) -> None:
        self.messages = [
            replace(row, delivery_status=status) if row.id == message_id else row
            for row in self.messages
        ]

    def _inbound_event(self, event_id: int) -> dict[str, Any]:
        for event in self.inbound_events.values():
            if event["id"] == event_id:
                return event
        raise KeyError(f"inbound event not found: {event_id}")
