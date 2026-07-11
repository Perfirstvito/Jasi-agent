from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from jasi.domain.models import (
    InboundClaim,
    InboundMessage,
    MessageRecord,
    OutboundPart,
    OutboxRecord,
)

if TYPE_CHECKING:
    from jasi.runtime.models import ToolExecutionRecord, TurnStart, Usage


class RuntimeRepositoryPort(Protocol):
    async def load_history_before(
        self, conversation_id: int, before_sequence: int, limit: int
    ) -> list[MessageRecord]: ...

    async def start_turn(
        self,
        conversation_id: int,
        inbound_message_id: int,
        profile: str,
        model: str,
        metadata: dict,
    ) -> TurnStart: ...

    async def finish_turn(
        self,
        turn_id: int,
        status: str,
        final_text: str,
        step_count: int,
        usage: Usage,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None: ...

    async def record_tool_execution(self, turn_id: int, record: ToolExecutionRecord) -> None: ...


class ChatRepositoryPort(Protocol):
    async def claim_inbound_message(self, message: InboundMessage) -> InboundClaim | None: ...

    async def release_inbound(self, event_id: int, error: str) -> None: ...

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
    ) -> tuple[MessageRecord, list[OutboxRecord]]: ...


class OutboxRepositoryPort(Protocol):
    async def get_outbox(self, outbox_id: int) -> OutboxRecord | None: ...

    async def claim_outbox_batch(self, limit: int) -> list[OutboxRecord]: ...

    async def mark_outbox_sent(self, outbox_id: int, external_message_id: str | None) -> None: ...

    async def mark_outbox_failed_attempt(
        self, outbox_id: int, error: str, retryable: bool
    ) -> None: ...
