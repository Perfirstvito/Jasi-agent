from __future__ import annotations

from dataclasses import dataclass

from jasi.domain.work import OutboundDraft, WorkExecutionResult, WorkRecord
from jasi.ports.repository import ConversationRepositoryPort


@dataclass(frozen=True)
class DirectWorkCommand:
    kind: str
    conversation_id: int
    text: str

    @classmethod
    def from_record(cls, work: WorkRecord) -> DirectWorkCommand:
        if work.action != "direct":
            raise ValueError(f"direct handler cannot execute action: {work.action}")
        if work.conversation_id is None:
            raise ValueError("direct work requires a conversation")
        if not work.input_text.strip():
            raise ValueError("direct work requires text")
        return cls(
            kind=work.kind,
            conversation_id=work.conversation_id,
            text=work.input_text,
        )


class DirectWorkHandler:
    def __init__(self, conversations: ConversationRepositoryPort) -> None:
        self._conversations = conversations

    async def execute(self, work: WorkRecord) -> WorkExecutionResult:
        command = DirectWorkCommand.from_record(work)
        conversation = await self._conversations.get_conversation(command.conversation_id)
        if conversation is None:
            raise ValueError(f"conversation not found: {command.conversation_id}")
        return WorkExecutionResult(
            outbound=OutboundDraft(
                channel=conversation.channel,
                external_chat_id=conversation.external_chat_id,
                text=command.text,
                origin=command.kind,
            )
        )
