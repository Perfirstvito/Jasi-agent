from __future__ import annotations

from dataclasses import dataclass

from jasi.domain.work import OutboundDraft, WorkExecutionResult, WorkRecord
from jasi.ports.repository import ConversationRepositoryPort
from jasi.ports.runtime import AgentRuntimePort
from jasi.runtime.models import ToolGrant, TurnRequest


@dataclass(frozen=True)
class AgentWorkCommand:
    work_id: int
    kind: str
    session_id: str
    conversation_id: int
    profile: str
    input_text: str
    history_before_sequence: int | None
    tool_grant: ToolGrant | None

    @classmethod
    def from_record(cls, work: WorkRecord) -> AgentWorkCommand:
        if work.action != "agent":
            raise ValueError(f"agent handler cannot execute action: {work.action}")
        if work.conversation_id is None:
            raise ValueError("agent work requires a conversation")
        if not (work.profile or "").strip():
            raise ValueError("agent work requires a profile")

        before_sequence = work.payload.get("history_before_sequence")
        if before_sequence is not None and (
            not isinstance(before_sequence, int) or isinstance(before_sequence, bool)
        ):
            raise ValueError("history_before_sequence must be an integer")

        raw_grant = work.payload.get("tool_grant")
        tool_grant: ToolGrant | None = None
        if raw_grant is not None:
            if not isinstance(raw_grant, dict) or not isinstance(raw_grant.get("tools"), list):
                raise ValueError("tool_grant must contain a tools list")
            raw_names = raw_grant["tools"]
            if any(not isinstance(name, str) or not name.strip() for name in raw_names):
                raise ValueError("tool_grant tools must be non-empty strings")
            tool_grant = ToolGrant(frozenset(raw_names))

        return cls(
            work_id=work.id,
            kind=work.kind,
            session_id=work.session_id,
            conversation_id=work.conversation_id,
            profile=work.profile,
            input_text=work.input_text,
            history_before_sequence=before_sequence,
            tool_grant=tool_grant,
        )


class AgentWorkHandler:
    def __init__(
        self,
        *,
        runtime: AgentRuntimePort,
        conversations: ConversationRepositoryPort,
    ) -> None:
        self._runtime = runtime
        self._conversations = conversations

    async def execute(self, work: WorkRecord) -> WorkExecutionResult:
        command = AgentWorkCommand.from_record(work)
        conversation = await self._conversations.get_conversation(command.conversation_id)
        if conversation is None:
            raise ValueError(f"conversation not found: {command.conversation_id}")

        result = await self._runtime.run(
            TurnRequest(
                work_id=command.work_id,
                session_id=command.session_id,
                conversation_id=command.conversation_id,
                input_text=command.input_text,
                profile=command.profile,
                history_before_sequence=command.history_before_sequence,
                tool_grant=command.tool_grant,
                metadata={"work_kind": command.kind},
            )
        )
        return WorkExecutionResult(
            turn_id=result.turn_id,
            outbound=OutboundDraft(
                channel=conversation.channel,
                external_chat_id=conversation.external_chat_id,
                text=result.final_text,
                origin="model" if result.succeeded else "system_error",
                metadata={
                    "runtime_status": result.status,
                    "error_code": result.error_code,
                    "profile": command.profile,
                    "tool_count": len(result.tool_records),
                },
            ),
        )
