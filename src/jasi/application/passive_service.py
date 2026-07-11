from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping

from jasi.domain.models import InboundMessage
from jasi.ports.channel import OutboundPolicy
from jasi.ports.repository import ChatRepositoryPort
from jasi.runtime.models import TurnRequest
from jasi.runtime.runtime import AgentRuntime

logger = logging.getLogger(__name__)


class SessionLockRegistry:
    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}
        self._guard = asyncio.Lock()

    async def get(self, session_id: str) -> asyncio.Lock:
        async with self._guard:
            lock = self._locks.get(session_id)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[session_id] = lock
            return lock


class PassiveChatService:
    def __init__(
        self,
        *,
        repository: ChatRepositoryPort,
        runtime: AgentRuntime,
        outbound_policies: Mapping[str, OutboundPolicy],
        outbox_wakeup: asyncio.Event,
        locks: SessionLockRegistry | None = None,
    ) -> None:
        self._repository = repository
        self._runtime = runtime
        self._outbound_policies = dict(outbound_policies)
        self._outbox_wakeup = outbox_wakeup
        self._locks = locks or SessionLockRegistry()

    async def handle(self, message: InboundMessage) -> None:
        try:
            outbound_policy = self._outbound_policies[message.channel]
        except KeyError as exc:
            raise ValueError(f"unsupported outbound channel: {message.channel}") from exc

        session_id = f"{message.channel}:{message.external_chat_id}"
        lock = await self._locks.get(session_id)
        async with lock:
            claim = await self._repository.claim_inbound_message(message)
            if claim is None:
                logger.info(
                    "skipping completed inbound event channel=%s update_id=%s",
                    message.channel,
                    message.external_update_id,
                )
                return
            try:
                result = await self._runtime.run(
                    TurnRequest(
                        session_id=session_id,
                        inbound_message_id=claim.message.id,
                        inbound_text=message.text,
                        profile="passive",
                        metadata={
                            "conversation_id": claim.conversation.id,
                            "message_sequence": claim.message.sequence,
                        },
                    )
                )

                origin = "model" if result.succeeded else "system_error"
                parts = outbound_policy.prepare(result.final_text)
                await self._repository.complete_inbound_response(
                    inbound_event_id=claim.event_id,
                    conversation_id=claim.conversation.id,
                    channel=message.channel,
                    external_chat_id=message.external_chat_id,
                    turn_id=result.turn_id,
                    text=result.final_text,
                    parts=parts,
                    origin=origin,
                    metadata={
                        "runtime_status": result.status,
                        "error_code": result.error_code,
                        "tool_count": len(result.tool_records),
                    },
                )
            except Exception as exc:
                try:
                    await self._repository.release_inbound(
                        claim.event_id,
                        exc.__class__.__name__,
                    )
                except Exception:
                    logger.exception(
                        "failed to release inbound event event_id=%s",
                        claim.event_id,
                    )
                raise
            self._outbox_wakeup.set()
