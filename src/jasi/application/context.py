from __future__ import annotations

import logging

from jasi.domain.context import HistoryItem, TurnContextQuery, TurnContextSnapshot
from jasi.ports.memory import MemoryContextPort
from jasi.ports.repository import RuntimeRepositoryPort

logger = logging.getLogger(__name__)


class TurnContextProvider:
    def __init__(
        self,
        *,
        repository: RuntimeRepositoryPort,
        memory: MemoryContextPort | None = None,
    ) -> None:
        self._repository = repository
        self._memory = memory

    async def prepare(self, query: TurnContextQuery) -> TurnContextSnapshot:
        rows = await self._repository.load_history(
            conversation_id=query.conversation_id,
            before_sequence=query.before_sequence,
            limit=query.history_limit,
        )
        history = tuple(
            HistoryItem(
                message_id=row.id,
                sequence=row.sequence,
                role=row.role,
                content=row.content,
            )
            for row in rows
            if row.role in {"user", "assistant"}
        )
        if not query.include_memory or self._memory is None:
            return TurnContextSnapshot(history=history)

        try:
            context_items = await self._memory.load_context(
                conversation_id=query.conversation_id,
                query_text=query.input_text,
                turn_id=query.turn_id,
            )
        except Exception:
            logger.exception(
                "memory context unavailable; continuing without memory turn_id=%s",
                query.turn_id,
            )
            context_items = ()
        return TurnContextSnapshot(history=history, context_items=context_items)
