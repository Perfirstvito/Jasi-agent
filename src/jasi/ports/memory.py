from __future__ import annotations

from typing import Protocol

from jasi.domain.context import ContextItem


class MemoryContextPort(Protocol):
    async def load_context(
        self,
        *,
        conversation_id: int,
        query_text: str,
        turn_id: int,
    ) -> tuple[ContextItem, ...]: ...
