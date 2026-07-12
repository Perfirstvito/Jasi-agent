from __future__ import annotations

from typing import Protocol

from jasi.domain.models import MessageRecord
from jasi.domain.processes import ProcessScope, ProcessSnapshot


class MessageLookupPort(Protocol):
    async def fetch_messages(
        self,
        conversation_id: int,
        message_ids: tuple[int, ...],
    ) -> list[MessageRecord]: ...

    async def search_messages(
        self,
        conversation_id: int,
        query: str,
        limit: int,
        offset: int,
    ) -> tuple[list[MessageRecord], int]: ...


class ProcessLookupPort(Protocol):
    @property
    def available_scopes(self) -> frozenset[ProcessScope]: ...

    async def inspect(self, scope: ProcessScope) -> ProcessSnapshot: ...
