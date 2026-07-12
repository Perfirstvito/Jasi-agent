from __future__ import annotations

from typing import Protocol

from jasi.domain.models import MessageRecord


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
