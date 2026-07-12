from __future__ import annotations

from typing import Protocol

from jasi.domain.context import TurnContextQuery, TurnContextSnapshot


class TurnContextProviderPort(Protocol):
    async def prepare(self, query: TurnContextQuery) -> TurnContextSnapshot: ...
