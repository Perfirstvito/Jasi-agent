from __future__ import annotations

from typing import Protocol

from jasi.runtime.models import TurnRequest, TurnResult


class AgentRuntimePort(Protocol):
    async def run(self, request: TurnRequest) -> TurnResult: ...
