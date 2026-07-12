from __future__ import annotations

from datetime import datetime
from typing import Protocol

from jasi.domain.operations import OperationsSnapshot


class OperationsRepositoryPort(Protocol):
    async def snapshot(self, now: datetime) -> OperationsSnapshot: ...
