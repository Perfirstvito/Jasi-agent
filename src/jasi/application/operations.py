from __future__ import annotations

from datetime import UTC, datetime

from jasi.domain.operations import OperationsSnapshot
from jasi.ports.operations import OperationsRepositoryPort


class OperationsService:
    def __init__(self, repository: OperationsRepositoryPort) -> None:
        self._repository = repository

    async def snapshot(self) -> OperationsSnapshot:
        return await self._repository.snapshot(datetime.now(UTC))
