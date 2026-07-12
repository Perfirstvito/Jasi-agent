from __future__ import annotations

from datetime import datetime
from typing import Protocol

from jasi.domain.models import OutboundPart
from jasi.domain.work import (
    WorkCompletion,
    WorkEnqueueResult,
    WorkExecutionResult,
    WorkRecord,
    WorkSpec,
)


class WorkRepositoryPort(Protocol):
    async def enqueue_work(self, spec: WorkSpec) -> WorkEnqueueResult: ...

    async def get_work(self, work_id: int) -> WorkRecord | None: ...

    async def claim_work_batch(
        self,
        limit: int,
        lease_seconds: float,
        background_limit: int | None = None,
    ) -> list[WorkRecord]: ...

    async def renew_work_lease(
        self,
        work_id: int,
        lease_token: str,
        lease_seconds: float,
    ) -> datetime: ...

    async def complete_work(
        self,
        work_id: int,
        lease_token: str,
        result: WorkExecutionResult,
        parts: tuple[OutboundPart, ...],
    ) -> WorkCompletion: ...

    async def mark_work_failed_attempt(
        self,
        work_id: int,
        lease_token: str,
        error: str,
    ) -> None: ...
