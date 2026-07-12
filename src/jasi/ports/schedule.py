from __future__ import annotations

from datetime import datetime
from typing import Protocol

from jasi.domain.schedule import ScheduleCreateResult, ScheduleJobRecord, ScheduleSpec
from jasi.domain.work import WorkRecord


class ScheduleRepositoryPort(Protocol):
    async def create_schedule(self, spec: ScheduleSpec) -> ScheduleCreateResult: ...

    async def get_schedule(self, job_id: int) -> ScheduleJobRecord | None: ...

    async def cancel_schedule(self, job_id: int) -> bool: ...

    async def materialize_due(self, now: datetime, limit: int) -> list[WorkRecord]: ...
