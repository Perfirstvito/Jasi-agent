from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

from jasi.domain.schedule import ScheduleCreateResult, ScheduleSpec
from jasi.ports.schedule import ScheduleRepositoryPort

logger = logging.getLogger(__name__)


class ScheduleService:
    def __init__(
        self,
        *,
        repository: ScheduleRepositoryPort,
        schedule_wakeup: asyncio.Event,
    ) -> None:
        self._repository = repository
        self._schedule_wakeup = schedule_wakeup

    async def create(self, spec: ScheduleSpec) -> ScheduleCreateResult:
        result = await self._repository.create_schedule(spec)
        self._schedule_wakeup.set()
        return result

    async def cancel(self, job_id: int) -> bool:
        return await self._repository.cancel_schedule(job_id)


class ScheduleWorker:
    def __init__(
        self,
        *,
        repository: ScheduleRepositoryPort,
        batch_size: int,
        schedule_wakeup: asyncio.Event,
        work_wakeup: asyncio.Event,
        poll_seconds: float = 1,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("schedule batch size must be positive")
        if poll_seconds <= 0:
            raise ValueError("schedule poll interval must be positive")
        self._repository = repository
        self._batch_size = batch_size
        self._schedule_wakeup = schedule_wakeup
        self._work_wakeup = work_wakeup
        self._poll_seconds = poll_seconds

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("schedule worker started")
        try:
            while not stop_event.is_set():
                self._schedule_wakeup.clear()
                processed = await self.drain_once()
                if processed >= self._batch_size:
                    continue
                try:
                    await asyncio.wait_for(
                        self._schedule_wakeup.wait(),
                        timeout=self._poll_seconds,
                    )
                except TimeoutError:
                    pass
        finally:
            logger.info("schedule worker stopped")

    async def drain_once(self, now: datetime | None = None) -> int:
        records = await self._repository.materialize_due(
            now or datetime.now(UTC),
            self._batch_size,
        )
        if records:
            self._work_wakeup.set()
        return len(records)
