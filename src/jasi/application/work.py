from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from typing import Protocol

from jasi.domain.work import WorkAction, WorkExecutionResult, WorkLeaseLost, WorkRecord
from jasi.ports.channel import OutboundPolicy
from jasi.ports.work import WorkRepositoryPort

logger = logging.getLogger(__name__)


class WorkHandler(Protocol):
    async def execute(self, work: WorkRecord) -> WorkExecutionResult: ...


class WorkDispatcher:
    def __init__(self, handlers: Mapping[WorkAction, WorkHandler]) -> None:
        self._handlers = dict(handlers)

    async def execute(self, work: WorkRecord) -> WorkExecutionResult:
        try:
            handler = self._handlers[work.action]
        except KeyError as exc:
            raise ValueError(f"unsupported work action: {work.action}") from exc
        return await handler.execute(work)


class WorkFinalizer:
    def __init__(
        self,
        *,
        repository: WorkRepositoryPort,
        outbound_policies: Mapping[str, OutboundPolicy],
        outbox_wakeup: asyncio.Event,
    ) -> None:
        self._repository = repository
        self._outbound_policies = dict(outbound_policies)
        self._outbox_wakeup = outbox_wakeup

    async def complete(self, work: WorkRecord, result: WorkExecutionResult) -> None:
        if work.lease_token is None:
            raise WorkLeaseLost(f"work has no lease token: {work.id}")

        parts = ()
        if result.outbound is not None:
            try:
                policy = self._outbound_policies[result.outbound.channel]
            except KeyError as exc:
                raise ValueError(
                    f"unsupported outbound channel: {result.outbound.channel}"
                ) from exc
            parts = policy.prepare(result.outbound.text)

        completion = await self._repository.complete_work(
            work.id,
            work.lease_token,
            result,
            parts,
        )
        if completion.outbox:
            self._outbox_wakeup.set()


class WorkWorker:
    def __init__(
        self,
        *,
        repository: WorkRepositoryPort,
        dispatcher: WorkDispatcher,
        finalizer: WorkFinalizer,
        batch_size: int,
        wakeup: asyncio.Event,
        lease_seconds: float = 600,
        idle_sleep_seconds: float = 1,
    ) -> None:
        self._repository = repository
        self._dispatcher = dispatcher
        self._finalizer = finalizer
        self._batch_size = batch_size
        self._wakeup = wakeup
        self._lease_seconds = lease_seconds
        self._idle_sleep_seconds = idle_sleep_seconds

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("work worker started")
        try:
            while not stop_event.is_set():
                self._wakeup.clear()
                processed = await self.drain_once()
                if processed == 0:
                    try:
                        await asyncio.wait_for(
                            self._wakeup.wait(),
                            timeout=self._idle_sleep_seconds,
                        )
                    except TimeoutError:
                        pass
        finally:
            logger.info("work worker stopped")

    async def drain_once(self) -> int:
        records = await self._repository.claim_work_batch(
            self._batch_size,
            self._lease_seconds,
        )
        await asyncio.gather(*(self._process(record) for record in records))
        return len(records)

    async def _process(self, work: WorkRecord) -> None:
        try:
            result = await self._dispatcher.execute(work)
            await self._finalizer.complete(work, result)
        except WorkLeaseLost:
            logger.warning("work lease lost work_id=%s", work.id)
        except Exception as exc:
            logger.exception("work execution failed work_id=%s kind=%s", work.id, work.kind)
            if work.lease_token is None:
                return
            try:
                await self._repository.mark_work_failed_attempt(
                    work.id,
                    work.lease_token,
                    exc.__class__.__name__,
                )
            except WorkLeaseLost:
                logger.warning("work lease lost while recording failure work_id=%s", work.id)
