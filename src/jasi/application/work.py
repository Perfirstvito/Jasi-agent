from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from contextlib import suppress
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
        heartbeat_seconds: float | None = None,
        background_limit: int = 2,
        idle_sleep_seconds: float = 1,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("work batch size must be positive")
        if lease_seconds <= 0:
            raise ValueError("work lease must be positive")
        if idle_sleep_seconds <= 0:
            raise ValueError("work idle sleep must be positive")
        heartbeat_seconds = heartbeat_seconds or lease_seconds / 3
        if heartbeat_seconds <= 0 or heartbeat_seconds >= lease_seconds:
            raise ValueError("work heartbeat must be shorter than its lease")
        if background_limit < 0:
            raise ValueError("background work limit cannot be negative")
        self._repository = repository
        self._dispatcher = dispatcher
        self._finalizer = finalizer
        self._batch_size = batch_size
        self._wakeup = wakeup
        self._lease_seconds = lease_seconds
        self._heartbeat_seconds = heartbeat_seconds
        self._background_limit = background_limit
        self._idle_sleep_seconds = idle_sleep_seconds

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("work worker started")
        active: set[asyncio.Task[None]] = set()
        try:
            while not stop_event.is_set():
                active = {task for task in active if not task.done()}
                self._wakeup.clear()
                capacity = self._batch_size - len(active)
                records = await self._repository.claim_work_batch(
                    capacity,
                    self._lease_seconds,
                    self._background_limit,
                )
                if records:
                    for record in records:
                        active.add(
                            asyncio.create_task(
                                self._process(record),
                                name=f"jasi-work-{record.id}",
                            )
                        )
                    continue

                wake_task = asyncio.create_task(self._wakeup.wait())
                await asyncio.wait(
                    {*active, wake_task},
                    timeout=self._idle_sleep_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not wake_task.done():
                    wake_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await wake_task
        finally:
            if active:
                await asyncio.gather(*active)
            logger.info("work worker stopped")

    async def drain_once(self) -> int:
        records = await self._repository.claim_work_batch(
            self._batch_size,
            self._lease_seconds,
            self._background_limit,
        )
        await asyncio.gather(*(self._process(record) for record in records))
        return len(records)

    async def _process(self, work: WorkRecord) -> None:
        stop_heartbeat = asyncio.Event()
        body = asyncio.create_task(self._execute_and_finalize(work))
        heartbeat = asyncio.create_task(self._heartbeat(work, stop_heartbeat))
        try:
            done, _ = await asyncio.wait(
                {body, heartbeat},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if body in done:
                await body
            else:
                body.cancel()
                with suppress(asyncio.CancelledError):
                    await body
                raise WorkLeaseLost(f"work lease heartbeat failed: {work.id}")
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
        finally:
            stop_heartbeat.set()
            if not heartbeat.done():
                await heartbeat

    async def _execute_and_finalize(self, work: WorkRecord) -> None:
        result = await self._dispatcher.execute(work)
        await self._finalizer.complete(work, result)

    async def _heartbeat(
        self,
        work: WorkRecord,
        stop_event: asyncio.Event,
    ) -> None:
        if work.lease_token is None:
            raise WorkLeaseLost(f"work has no lease token: {work.id}")
        while True:
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self._heartbeat_seconds,
                )
                return
            except TimeoutError:
                pass
            try:
                await self._repository.renew_work_lease(
                    work.id,
                    work.lease_token,
                    self._lease_seconds,
                )
            except Exception as exc:
                logger.warning(
                    "work heartbeat failed work_id=%s error=%s",
                    work.id,
                    exc.__class__.__name__,
                )
                return
