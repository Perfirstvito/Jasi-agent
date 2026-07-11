from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from datetime import UTC, datetime

from jasi.domain.source import (
    InitiativeKind,
    SourceBatch,
    SourceCreateResult,
    SourceLeaseLost,
    SourceSubscriptionRecord,
    SourceSubscriptionSpec,
)
from jasi.ports.source import (
    InitiativeRepositoryPort,
    SourcePort,
    SourceRepositoryPort,
)

logger = logging.getLogger(__name__)


class SourceService:
    def __init__(
        self,
        *,
        repository: SourceRepositoryPort,
        source_wakeup: asyncio.Event,
    ) -> None:
        self._repository = repository
        self._source_wakeup = source_wakeup

    async def subscribe(self, spec: SourceSubscriptionSpec) -> SourceCreateResult:
        result = await self._repository.create_subscription(spec)
        self._source_wakeup.set()
        return result

    async def disable(self, subscription_id: int) -> bool:
        return await self._repository.disable_subscription(subscription_id)


class SourceDispatcher:
    def __init__(self, sources: Mapping[str, SourcePort]) -> None:
        self._sources = dict(sources)

    async def poll(self, subscription: SourceSubscriptionRecord) -> SourceBatch:
        try:
            source = self._sources[subscription.source]
        except KeyError as exc:
            raise ValueError(f"unsupported source: {subscription.source}") from exc
        return await source.poll(subscription)


class SourceWorker:
    def __init__(
        self,
        *,
        repository: SourceRepositoryPort,
        dispatcher: SourceDispatcher,
        batch_size: int,
        source_wakeup: asyncio.Event,
        initiative_wakeup: asyncio.Event,
        lease_seconds: float = 120,
        poll_timeout_seconds: float = 60,
        idle_sleep_seconds: float = 1,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("source batch size must be positive")
        if lease_seconds <= 0 or poll_timeout_seconds <= 0 or idle_sleep_seconds <= 0:
            raise ValueError("source worker timeouts must be positive")
        if poll_timeout_seconds >= lease_seconds:
            raise ValueError("source poll timeout must be shorter than its lease")
        self._repository = repository
        self._dispatcher = dispatcher
        self._batch_size = batch_size
        self._source_wakeup = source_wakeup
        self._initiative_wakeup = initiative_wakeup
        self._lease_seconds = lease_seconds
        self._poll_timeout_seconds = poll_timeout_seconds
        self._idle_sleep_seconds = idle_sleep_seconds

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("source worker started")
        try:
            while not stop_event.is_set():
                self._source_wakeup.clear()
                processed = await self.drain_once()
                if processed >= self._batch_size:
                    continue
                try:
                    await asyncio.wait_for(
                        self._source_wakeup.wait(),
                        timeout=self._idle_sleep_seconds,
                    )
                except TimeoutError:
                    pass
        finally:
            logger.info("source worker stopped")

    async def drain_once(self, now: datetime | None = None) -> int:
        claimed = await self._repository.claim_due_subscriptions(
            now or datetime.now(UTC),
            self._batch_size,
            self._lease_seconds,
        )
        await asyncio.gather(*(self._poll(subscription) for subscription in claimed))
        return len(claimed)

    async def _poll(self, subscription: SourceSubscriptionRecord) -> None:
        if subscription.lease_token is None:
            return
        try:
            async with asyncio.timeout(self._poll_timeout_seconds):
                batch = await self._dispatcher.poll(subscription)
            inserted = await self._repository.complete_source_poll(
                subscription.id,
                subscription.lease_token,
                batch,
                datetime.now(UTC),
            )
            if inserted:
                self._initiative_wakeup.set()
        except SourceLeaseLost:
            logger.warning("source lease lost subscription_id=%s", subscription.id)
        except Exception as exc:
            logger.exception("source poll failed subscription_id=%s", subscription.id)
            try:
                await self._repository.mark_source_poll_failed(
                    subscription.id,
                    subscription.lease_token,
                    exc.__class__.__name__,
                    datetime.now(UTC),
                )
            except SourceLeaseLost:
                logger.warning(
                    "source lease lost while recording failure subscription_id=%s",
                    subscription.id,
                )


class InitiativePlanner:
    def __init__(
        self,
        *,
        kind: InitiativeKind,
        repository: InitiativeRepositoryPort,
        batch_size: int,
        initiative_wakeup: asyncio.Event,
        work_wakeup: asyncio.Event,
        idle_sleep_seconds: float = 1,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("initiative batch size must be positive")
        if idle_sleep_seconds <= 0:
            raise ValueError("initiative idle sleep must be positive")
        self._kind = kind
        self._repository = repository
        self._batch_size = batch_size
        self._initiative_wakeup = initiative_wakeup
        self._work_wakeup = work_wakeup
        self._idle_sleep_seconds = idle_sleep_seconds

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("initiative planner started kind=%s", self._kind)
        try:
            while not stop_event.is_set():
                self._initiative_wakeup.clear()
                processed = await self.drain_once()
                if processed >= self._batch_size:
                    continue
                try:
                    await asyncio.wait_for(
                        self._initiative_wakeup.wait(),
                        timeout=self._idle_sleep_seconds,
                    )
                except TimeoutError:
                    pass
        finally:
            logger.info("initiative planner stopped kind=%s", self._kind)

    async def drain_once(self, now: datetime | None = None) -> int:
        records = await self._repository.materialize_initiatives(
            self._kind,
            now or datetime.now(UTC),
            self._batch_size,
        )
        if records:
            self._work_wakeup.set()
        return len(records)
