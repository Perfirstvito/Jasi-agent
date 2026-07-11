from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping

from jasi.domain.effect import EffectRecord
from jasi.ports.effect import EffectPort, EffectRepositoryPort

logger = logging.getLogger(__name__)


class EffectDispatcher:
    def __init__(
        self,
        *,
        repository: EffectRepositoryPort,
        adapters: Mapping[str, EffectPort],
    ) -> None:
        self._repository = repository
        self._adapters = dict(adapters)

    async def execute(self, effect: EffectRecord) -> None:
        adapter = self._adapters.get(effect.adapter)
        if adapter is None:
            await self._repository.mark_effect_failed_attempt(
                effect.id,
                f"unsupported effect adapter: {effect.adapter}",
                retryable=False,
            )
            return
        try:
            result = await adapter.execute(effect)
        except Exception as exc:
            logger.exception("effect execution failed effect_id=%s", effect.id)
            await self._repository.mark_effect_failed_attempt(
                effect.id,
                exc.__class__.__name__,
                retryable=True,
            )
            return
        if result.success:
            await self._repository.mark_effect_succeeded(effect.id, result.result)
            return
        await self._repository.mark_effect_failed_attempt(
            effect.id,
            result.error or "effect execution failed",
            retryable=result.retryable,
        )


class EffectWorker:
    def __init__(
        self,
        *,
        repository: EffectRepositoryPort,
        dispatcher: EffectDispatcher,
        batch_size: int,
        wakeup: asyncio.Event,
        idle_sleep_seconds: float = 1,
    ) -> None:
        if batch_size <= 0 or idle_sleep_seconds <= 0:
            raise ValueError("effect worker settings must be positive")
        self._repository = repository
        self._dispatcher = dispatcher
        self._batch_size = batch_size
        self._wakeup = wakeup
        self._idle_sleep_seconds = idle_sleep_seconds

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("effect worker started")
        try:
            while not stop_event.is_set():
                self._wakeup.clear()
                processed = await self.drain_once()
                if processed >= self._batch_size:
                    continue
                try:
                    await asyncio.wait_for(
                        self._wakeup.wait(),
                        timeout=self._idle_sleep_seconds,
                    )
                except TimeoutError:
                    pass
        finally:
            logger.info("effect worker stopped")

    async def drain_once(self) -> int:
        effects = await self._repository.claim_effect_batch(self._batch_size)
        await asyncio.gather(*(self._dispatcher.execute(effect) for effect in effects))
        return len(effects)
