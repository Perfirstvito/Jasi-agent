from __future__ import annotations

import asyncio
import logging

from jasi.domain.models import InboundMessage
from jasi.ports.passive import PassiveIngressRepositoryPort

logger = logging.getLogger(__name__)


class PassiveIngressService:
    def __init__(
        self,
        *,
        repository: PassiveIngressRepositoryPort,
        work_wakeup: asyncio.Event,
    ) -> None:
        self._repository = repository
        self._work_wakeup = work_wakeup

    async def handle(self, message: InboundMessage) -> None:
        result = await self._repository.enqueue_passive(message)
        if result is None:
            logger.info(
                "skipping completed inbound event channel=%s update_id=%s",
                message.channel,
                message.external_update_id,
            )
            return
        if not result.created:
            logger.info(
                "passive work already exists channel=%s update_id=%s work_id=%s",
                message.channel,
                message.external_update_id,
                result.work.id,
            )
        self._work_wakeup.set()
