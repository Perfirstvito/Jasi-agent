from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import replace

from jasi.domain.models import InboundMessage
from jasi.ports.passive import PassiveIngressRepositoryPort

logger = logging.getLogger(__name__)


class PassiveIngressService:
    def __init__(
        self,
        *,
        repository: PassiveIngressRepositoryPort,
        work_wakeup: asyncio.Event,
        memory_scope_map: Mapping[str, str] | None = None,
    ) -> None:
        self._repository = repository
        self._work_wakeup = work_wakeup
        self._memory_scope_map = dict(memory_scope_map or {})

    async def handle(self, message: InboundMessage) -> None:
        identity = f"{message.channel}:{message.external_user_id}"
        scope_key = self._memory_scope_map.get(identity, identity)
        message = replace(
            message,
            metadata={**message.metadata, "memory_scope_key": scope_key},
        )
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
