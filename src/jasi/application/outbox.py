from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping

from jasi.domain.models import OutboundMessage, OutboxRecord
from jasi.ports.channel import ChannelPort
from jasi.ports.repository import OutboxRepositoryPort

logger = logging.getLogger(__name__)


class OutboxDispatcher:
    def __init__(
        self,
        *,
        repository: OutboxRepositoryPort,
        channels: Mapping[str, ChannelPort],
    ) -> None:
        self._repository = repository
        self._channels = dict(channels)

    async def deliver(self, record: OutboxRecord) -> None:
        channel = self._channels.get(record.channel)
        if channel is None:
            await self._repository.mark_outbox_failed_attempt(
                record.id,
                f"unsupported outbound channel: {record.channel}",
                retryable=False,
            )
            return

        outbound = OutboundMessage(
            channel=record.channel,
            external_chat_id=record.external_chat_id,
            text=record.text,
            outbox_id=record.id,
            metadata={
                "message_id": record.message_id,
                "segment_index": record.segment_index,
                "segment_count": record.segment_count,
            },
        )
        try:
            result = await channel.send(outbound)
        except Exception as exc:
            logger.exception("channel send failed outbox_id=%s", record.id)
            await self._repository.mark_outbox_failed_attempt(record.id, str(exc), retryable=True)
            return

        if result.success:
            await self._repository.mark_outbox_sent(record.id, result.external_message_id)
            return

        await self._repository.mark_outbox_failed_attempt(
            record.id,
            result.error or "channel send failed",
            retryable=result.retryable,
        )


class OutboxWorker:
    def __init__(
        self,
        *,
        repository: OutboxRepositoryPort,
        dispatcher: OutboxDispatcher,
        batch_size: int,
        wakeup: asyncio.Event,
        idle_sleep_seconds: float = 1.0,
    ) -> None:
        self._repository = repository
        self._dispatcher = dispatcher
        self._batch_size = batch_size
        self._wakeup = wakeup
        self._idle_sleep_seconds = idle_sleep_seconds

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("outbox worker started")
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
            logger.info("outbox worker stopped")

    async def drain_once(self) -> int:
        records = await self._repository.claim_outbox_batch(self._batch_size)
        for record in records:
            await self._dispatcher.deliver(record)
        return len(records)
