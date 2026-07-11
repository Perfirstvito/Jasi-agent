from __future__ import annotations

from typing import Protocol

from jasi.domain.models import DeliveryResult, InboundMessage, OutboundMessage, OutboundPart


class OutboundPolicy(Protocol):
    def prepare(self, text: str) -> tuple[OutboundPart, ...]: ...


class ChannelPort(Protocol):
    async def send(self, message: OutboundMessage) -> DeliveryResult: ...


class InboundHandler(Protocol):
    async def handle(self, message: InboundMessage) -> None: ...
