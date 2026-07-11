from __future__ import annotations

from typing import Protocol

from jasi.domain.models import InboundMessage
from jasi.domain.work import WorkEnqueueResult


class PassiveIngressRepositoryPort(Protocol):
    async def enqueue_passive(self, message: InboundMessage) -> WorkEnqueueResult | None: ...
