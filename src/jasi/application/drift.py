from __future__ import annotations

import asyncio

from jasi.domain.drift import DriftOfferResult, DriftOpportunitySpec
from jasi.ports.drift import DriftRepositoryPort


class DriftOpportunityProducer:
    def __init__(
        self,
        *,
        repository: DriftRepositoryPort,
        initiative_wakeup: asyncio.Event,
    ) -> None:
        self._repository = repository
        self._initiative_wakeup = initiative_wakeup

    async def offer(self, spec: DriftOpportunitySpec) -> DriftOfferResult:
        result = await self._repository.offer_drift(spec)
        self._initiative_wakeup.set()
        return result

    async def dismiss(self, opportunity_id: int) -> bool:
        return await self._repository.dismiss_drift(opportunity_id)
