from __future__ import annotations

from typing import Protocol

from jasi.domain.drift import DriftOfferResult, DriftOpportunitySpec


class DriftRepositoryPort(Protocol):
    async def offer_drift(self, spec: DriftOpportunitySpec) -> DriftOfferResult: ...

    async def dismiss_drift(self, opportunity_id: int) -> bool: ...
