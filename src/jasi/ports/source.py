from __future__ import annotations

from datetime import datetime
from typing import Protocol

from jasi.domain.source import (
    InitiativeKind,
    SourceBatch,
    SourceCreateResult,
    SourceSubscriptionRecord,
    SourceSubscriptionSpec,
)
from jasi.domain.work import WorkRecord


class SourcePort(Protocol):
    async def poll(self, subscription: SourceSubscriptionRecord) -> SourceBatch: ...


class SourceRepositoryPort(Protocol):
    async def create_subscription(self, spec: SourceSubscriptionSpec) -> SourceCreateResult: ...

    async def disable_subscription(self, subscription_id: int) -> bool: ...

    async def claim_due_subscriptions(
        self,
        now: datetime,
        limit: int,
        lease_seconds: float,
    ) -> list[SourceSubscriptionRecord]: ...

    async def complete_source_poll(
        self,
        subscription_id: int,
        lease_token: str,
        batch: SourceBatch,
        now: datetime,
    ) -> int: ...

    async def mark_source_poll_failed(
        self,
        subscription_id: int,
        lease_token: str,
        error: str,
        now: datetime,
    ) -> None: ...


class InitiativeRepositoryPort(Protocol):
    async def materialize_initiatives(
        self,
        kind: InitiativeKind,
        now: datetime,
        limit: int,
    ) -> list[WorkRecord]: ...
