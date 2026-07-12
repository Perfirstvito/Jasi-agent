from __future__ import annotations

from typing import Protocol

from jasi.domain.effect import EffectRecord, EffectResult


class EffectPort(Protocol):
    async def execute(self, effect: EffectRecord) -> EffectResult: ...


class EffectRepositoryPort(Protocol):
    async def get_effect(self, effect_id: int) -> EffectRecord | None: ...

    async def claim_effect_batch(self, limit: int) -> list[EffectRecord]: ...

    async def mark_effect_succeeded(
        self,
        effect_id: int,
        result: dict,
    ) -> None: ...

    async def mark_effect_failed_attempt(
        self,
        effect_id: int,
        error: str,
        retryable: bool,
    ) -> None: ...
