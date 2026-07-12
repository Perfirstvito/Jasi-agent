from __future__ import annotations

from typing import Protocol

from jasi.domain.memory import (
    MemoryCandidate,
    MemoryMessage,
    MemoryRecordDraft,
    MemorySearchHit,
    StableMemoryDecision,
)


class MemoryMaintenanceReasonerPort(Protocol):
    async def extract(self, messages: tuple[MemoryMessage, ...]) -> tuple[MemoryCandidate, ...]: ...

    async def reconcile_stable(
        self,
        existing: tuple[MemoryRecordDraft, ...],
        candidates: tuple[MemoryCandidate, ...],
    ) -> tuple[StableMemoryDecision, ...]: ...

    async def summarize(
        self,
        existing_summary: str,
        messages: tuple[MemoryMessage, ...],
    ) -> str: ...


class MemoryRetrievalReasonerPort(Protocol):
    async def gate(self, query: str) -> tuple[bool, str]: ...

    async def rewrite(self, query: str) -> str: ...

    async def hyde(self, query: str) -> str: ...

    async def rerank(
        self,
        query: str,
        hits: tuple[MemorySearchHit, ...],
    ) -> tuple[tuple[int, float], ...]: ...

    async def sufficient(self, query: str, hits: tuple[MemorySearchHit, ...]) -> bool: ...
