from __future__ import annotations

from datetime import datetime
from typing import Protocol

from jasi.domain.context import ContextItem
from jasi.domain.memory import (
    MemoryConsolidationBatch,
    MemoryDocumentName,
    MemoryDocumentSnapshot,
    MemoryDocumentState,
    MemoryJobRecord,
    MemoryRecordDraft,
    MemoryRetrievalAudit,
    MemoryScopeRecord,
    MemorySearchHit,
    MemoryWorkspaceSnapshot,
)


class MemoryDocumentStorePort(Protocol):
    def ensure_workspace(self, scope_directory: str) -> MemoryWorkspaceSnapshot: ...

    def read_workspace(self, scope_directory: str) -> MemoryWorkspaceSnapshot: ...

    def write_document(
        self,
        scope_directory: str,
        name: MemoryDocumentName,
        content: str,
        *,
        expected_hash: str | None = None,
    ) -> MemoryDocumentSnapshot: ...


class MemoryContextPort(Protocol):
    async def load_context(
        self,
        *,
        conversation_id: int,
        query_text: str,
        turn_id: int,
    ) -> tuple[ContextItem, ...]: ...


class MemoryIndexRepositoryPort(Protocol):
    async def resolve_scope(self, conversation_id: int) -> MemoryScopeRecord: ...

    async def list_scopes(self) -> list[MemoryScopeRecord]: ...

    async def load_document_states(self, scope_id: int) -> list[MemoryDocumentState]: ...

    async def replace_document_index(
        self,
        scope: MemoryScopeRecord,
        document: MemoryDocumentSnapshot,
        records: tuple[MemoryRecordDraft, ...],
    ) -> MemoryDocumentState: ...

    async def search_records(
        self,
        *,
        scope_id: int,
        query_text: str,
        query_embedding: tuple[float, ...] | None,
        limit: int,
    ) -> list[MemorySearchHit]: ...

    async def record_retrieval(self, audit: MemoryRetrievalAudit) -> None: ...


class MemoryJobRepositoryPort(Protocol):
    async def enqueue_reindex(
        self,
        scope_id: int,
        dedupe_key: str,
        payload: dict,
    ) -> bool: ...

    async def claim_jobs(
        self,
        *,
        kind: str,
        limit: int,
        lease_seconds: float,
        consolidation_batch_messages: int,
        now: datetime,
    ) -> list[MemoryJobRecord]: ...

    async def load_consolidation_batch(
        self,
        jobs: tuple[MemoryJobRecord, ...],
        *,
        history_keep_count: int,
    ) -> MemoryConsolidationBatch: ...

    async def commit_consolidation(self, batch: MemoryConsolidationBatch) -> None: ...

    async def complete_jobs(self, jobs: tuple[MemoryJobRecord, ...]) -> None: ...

    async def fail_jobs(
        self,
        jobs: tuple[MemoryJobRecord, ...],
        error: str,
        now: datetime,
    ) -> None: ...
