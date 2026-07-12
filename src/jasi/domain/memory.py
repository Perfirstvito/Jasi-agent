from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

MemoryTier = Literal["stable", "episodic"]
MemoryDocumentName = Literal[
    "MEMORY.md",
    "HISTORY.md",
    "RECENT_CONTEXT.md",
    "PENDING.md",
]

MEMORY_DOCUMENT_NAMES: tuple[MemoryDocumentName, ...] = (
    "MEMORY.md",
    "HISTORY.md",
    "RECENT_CONTEXT.md",
    "PENDING.md",
)

MEMORY_DOCUMENT_TEMPLATES: dict[MemoryDocumentName, str] = {
    "MEMORY.md": "# Long-term Memory\n",
    "HISTORY.md": "# Memory History\n",
    "RECENT_CONTEXT.md": "# Recent Context\n",
    "PENDING.md": "# Pending Memory\n",
}


def content_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class MemoryDocumentSnapshot:
    name: MemoryDocumentName
    content: str
    content_hash: str


@dataclass(frozen=True)
class MemoryWorkspaceSnapshot:
    scope_directory: str
    documents: tuple[MemoryDocumentSnapshot, ...]

    def document(self, name: MemoryDocumentName) -> MemoryDocumentSnapshot:
        for document in self.documents:
            if document.name == name:
                return document
        raise KeyError(name)


@dataclass(frozen=True)
class MemoryScopeRecord:
    id: int
    scope_key: str
    directory_name: str


@dataclass(frozen=True)
class MemoryDocumentState:
    id: int
    scope_id: int
    name: MemoryDocumentName
    content_hash: str
    indexed_hash: str
    version: int


@dataclass(frozen=True)
class MemoryRecordDraft:
    record_key: str
    tier: MemoryTier
    ordinal: int
    heading: str
    content: str
    content_hash: str
    tags: tuple[str, ...] = ()
    evidence_message_ids: tuple[int, ...] = ()
    embedding: tuple[float, ...] | None = None
    embedding_model: str | None = None
    happened_at: datetime | None = None


@dataclass(frozen=True)
class MemorySearchHit:
    record_id: int
    record_key: str
    tier: MemoryTier
    content: str
    tags: tuple[str, ...]
    semantic_score: float = 0.0
    lexical_score: float = 0.0
    rerank_score: float = 0.0
    final_score: float = 0.0
    injected: bool = False


@dataclass(frozen=True)
class MemoryRetrievalAudit:
    turn_id: int
    scope_id: int
    query: str
    rewritten_query: str | None
    hyde_text: str | None
    gate_decision: Literal["retrieve", "skip", "fallback"]
    sufficient: bool
    trace: dict[str, Any]
    hits: tuple[MemorySearchHit, ...]


@dataclass(frozen=True)
class MemoryJobRecord:
    id: int
    scope_id: int
    scope_directory: str
    conversation_id: int | None
    kind: Literal["consolidate", "reindex"]
    trigger_message_id: int | None
    status: str
    attempts: int
    lease_token: str | None
    payload: dict[str, Any]


@dataclass(frozen=True)
class MemoryCheckpointRecord:
    scope_id: int
    conversation_id: int
    consolidated_through_sequence: int
    summarized_through_sequence: int


class MemoryJobLeaseLost(RuntimeError):
    pass
