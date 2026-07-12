from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Literal

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
