from __future__ import annotations

from pathlib import Path

import pytest

from jasi.adapters.persistence.markdown.memory_store import (
    MarkdownMemoryStore,
    MemoryDocumentConflict,
)
from jasi.domain.memory import MEMORY_DOCUMENT_NAMES


def test_markdown_store_creates_all_authoritative_documents(tmp_path: Path) -> None:
    store = MarkdownMemoryStore(tmp_path)

    snapshot = store.ensure_workspace("scope-1")

    assert tuple(document.name for document in snapshot.documents) == MEMORY_DOCUMENT_NAMES
    assert all((tmp_path / "scope-1" / name).exists() for name in MEMORY_DOCUMENT_NAMES)


def test_markdown_store_detects_manual_edit_conflict(tmp_path: Path) -> None:
    store = MarkdownMemoryStore(tmp_path)
    snapshot = store.ensure_workspace("scope-1")
    memory = snapshot.document("MEMORY.md")
    (tmp_path / "scope-1" / "MEMORY.md").write_text("# Manual edit\n", encoding="utf-8")

    with pytest.raises(MemoryDocumentConflict):
        store.write_document(
            "scope-1",
            "MEMORY.md",
            "# Generated update",
            expected_hash=memory.content_hash,
        )

    assert (tmp_path / "scope-1" / "MEMORY.md").read_text(encoding="utf-8") == ("# Manual edit\n")


def test_markdown_store_rejects_scope_path_traversal(tmp_path: Path) -> None:
    store = MarkdownMemoryStore(tmp_path)

    with pytest.raises(ValueError):
        store.ensure_workspace("../outside")
