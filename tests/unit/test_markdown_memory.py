from __future__ import annotations

from pathlib import Path

import pytest

from jasi.adapters.persistence.markdown.memory_store import (
    MarkdownMemoryStore,
    MemoryDocumentConflict,
)
from jasi.application.memory.indexing import (
    MarkdownMemoryIndexer,
    MarkdownRecordParser,
    render_memory_entry,
)
from jasi.domain.memory import MEMORY_DOCUMENT_NAMES, MemoryDocumentState, MemoryScopeRecord


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


def test_markdown_parser_preserves_marker_tags_and_evidence(tmp_path: Path) -> None:
    store = MarkdownMemoryStore(tmp_path)
    workspace = store.ensure_workspace("scope-1")
    original = workspace.document("MEMORY.md")
    entry = render_memory_entry(
        record_key="memory-1",
        tier="stable",
        content="The user prefers PostgreSQL.",
        tags=("database", "preference"),
        evidence_message_ids=(10, 11),
    )
    document = store.write_document(
        "scope-1",
        "MEMORY.md",
        f"# Long-term Memory\n\n## Preferences\n\n{entry}",
        expected_hash=original.content_hash,
    )

    records = MarkdownRecordParser().parse(document)

    assert len(records) == 1
    assert records[0].record_key == "memory-1"
    assert records[0].tier == "stable"
    assert records[0].heading == "Preferences"
    assert records[0].tags == ("database", "preference")
    assert records[0].evidence_message_ids == (10, 11)


@pytest.mark.asyncio
async def test_indexer_rebuilds_only_changed_markdown_documents(tmp_path: Path) -> None:
    scope = MemoryScopeRecord(id=1, scope_key="owner", directory_name="scope-1")
    store = MarkdownMemoryStore(tmp_path)
    workspace = store.ensure_workspace(scope.directory_name)
    memory = workspace.document("MEMORY.md")
    store.write_document(
        scope.directory_name,
        "MEMORY.md",
        "# Long-term Memory\n\n- The user likes databases.",
        expected_hash=memory.content_hash,
    )

    class Embedding:
        model_name = "fake-embedding"

        async def embed(self, texts):
            return tuple((float(len(text)), 1.0) for text in texts)

    class Repository:
        def __init__(self) -> None:
            self.states: dict[str, MemoryDocumentState] = {}
            self.writes: list[tuple[str, tuple]] = []

        async def load_document_states(self, _scope_id):
            return list(self.states.values())

        async def replace_document_index(self, scope, document, records):
            self.writes.append((document.name, records))
            state = MemoryDocumentState(
                id=len(self.states) + 1,
                scope_id=scope.id,
                name=document.name,
                content_hash=document.content_hash,
                indexed_hash=document.content_hash,
                version=self.states.get(
                    document.name,
                    MemoryDocumentState(0, scope.id, document.name, "", "", 0),
                ).version
                + 1,
            )
            self.states[document.name] = state
            return state

    repository = Repository()
    indexer = MarkdownMemoryIndexer(
        store=store,
        repository=repository,
        embedding=Embedding(),
    )

    assert await indexer.sync_scope(scope) == 4
    assert await indexer.sync_scope(scope) == 0
    memory_write = next(records for name, records in repository.writes if name == "MEMORY.md")
    assert memory_write[0].embedding == (float(len(memory_write[0].content)), 1.0)
    assert memory_write[0].embedding_model == "fake-embedding"
