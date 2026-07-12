from __future__ import annotations

import asyncio
import json
import re
from dataclasses import replace
from datetime import datetime
from typing import Any

from markdown_it import MarkdownIt

from jasi.domain.memory import (
    MemoryDocumentSnapshot,
    MemoryRecordDraft,
    MemoryScopeRecord,
    content_hash,
)
from jasi.ports.embedding import EmbeddingPort
from jasi.ports.memory import MemoryDocumentStorePort, MemoryIndexRepositoryPort

_MARKER_RE = re.compile(r"^<!--\s*jasi-memory\s+(\{.*\})\s*-->$", re.DOTALL)
_WHITESPACE_RE = re.compile(r"\s+")
_INDEXED_DOCUMENTS = frozenset({"MEMORY.md", "HISTORY.md"})


class MarkdownRecordParser:
    def __init__(self) -> None:
        self._markdown = MarkdownIt("commonmark")

    def parse(self, document: MemoryDocumentSnapshot) -> tuple[MemoryRecordDraft, ...]:
        if document.name not in _INDEXED_DOCUMENTS:
            return ()
        default_tier = "stable" if document.name == "MEMORY.md" else "episodic"
        tokens = self._markdown.parse(document.content)
        heading = ""
        pending_marker: dict[str, Any] | None = None
        records: list[MemoryRecordDraft] = []

        for index, token in enumerate(tokens):
            if token.type in {"html_block", "html_inline"}:
                parsed = _parse_marker(token.content.strip())
                if parsed is not None:
                    pending_marker = parsed
                continue
            if token.type != "inline" or index == 0:
                continue
            previous = tokens[index - 1]
            if previous.type == "heading_open":
                heading = _normalize(token.content)
                continue
            if previous.type != "paragraph_open":
                continue
            text = _normalize(token.content)
            if not text:
                pending_marker = None
                continue

            marker = pending_marker or {}
            tier = str(marker.get("tier") or default_tier)
            if tier not in {"stable", "episodic"}:
                raise ValueError(f"invalid memory tier in {document.name}: {tier}")
            tags = _string_tuple(marker.get("tags"))
            evidence = _integer_tuple(marker.get("evidence"))
            happened_at = _parse_datetime(marker.get("happened_at"))
            key = str(marker.get("id") or "").strip()
            if not key:
                digest = content_hash(f"{document.name}\n{heading}\n{text}")[:24]
                key = f"manual:{document.name}:{digest}"
            records.append(
                MemoryRecordDraft(
                    record_key=key,
                    tier=tier,
                    ordinal=len(records),
                    heading=heading,
                    content=text,
                    content_hash=content_hash(text),
                    tags=tags,
                    evidence_message_ids=evidence,
                    happened_at=happened_at,
                )
            )
            pending_marker = None
        return tuple(records)


class MarkdownMemoryIndexer:
    def __init__(
        self,
        *,
        store: MemoryDocumentStorePort,
        repository: MemoryIndexRepositoryPort,
        embedding: EmbeddingPort | None,
        parser: MarkdownRecordParser | None = None,
        embedding_batch_size: int = 16,
    ) -> None:
        if embedding_batch_size <= 0:
            raise ValueError("embedding batch size must be positive")
        self._store = store
        self._repository = repository
        self._embedding = embedding
        self._parser = parser or MarkdownRecordParser()
        self._embedding_batch_size = embedding_batch_size

    async def sync_scope(self, scope: MemoryScopeRecord) -> int:
        workspace = await asyncio.to_thread(
            self._store.ensure_workspace,
            scope.directory_name,
        )
        states = {
            state.name: state for state in await self._repository.load_document_states(scope.id)
        }
        changed = 0
        for document in workspace.documents:
            state = states.get(document.name)
            if state is not None and state.indexed_hash == document.content_hash:
                continue
            records = self._parser.parse(document)
            indexed = await self._embed_records(records)
            await self._repository.replace_document_index(scope, document, indexed)
            changed += 1
        return changed

    async def _embed_records(
        self,
        records: tuple[MemoryRecordDraft, ...],
    ) -> tuple[MemoryRecordDraft, ...]:
        if not records:
            return ()
        if self._embedding is None:
            return records
        indexed: list[MemoryRecordDraft] = []
        for start in range(0, len(records), self._embedding_batch_size):
            batch = records[start : start + self._embedding_batch_size]
            vectors = await self._embedding.embed(tuple(record.content for record in batch))
            indexed.extend(
                replace(
                    record,
                    embedding=vector,
                    embedding_model=self._embedding.model_name,
                )
                for record, vector in zip(batch, vectors, strict=True)
            )
        return tuple(indexed)


def render_memory_entry(
    *,
    record_key: str,
    tier: str,
    content: str,
    tags: tuple[str, ...] = (),
    evidence_message_ids: tuple[int, ...] = (),
    happened_at: datetime | None = None,
) -> str:
    marker: dict[str, Any] = {
        "id": record_key,
        "tier": tier,
        "tags": list(tags),
        "evidence": list(evidence_message_ids),
    }
    if happened_at is not None:
        marker["happened_at"] = happened_at.isoformat()
    text = _normalize(content)
    return (
        "<!-- jasi-memory "
        + json.dumps(marker, ensure_ascii=False, separators=(",", ":"))
        + " -->\n- "
        + text
    )


def _parse_marker(text: str) -> dict[str, Any] | None:
    match = _MARKER_RE.fullmatch(text)
    if match is None:
        return None
    try:
        payload = json.loads(match.group(1))
    except ValueError as exc:
        raise ValueError("invalid jasi-memory marker JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("jasi-memory marker must contain a JSON object")
    return payload


def _string_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(dict.fromkeys(text for item in value if (text := str(item).strip())))


def _integer_tuple(value: Any) -> tuple[int, ...]:
    if not isinstance(value, list):
        return ()
    result: list[int] = []
    for item in value:
        if isinstance(item, bool):
            continue
        try:
            number = int(item)
        except (TypeError, ValueError):
            continue
        if number > 0 and number not in result:
            result.append(number)
    return tuple(result)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"invalid memory happened_at value: {value}") from exc
    if parsed.tzinfo is None:
        raise ValueError("memory happened_at must include a timezone")
    return parsed


def _normalize(value: str) -> str:
    return _WHITESPACE_RE.sub(" ", value).strip()
