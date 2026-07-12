from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import replace
from datetime import UTC, datetime

from jasi.application.memory.indexing import (
    MarkdownMemoryIndexer,
    MarkdownRecordParser,
    render_memory_entry,
)
from jasi.domain.memory import (
    MemoryCandidate,
    MemoryConsolidationBatch,
    MemoryJobRecord,
    MemoryRecordDraft,
    MemoryScopeRecord,
    content_hash,
)
from jasi.ports.memory import (
    MemoryDocumentStorePort,
    MemoryIndexRepositoryPort,
    MemoryJobRepositoryPort,
)
from jasi.ports.memory_reasoning import MemoryMaintenanceReasonerPort

logger = logging.getLogger(__name__)

_MANAGED_MEMORY_START = "<!-- jasi-managed stable:start -->"
_MANAGED_MEMORY_END = "<!-- jasi-managed stable:end -->"
_PENDING_START = "<!-- jasi-managed pending:start -->"
_PENDING_END = "<!-- jasi-managed pending:end -->"


class MemoryConsolidator:
    def __init__(
        self,
        *,
        store: MemoryDocumentStorePort,
        repository: MemoryJobRepositoryPort,
        indexer: MarkdownMemoryIndexer,
        reasoner: MemoryMaintenanceReasonerPort,
        parser: MarkdownRecordParser | None = None,
        history_keep_count: int = 30,
    ) -> None:
        if history_keep_count <= 0:
            raise ValueError("memory history keep count must be positive")
        self._store = store
        self._repository = repository
        self._indexer = indexer
        self._reasoner = reasoner
        self._parser = parser or MarkdownRecordParser()
        self._history_keep_count = history_keep_count

    async def run(self, jobs: tuple[MemoryJobRecord, ...]) -> None:
        batch = await self._repository.load_consolidation_batch(
            jobs,
            history_keep_count=self._history_keep_count,
        )
        candidates = await self._reasoner.extract(batch.messages)
        stable = tuple(item for item in candidates if item.tier == "stable")
        episodic = tuple(item for item in candidates if item.tier == "episodic")

        workspace = await asyncio.to_thread(
            self._store.ensure_workspace,
            batch.scope.directory_name,
        )
        pending = workspace.document("PENDING.md")
        staged_pending = _replace_managed_block(
            pending.content,
            _PENDING_START,
            _PENDING_END,
            "## Extracted Candidates",
            tuple(_render_candidate(item) for item in candidates),
        )
        await asyncio.to_thread(
            self._store.write_document,
            batch.scope.directory_name,
            "PENDING.md",
            staged_pending,
            expected_hash=pending.content_hash,
        )

        workspace = await asyncio.to_thread(
            self._store.read_workspace,
            batch.scope.directory_name,
        )
        memory_document = workspace.document("MEMORY.md")
        existing = self._parser.parse(memory_document)
        decisions = await self._reasoner.reconcile_stable(existing, stable)
        managed = _apply_stable_decisions(existing, stable, decisions)
        memory_content = _replace_managed_block(
            memory_document.content,
            _MANAGED_MEMORY_START,
            _MANAGED_MEMORY_END,
            "## Managed Stable Memory",
            tuple(_render_record(item) for item in managed),
        )
        await asyncio.to_thread(
            self._store.write_document,
            batch.scope.directory_name,
            "MEMORY.md",
            memory_content,
            expected_hash=memory_document.content_hash,
        )

        workspace = await asyncio.to_thread(
            self._store.read_workspace,
            batch.scope.directory_name,
        )
        history_document = workspace.document("HISTORY.md")
        history_content = _append_history(
            history_document.content,
            self._parser.parse(history_document),
            episodic,
        )
        await asyncio.to_thread(
            self._store.write_document,
            batch.scope.directory_name,
            "HISTORY.md",
            history_content,
            expected_hash=history_document.content_hash,
        )

        if batch.summary_messages:
            await self._update_summary(batch)

        workspace = await asyncio.to_thread(
            self._store.read_workspace,
            batch.scope.directory_name,
        )
        pending = workspace.document("PENDING.md")
        cleared_pending = _replace_managed_block(
            pending.content,
            _PENDING_START,
            _PENDING_END,
            "## Extracted Candidates",
            (),
        )
        await asyncio.to_thread(
            self._store.write_document,
            batch.scope.directory_name,
            "PENDING.md",
            cleared_pending,
            expected_hash=pending.content_hash,
        )

        await self._indexer.sync_scope(batch.scope)
        await self._repository.commit_consolidation(batch)

    async def _update_summary(self, batch: MemoryConsolidationBatch) -> None:
        workspace = await asyncio.to_thread(
            self._store.read_workspace,
            batch.scope.directory_name,
        )
        document = workspace.document("RECENT_CONTEXT.md")
        current = _summary_body(document.content, batch.conversation_id)
        summary = await self._reasoner.summarize(current, batch.summary_messages)
        updated = _replace_summary(
            document.content,
            conversation_id=batch.conversation_id,
            through_sequence=batch.summary_through_sequence or 0,
            summary=summary,
        )
        await asyncio.to_thread(
            self._store.write_document,
            batch.scope.directory_name,
            "RECENT_CONTEXT.md",
            updated,
            expected_hash=document.content_hash,
        )


class MemoryWorker:
    def __init__(
        self,
        *,
        repository: MemoryJobRepositoryPort,
        index_repository: MemoryIndexRepositoryPort,
        store: MemoryDocumentStorePort,
        indexer: MarkdownMemoryIndexer,
        consolidator: MemoryConsolidator,
        wakeup: asyncio.Event,
        batch_size: int,
        consolidation_batch_messages: int,
        lease_seconds: float,
        reconcile_seconds: float,
        idle_sleep_seconds: float = 1.0,
    ) -> None:
        if batch_size <= 0 or lease_seconds <= 0 or reconcile_seconds <= 0:
            raise ValueError("memory worker limits must be positive")
        if not 4 <= consolidation_batch_messages <= 8:
            raise ValueError("memory consolidation batch must contain 4 to 8 messages")
        self._repository = repository
        self._index_repository = index_repository
        self._store = store
        self._indexer = indexer
        self._consolidator = consolidator
        self._wakeup = wakeup
        self._batch_size = batch_size
        self._consolidation_batch_messages = consolidation_batch_messages
        self._lease_seconds = lease_seconds
        self._reconcile_seconds = reconcile_seconds
        self._idle_sleep_seconds = idle_sleep_seconds
        self._last_reconcile = 0.0

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("memory worker started")
        try:
            while not stop_event.is_set():
                self._wakeup.clear()
                try:
                    processed = await self.drain_once()
                except Exception:
                    logger.exception("memory worker iteration failed")
                    processed = 0
                if processed:
                    continue
                try:
                    await asyncio.wait_for(
                        self._wakeup.wait(),
                        timeout=self._idle_sleep_seconds,
                    )
                except TimeoutError:
                    pass
        finally:
            logger.info("memory worker stopped")

    async def drain_once(self) -> int:
        loop_time = asyncio.get_running_loop().time()
        if loop_time - self._last_reconcile >= self._reconcile_seconds:
            await self.reconcile_once()
            self._last_reconcile = loop_time

        now = datetime.now(UTC)
        consolidate_jobs = await self._repository.claim_jobs(
            kind="consolidate",
            limit=self._batch_size,
            lease_seconds=self._lease_seconds,
            consolidation_batch_messages=self._consolidation_batch_messages,
            now=now,
        )
        groups = _job_groups(consolidate_jobs)
        for jobs in groups:
            try:
                await self._consolidator.run(jobs)
            except Exception as exc:
                logger.exception("memory consolidation failed jobs=%s", [job.id for job in jobs])
                await self._repository.fail_jobs(jobs, str(exc), datetime.now(UTC))

        reindex_jobs = await self._repository.claim_jobs(
            kind="reindex",
            limit=self._batch_size,
            lease_seconds=self._lease_seconds,
            consolidation_batch_messages=self._consolidation_batch_messages,
            now=datetime.now(UTC),
        )
        for jobs in _job_groups(reindex_jobs):
            try:
                scope = MemoryScopeRecord(
                    id=jobs[0].scope_id,
                    scope_key=str(jobs[0].payload.get("scope_key") or ""),
                    directory_name=jobs[0].scope_directory,
                )
                await self._indexer.sync_scope(scope)
                await self._repository.complete_jobs(jobs)
            except Exception as exc:
                logger.exception("memory reindex failed jobs=%s", [job.id for job in jobs])
                await self._repository.fail_jobs(jobs, str(exc), datetime.now(UTC))
        return len(groups) + len(_job_groups(reindex_jobs))

    async def reconcile_once(self) -> int:
        enqueued = 0
        for scope in await self._index_repository.list_scopes():
            try:
                workspace = await asyncio.to_thread(
                    self._store.ensure_workspace,
                    scope.directory_name,
                )
                states = {
                    state.name: state
                    for state in await self._index_repository.load_document_states(scope.id)
                }
                changed = [
                    document
                    for document in workspace.documents
                    if states.get(document.name) is None
                    or states[document.name].indexed_hash != document.content_hash
                ]
                if not changed:
                    continue
                digest = content_hash("|".join(document.content_hash for document in changed))
                created = await self._repository.enqueue_reindex(
                    scope.id,
                    f"reindex:{scope.id}:{digest}",
                    {
                        "scope_key": scope.scope_key,
                        "documents": [document.name for document in changed],
                    },
                )
                enqueued += int(created)
            except Exception:
                logger.exception("memory reconciliation failed scope_id=%s", scope.id)
        if enqueued:
            self._wakeup.set()
        return enqueued


def _apply_stable_decisions(
    existing: tuple[MemoryRecordDraft, ...],
    candidates: tuple[MemoryCandidate, ...],
    decisions: tuple,
) -> tuple[MemoryRecordDraft, ...]:
    managed = {
        record.record_key: record
        for record in existing
        if not record.record_key.startswith("manual:") and record.tier == "stable"
    }
    for decision in decisions:
        candidate = candidates[decision.candidate_index]
        draft = MemoryRecordDraft(
            record_key=candidate.record_key,
            tier="stable",
            ordinal=len(managed),
            heading="Managed Stable Memory",
            content=candidate.content,
            content_hash=content_hash(candidate.content),
            tags=candidate.tags,
            evidence_message_ids=candidate.evidence_message_ids,
            happened_at=candidate.happened_at,
        )
        if decision.action == "ignore":
            continue
        if decision.action == "add":
            prior = managed.get(draft.record_key)
            managed[draft.record_key] = _merge_evidence(prior, draft) if prior else draft
            continue
        target = managed.get(decision.target_record_key or "")
        if target is None:
            managed[draft.record_key] = draft
        elif decision.action == "replace":
            managed[target.record_key] = replace(
                draft,
                record_key=target.record_key,
                evidence_message_ids=tuple(
                    dict.fromkeys((*target.evidence_message_ids, *draft.evidence_message_ids))
                ),
            )
        else:
            managed[target.record_key] = replace(
                target,
                evidence_message_ids=tuple(
                    dict.fromkeys((*target.evidence_message_ids, *draft.evidence_message_ids))
                ),
            )
    return tuple(replace(record, ordinal=index) for index, record in enumerate(managed.values()))


def _merge_evidence(
    existing: MemoryRecordDraft,
    candidate: MemoryRecordDraft,
) -> MemoryRecordDraft:
    return replace(
        candidate,
        evidence_message_ids=tuple(
            dict.fromkeys((*existing.evidence_message_ids, *candidate.evidence_message_ids))
        ),
    )


def _render_candidate(candidate: MemoryCandidate) -> str:
    return render_memory_entry(
        record_key=candidate.record_key,
        tier=candidate.tier,
        content=candidate.content,
        tags=candidate.tags,
        evidence_message_ids=candidate.evidence_message_ids,
        happened_at=candidate.happened_at,
    )


def _render_record(record: MemoryRecordDraft) -> str:
    return render_memory_entry(
        record_key=record.record_key,
        tier=record.tier,
        content=record.content,
        tags=record.tags,
        evidence_message_ids=record.evidence_message_ids,
        happened_at=record.happened_at,
    )


def _append_history(
    content: str,
    existing: tuple[MemoryRecordDraft, ...],
    candidates: tuple[MemoryCandidate, ...],
) -> str:
    keys = {record.record_key for record in existing}
    additions = [_render_candidate(item) for item in candidates if item.record_key not in keys]
    if not additions:
        return content
    return content.rstrip() + "\n\n" + "\n\n".join(additions) + "\n"


def _replace_managed_block(
    content: str,
    start: str,
    end: str,
    heading: str,
    entries: tuple[str, ...],
) -> str:
    body = heading + "\n\n" + start
    if entries:
        body += "\n\n" + "\n\n".join(entries)
    body += "\n\n" + end
    start_index = content.find(start)
    end_index = content.find(end)
    if start_index >= 0 and end_index >= start_index:
        heading_index = content.rfind(heading, 0, start_index)
        replace_from = heading_index if heading_index >= 0 else start_index
        return content[:replace_from].rstrip() + "\n\n" + body + content[end_index + len(end) :]
    return content.rstrip() + "\n\n" + body + "\n"


def _summary_markers(conversation_id: int) -> tuple[str, str]:
    return (
        f"<!-- jasi-summary conversation={conversation_id}:start -->",
        f"<!-- jasi-summary conversation={conversation_id}:end -->",
    )


def _summary_body(content: str, conversation_id: int) -> str:
    start, end = _summary_markers(conversation_id)
    start_index = content.find(start)
    end_index = content.find(end)
    if start_index < 0 or end_index < start_index:
        return ""
    body = content[start_index + len(start) : end_index].strip()
    lines = body.splitlines()
    if lines and lines[0].startswith("through_sequence:"):
        lines = lines[1:]
    return "\n".join(lines).strip()


def _replace_summary(
    content: str,
    *,
    conversation_id: int,
    through_sequence: int,
    summary: str,
) -> str:
    start, end = _summary_markers(conversation_id)
    body = f"{start}\nthrough_sequence: {through_sequence}\n{summary.strip()}\n{end}"
    start_index = content.find(start)
    end_index = content.find(end)
    if start_index >= 0 and end_index >= start_index:
        return content[:start_index] + body + content[end_index + len(end) :]
    return content.rstrip() + "\n\n" + body + "\n"


def _job_groups(jobs: list[MemoryJobRecord]) -> list[tuple[MemoryJobRecord, ...]]:
    groups: dict[tuple[int, str | None], list[MemoryJobRecord]] = defaultdict(list)
    for job in jobs:
        groups[(job.scope_id, job.lease_token)].append(job)
    return [tuple(group) for group in groups.values()]
