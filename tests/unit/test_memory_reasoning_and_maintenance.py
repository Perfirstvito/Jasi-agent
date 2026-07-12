from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

import pytest

from jasi.adapters.persistence.markdown.memory_store import MarkdownMemoryStore
from jasi.application.memory.maintenance import MemoryConsolidator, MemoryWorker
from jasi.application.memory.reasoning import MemoryReasoningFailure, ModelMemoryReasoner
from jasi.application.memory.retrieval import MemoryContextService
from jasi.domain.memory import (
    MemoryCandidate,
    MemoryConsolidationBatch,
    MemoryJobRecord,
    MemoryMessage,
    MemoryRecordDraft,
    MemoryScopeRecord,
    MemorySearchHit,
    StableMemoryDecision,
    content_hash,
)
from jasi.runtime.models import ModelResponse
from tests.unit.fakes import FakeModel


def _message(message_id: int, role: str, content: str, sequence: int) -> MemoryMessage:
    return MemoryMessage(
        id=message_id,
        conversation_id=1,
        sequence=sequence,
        role=role,
        origin="telegram" if role == "user" else "model",
        content=content,
        created_at=datetime.now(UTC),
    )


@pytest.mark.asyncio
async def test_memory_worker_survives_a_transient_iteration_failure() -> None:
    stop_event = asyncio.Event()

    class RecoveringWorker(MemoryWorker):
        def __init__(self) -> None:
            self._wakeup = asyncio.Event()
            self._idle_sleep_seconds = 0.001
            self.calls = 0

        async def drain_once(self) -> int:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("transient database failure")
            stop_event.set()
            return 0

    worker = RecoveringWorker()
    await worker.run(stop_event)

    assert worker.calls == 2


@pytest.mark.asyncio
async def test_memory_reasoner_only_accepts_user_grounded_candidates() -> None:
    model = FakeModel(
        [
            ModelResponse(
                content=(
                    '{"items":['
                    '{"tier":"stable","content":"The user prefers PostgreSQL.",'
                    '"tags":["database"],"evidence_message_ids":[1],"happened_at":null},'
                    '{"tier":"stable","content":"The assistant recommends Redis.",'
                    '"tags":[],"evidence_message_ids":[2],"happened_at":null}'
                    "]}"
                )
            )
        ]
    )
    reasoner = ModelMemoryReasoner(
        model=model,
        model_name="memory-model",
        timeout_seconds=5,
    )

    candidates = await reasoner.extract(
        (
            _message(1, "user", "I prefer PostgreSQL.", 1),
            _message(2, "assistant", "You should use Redis.", 2),
        )
    )

    assert len(candidates) == 1
    assert candidates[0].content == "The user prefers PostgreSQL."
    assert candidates[0].evidence_message_ids == (1,)
    assert candidates[0].record_key.startswith("auto:")


@pytest.mark.asyncio
async def test_memory_reasoner_runs_full_retrieval_reasoning_contract() -> None:
    hit = MemorySearchHit(
        record_id=7,
        record_key="memory-7",
        tier="stable",
        content="The user prefers PostgreSQL.",
        tags=("database",),
    )
    model = FakeModel(
        [
            ModelResponse(content='{"retrieve":true,"reason":"personal preference"}'),
            ModelResponse(content='{"query":"user preferred database"}'),
            ModelResponse(content='{"hypothesis":"The user prefers PostgreSQL."}'),
            ModelResponse(content='{"scores":[{"record_id":7,"score":0.95}]}'),
            ModelResponse(content='{"sufficient":true}'),
        ]
    )
    reasoner = ModelMemoryReasoner(
        model=model,
        model_name="memory-model",
        timeout_seconds=5,
    )

    assert await reasoner.gate("Which database do I prefer?") == (
        True,
        "personal preference",
    )
    assert await reasoner.rewrite("Which database do I prefer?") == "user preferred database"
    assert await reasoner.hyde("Which database do I prefer?") == ("The user prefers PostgreSQL.")
    assert await reasoner.rerank("Which database?", (hit,)) == ((7, 0.95),)
    assert await reasoner.sufficient("Which database?", (hit,)) is True
    assert len(model.requests) == 5


@pytest.mark.asyncio
async def test_memory_reasoner_cannot_replace_manual_memory() -> None:
    model = FakeModel(
        [
            ModelResponse(
                content=(
                    '{"decisions":[{"candidate_index":0,"action":"replace",'
                    '"target_record_key":"manual:MEMORY.md:one"}]}'
                )
            )
        ]
    )
    reasoner = ModelMemoryReasoner(
        model=model,
        model_name="memory-model",
        timeout_seconds=5,
    )
    existing = MemoryRecordDraft(
        record_key="manual:MEMORY.md:one",
        tier="stable",
        ordinal=0,
        heading="Manual",
        content="The manually curated fact is authoritative.",
        content_hash=content_hash("The manually curated fact is authoritative."),
    )
    candidate = MemoryCandidate(
        record_key="auto:one",
        tier="stable",
        content="A conflicting candidate.",
        tags=(),
        evidence_message_ids=(1,),
    )

    decisions = await reasoner.reconcile_stable((existing,), (candidate,))

    assert decisions == (StableMemoryDecision(candidate_index=0, action="ignore"),)


@pytest.mark.asyncio
async def test_memory_reasoner_requires_real_json_booleans() -> None:
    reasoner = ModelMemoryReasoner(
        model=FakeModel([ModelResponse(content='{"retrieve":"false","reason":"none"}')]),
        model_name="memory-model",
        timeout_seconds=5,
    )

    with pytest.raises(MemoryReasoningFailure, match="invalid retrieve"):
        await reasoner.gate("hello")


@pytest.mark.asyncio
async def test_consolidator_updates_authoritative_markdown_idempotently(tmp_path: Path) -> None:
    scope = MemoryScopeRecord(id=1, scope_key="owner", directory_name="scope-1")
    jobs = tuple(
        MemoryJobRecord(
            id=index,
            scope_id=1,
            scope_directory="scope-1",
            conversation_id=1,
            kind="consolidate",
            trigger_message_id=index * 2,
            status="running",
            attempts=1,
            lease_token="lease",
            payload={},
        )
        for index in range(1, 4)
    )
    messages = tuple(
        _message(index, "user" if index % 2 else "assistant", f"message {index}", index)
        for index in range(1, 7)
    )
    batch = MemoryConsolidationBatch(
        scope=scope,
        conversation_id=1,
        jobs=jobs,
        messages=messages,
        through_sequence=6,
    )
    stable = MemoryCandidate(
        record_key="stable-1",
        tier="stable",
        content="The user prefers concise answers.",
        tags=("preference",),
        evidence_message_ids=(1,),
    )
    episodic = MemoryCandidate(
        record_key="episode-1",
        tier="episodic",
        content="The user began testing Jasi memory.",
        tags=("project",),
        evidence_message_ids=(3,),
    )

    class Repository:
        def __init__(self) -> None:
            self.commits = 0

        async def load_consolidation_batch(self, _jobs, *, history_keep_count):
            assert history_keep_count == 30
            return batch

        async def commit_consolidation(self, committed):
            assert committed is batch
            self.commits += 1

    class Reasoner:
        pending_during_reconcile = ""

        async def extract(self, _messages):
            return (stable, episodic)

        async def reconcile_stable(self, _existing, candidates):
            self.pending_during_reconcile = (
                store.read_workspace(scope.directory_name).document("PENDING.md").content
            )
            return tuple(
                StableMemoryDecision(candidate_index=index, action="add")
                for index in range(len(candidates))
            )

        async def summarize(self, _existing, _messages):
            raise AssertionError("summary should not run inside the raw history window")

    class Indexer:
        def __init__(self) -> None:
            self.calls = 0

        async def sync_scope(self, synced_scope):
            assert synced_scope == scope
            self.calls += 1
            return 2

    store = MarkdownMemoryStore(tmp_path)
    repository = Repository()
    indexer = Indexer()
    reasoner = Reasoner()
    consolidator = MemoryConsolidator(
        store=store,
        repository=repository,
        indexer=indexer,
        reasoner=reasoner,
    )

    await consolidator.run(jobs)
    await consolidator.run(jobs)

    workspace = store.read_workspace(scope.directory_name)
    memory = workspace.document("MEMORY.md").content
    history = workspace.document("HISTORY.md").content
    pending = workspace.document("PENDING.md").content
    assert memory.count("stable-1") == 1
    assert "The user prefers concise answers." in memory
    assert history.count("episode-1") == 1
    assert "stable-1" not in pending
    assert "stable-1" in reasoner.pending_during_reconcile
    assert "episode-1" in reasoner.pending_during_reconcile
    assert repository.commits == 2
    assert indexer.calls == 2


@pytest.mark.asyncio
async def test_memory_context_runs_hybrid_pipeline_and_audits_injected_hits(
    tmp_path: Path,
) -> None:
    scope = MemoryScopeRecord(id=1, scope_key="owner", directory_name="scope-1")
    store = MarkdownMemoryStore(tmp_path)
    workspace = store.ensure_workspace(scope.directory_name)
    memory = workspace.document("MEMORY.md")
    store.write_document(
        scope.directory_name,
        "MEMORY.md",
        "# Long-term Memory\n\n- The user values concise answers.",
        expected_hash=memory.content_hash,
    )
    base_hit = MemorySearchHit(
        record_id=7,
        record_key="memory-7",
        tier="episodic",
        content="The user selected PostgreSQL for Jasi.",
        tags=("database",),
        semantic_score=0.8,
        lexical_score=0.2,
        final_score=0.65,
    )

    class Repository:
        def __init__(self) -> None:
            self.audit = None
            self.searches = 0

        async def resolve_scope(self, _conversation_id):
            return scope

        async def search_records(self, **_kwargs):
            self.searches += 1
            return [base_hit]

        async def record_retrieval(self, audit):
            self.audit = audit

    class Embedding:
        model_name = "fake"

        async def embed(self, texts):
            assert len(texts) == 2
            return ((1.0, 0.0), (0.0, 1.0))

    class Reasoner:
        async def gate(self, _query):
            return True, "personal history"

        async def rewrite(self, _query):
            return "Jasi database choice"

        async def hyde(self, _query):
            return "The user selected a relational database for Jasi."

        async def rerank(self, _query, hits):
            return tuple((hit.record_id, 0.9) for hit in hits)

        async def sufficient(self, _query, _hits):
            return True

    repository = Repository()
    service = MemoryContextService(
        repository=repository,
        store=store,
        embedding=Embedding(),
        reasoner=Reasoner(),
        max_context_chars=1000,
    )

    context = await service.load_context(
        conversation_id=1,
        query_text="Which database did I choose?",
        turn_id=10,
    )

    assert [item.kind for item in context] == ["stable_memory", "episodic_memory"]
    assert "concise answers" in context[0].content
    assert "PostgreSQL" in context[1].content
    assert repository.searches == 3
    assert repository.audit.gate_decision == "retrieve"
    assert repository.audit.sufficient is True
    assert repository.audit.hits[0].injected is True


@pytest.mark.asyncio
async def test_memory_context_uses_audited_lexical_pipeline_without_embeddings(
    tmp_path: Path,
) -> None:
    scope = MemoryScopeRecord(id=1, scope_key="owner", directory_name="scope-1")
    store = MarkdownMemoryStore(tmp_path)
    store.ensure_workspace(scope.directory_name)
    hit = MemorySearchHit(
        record_id=8,
        record_key="episode-8",
        tier="episodic",
        content="The user selected SQLite for a local prototype.",
        tags=("database",),
        lexical_score=0.8,
        final_score=0.8,
    )

    class Repository:
        def __init__(self) -> None:
            self.searches = []
            self.audit = None

        async def resolve_scope(self, _conversation_id):
            return scope

        async def search_records(self, **kwargs):
            self.searches.append(kwargs)
            return [hit]

        async def record_retrieval(self, audit):
            self.audit = audit

    class Reasoner:
        async def gate(self, _query):
            return True, "prior choice"

        async def rewrite(self, _query):
            return "user local database choice"

        async def hyde(self, _query):
            raise AssertionError("HyDE is only useful when vector retrieval is enabled")

        async def rerank(self, _query, hits):
            return tuple((item.record_id, 0.9) for item in hits)

        async def sufficient(self, _query, _hits):
            return True

    repository = Repository()
    service = MemoryContextService(
        repository=repository,
        store=store,
        embedding=None,
        reasoner=Reasoner(),
    )

    context = await service.load_context(
        conversation_id=1,
        query_text="Which local database did I select?",
        turn_id=11,
    )

    assert [item.kind for item in context] == ["episodic_memory"]
    assert len(repository.searches) == 2
    assert all(search["query_embedding"] is None for search in repository.searches)
    assert repository.audit.hyde_text is None
    assert repository.audit.trace["retrieval_mode"] == "lexical"
    assert repository.audit.hits[0].injected is True

    constrained = MemoryContextService(
        repository=repository,
        store=store,
        embedding=None,
        reasoner=Reasoner(),
        max_context_chars=10,
    )
    assert (
        await constrained.load_context(
            conversation_id=1,
            query_text="Which local database did I select?",
            turn_id=12,
        )
        == ()
    )
    assert repository.audit.hits[0].injected is False
