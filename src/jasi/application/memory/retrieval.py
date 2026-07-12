from __future__ import annotations

import asyncio
import logging
from dataclasses import replace

from jasi.domain.context import ContextItem
from jasi.domain.memory import MemoryRetrievalAudit, MemorySearchHit
from jasi.ports.embedding import EmbeddingPort
from jasi.ports.memory import MemoryDocumentStorePort, MemoryIndexRepositoryPort
from jasi.ports.memory_reasoning import MemoryRetrievalReasonerPort

logger = logging.getLogger(__name__)
_RECALLED_TIERS = frozenset({"episodic"})


class MemoryContextService:
    def __init__(
        self,
        *,
        repository: MemoryIndexRepositoryPort,
        store: MemoryDocumentStorePort,
        embedding: EmbeddingPort | None,
        reasoner: MemoryRetrievalReasonerPort,
        search_limit: int = 12,
        inject_limit: int = 6,
        score_threshold: float = 0.35,
        max_context_chars: int = 6000,
    ) -> None:
        if search_limit <= 0 or inject_limit <= 0 or max_context_chars <= 0:
            raise ValueError("memory retrieval limits must be positive")
        self._repository = repository
        self._store = store
        self._embedding = embedding
        self._reasoner = reasoner
        self._search_limit = search_limit
        self._inject_limit = inject_limit
        self._score_threshold = score_threshold
        self._max_context_chars = max_context_chars

    async def load_context(
        self,
        *,
        conversation_id: int,
        query_text: str,
        turn_id: int,
    ) -> tuple[ContextItem, ...]:
        scope = await self._repository.resolve_scope(conversation_id)
        workspace = await asyncio.to_thread(
            self._store.ensure_workspace,
            scope.directory_name,
        )
        stable = _document_body(workspace.document("MEMORY.md").content)
        recent = _document_body(workspace.document("RECENT_CONTEXT.md").content)
        context: list[ContextItem] = []
        if stable:
            context.append(
                ContextItem(
                    kind="stable_memory",
                    content=stable,
                    trust="derived",
                    references=(f"memory-scope:{scope.id}:MEMORY.md",),
                )
            )
        if recent:
            context.append(
                ContextItem(
                    kind="recent_summary",
                    content=recent,
                    trust="derived",
                    references=(f"memory-scope:{scope.id}:RECENT_CONTEXT.md",),
                )
            )

        gate_decision = "retrieve"
        trace: dict[str, object] = {}
        try:
            should_retrieve, gate_reason = await self._reasoner.gate(query_text)
            trace["gate_reason"] = gate_reason
        except Exception as exc:
            logger.warning("memory gate failed; falling back to retrieval: %s", exc)
            should_retrieve = True
            gate_decision = "fallback"
            trace["gate_error"] = exc.__class__.__name__

        rewritten: str | None = None
        hyde_text: str | None = None
        ranked: tuple[MemorySearchHit, ...] = ()
        sufficient = False
        if should_retrieve:
            rewritten, hyde_text, ranked, sufficient, retrieval_trace = await self._retrieve(
                scope.id,
                query_text,
            )
            trace.update(retrieval_trace)
        else:
            gate_decision = "skip"

        remaining = max(
            0,
            self._max_context_chars - sum(len(item.content) for item in context),
        )
        injected: list[MemorySearchHit] = []
        if sufficient:
            for hit in ranked[: self._inject_limit]:
                rendered = _render_hit(hit)
                if len(rendered) > remaining:
                    continue
                remaining -= len(rendered)
                injected_hit = replace(hit, injected=True)
                injected.append(injected_hit)
                context.append(
                    ContextItem(
                        kind=f"{hit.tier}_memory",
                        content=rendered,
                        trust="derived",
                        references=(f"memory-record:{hit.record_key}",),
                    )
                )

        injected_ids = {hit.record_id for hit in injected}
        audited_hits = tuple(replace(hit, injected=hit.record_id in injected_ids) for hit in ranked)
        try:
            await self._repository.record_retrieval(
                MemoryRetrievalAudit(
                    turn_id=turn_id,
                    scope_id=scope.id,
                    query=query_text,
                    rewritten_query=rewritten,
                    hyde_text=hyde_text,
                    gate_decision=gate_decision,
                    sufficient=sufficient,
                    trace=trace,
                    hits=audited_hits,
                )
            )
        except Exception:
            logger.exception("memory retrieval audit failed turn_id=%s", turn_id)
        return tuple(context)

    async def _retrieve(
        self,
        scope_id: int,
        query_text: str,
    ) -> tuple[str, str | None, tuple[MemorySearchHit, ...], bool, dict[str, object]]:
        trace: dict[str, object] = {}
        rewritten, rewrite_error = await _fallback_text(
            self._reasoner.rewrite(query_text),
            query_text,
        )
        if rewrite_error:
            trace["rewrite_error"] = rewrite_error

        hyde_text: str | None = None
        rewritten_vector: tuple[float, ...] | None = None
        hyde_vector: tuple[float, ...] | None = None
        if self._embedding is not None:
            hyde_text, hyde_error = await _fallback_text(
                self._reasoner.hyde(query_text),
                query_text,
            )
            if hyde_error:
                trace["hyde_error"] = hyde_error
            try:
                rewritten_vector, hyde_vector = await self._embedding.embed((rewritten, hyde_text))
            except Exception as exc:
                logger.warning("memory query embedding failed; using lexical search: %s", exc)
                trace["embedding_error"] = exc.__class__.__name__
                rewritten_vector = None
                hyde_vector = None
        else:
            trace["retrieval_mode"] = "lexical"

        searches = [
            self._repository.search_records(
                scope_id=scope_id,
                query_text=rewritten,
                query_embedding=rewritten_vector,
                limit=self._search_limit,
                tiers=_RECALLED_TIERS,
            ),
            self._repository.search_records(
                scope_id=scope_id,
                query_text=query_text,
                query_embedding=None,
                limit=self._search_limit,
                tiers=_RECALLED_TIERS,
            ),
        ]
        if hyde_vector is not None and hyde_text is not None:
            searches.append(
                self._repository.search_records(
                    scope_id=scope_id,
                    query_text=hyde_text,
                    query_embedding=hyde_vector,
                    limit=self._search_limit,
                    tiers=_RECALLED_TIERS,
                )
            )
        try:
            result_sets = await asyncio.gather(*searches)
        except Exception as exc:
            logger.warning("memory search failed: %s", exc)
            trace["search_error"] = exc.__class__.__name__
            return rewritten, hyde_text, (), False, trace

        merged = _merge_hits(result_sets)
        candidates = tuple(
            hit
            for hit in sorted(merged.values(), key=lambda item: item.final_score, reverse=True)
            if hit.final_score >= self._score_threshold
        )[: self._search_limit]
        if not candidates:
            return rewritten, hyde_text, (), False, trace

        try:
            rerank_scores = dict(await self._reasoner.rerank(query_text, candidates))
            candidates = tuple(
                sorted(
                    (
                        replace(
                            hit,
                            rerank_score=rerank_scores[hit.record_id],
                            final_score=(hit.final_score * 0.6)
                            + (rerank_scores[hit.record_id] * 0.4),
                        )
                        for hit in candidates
                    ),
                    key=lambda item: item.final_score,
                    reverse=True,
                )
            )
        except Exception as exc:
            logger.warning("memory rerank failed; preserving hybrid order: %s", exc)
            trace["rerank_error"] = exc.__class__.__name__

        try:
            sufficient = await self._reasoner.sufficient(query_text, candidates)
        except Exception as exc:
            logger.warning("memory sufficiency check failed; using ranked hits: %s", exc)
            sufficient = bool(candidates)
            trace["sufficiency_error"] = exc.__class__.__name__
        trace["candidate_count"] = len(candidates)
        return rewritten, hyde_text, candidates, sufficient, trace


async def _fallback_text(awaitable, fallback: str) -> tuple[str, str | None]:
    try:
        value = (await awaitable).strip()
    except Exception as exc:
        return fallback, exc.__class__.__name__
    return (value or fallback), None


def _merge_hits(result_sets: list[list[MemorySearchHit]]) -> dict[int, MemorySearchHit]:
    merged: dict[int, MemorySearchHit] = {}
    for hits in result_sets:
        for hit in hits:
            existing = merged.get(hit.record_id)
            if existing is None:
                merged[hit.record_id] = hit
                continue
            semantic = max(existing.semantic_score, hit.semantic_score)
            lexical = max(existing.lexical_score, hit.lexical_score)
            merged[hit.record_id] = replace(
                existing,
                semantic_score=semantic,
                lexical_score=lexical,
                final_score=_hybrid_score(semantic, lexical),
            )
    return merged


def _hybrid_score(semantic: float, lexical: float) -> float:
    if semantic <= 0:
        return lexical
    return (semantic * 0.75) + (lexical * 0.25)


def _document_body(content: str) -> str:
    lines = content.strip().splitlines()
    while lines and (not lines[0].strip() or lines[0].lstrip().startswith("#")):
        lines.pop(0)
    return "\n".join(lines).strip()


def _render_hit(hit: MemorySearchHit) -> str:
    tags = f" tags={','.join(hit.tags)}" if hit.tags else ""
    return f"[memory id={hit.record_key}{tags}] {hit.content}"
