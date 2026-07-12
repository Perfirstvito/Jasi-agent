from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, replace

from jasi.domain.context import ContextItem
from jasi.domain.memory import (
    MemoryQueryKind,
    MemoryQueryVariant,
    MemoryRetrievalAudit,
    MemorySearchHit,
)
from jasi.ports.embedding import EmbeddingPort
from jasi.ports.memory import MemoryDocumentStorePort, MemoryIndexRepositoryPort
from jasi.ports.memory_reasoning import MemoryRetrievalReasonerPort

logger = logging.getLogger(__name__)
_RECALLED_TIERS = frozenset({"episodic"})


@dataclass(frozen=True)
class _SearchQuery:
    kind: MemoryQueryKind
    text: str
    embedding: tuple[float, ...] | None = None
    search: bool = True


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
        query_variants = (
            MemoryQueryVariant(
                kind="original",
                text=query_text,
                semantic=False,
                lexical=False,
            ),
        )
        sufficient = False
        if should_retrieve:
            (
                rewritten,
                hyde_text,
                ranked,
                sufficient,
                retrieval_trace,
                query_variants,
            ) = await self._retrieve(scope.id, query_text)
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
                    reasoning_model=self._reasoner.model_name,
                    query_variants=query_variants,
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
    ) -> tuple[
        str,
        str | None,
        tuple[MemorySearchHit, ...],
        bool,
        dict[str, object],
        tuple[MemoryQueryVariant, ...],
    ]:
        trace: dict[str, object] = {}
        rewritten, rewrite_error = await _fallback_text(
            self._reasoner.rewrite(query_text),
            query_text,
        )
        if rewrite_error:
            trace["rewrite_error"] = rewrite_error

        hyde_text: str | None = None
        hyde_usable = False
        embeddings: dict[str, tuple[float, ...]] = {}
        if self._embedding is not None:
            hyde_text, hyde_error = await _fallback_text(
                self._reasoner.hyde(query_text),
                query_text,
            )
            if hyde_error:
                trace["hyde_error"] = hyde_error
            else:
                hyde_usable = True
            embedding_texts = [query_text]
            if rewrite_error is None and rewritten != query_text:
                embedding_texts.append(rewritten)
            if hyde_usable and hyde_text not in embedding_texts:
                embedding_texts.append(hyde_text)
            try:
                vectors = await self._embedding.embed(tuple(embedding_texts))
                embeddings = dict(zip(embedding_texts, vectors, strict=True))
                trace["retrieval_mode"] = "hybrid"
            except Exception as exc:
                logger.warning("memory query embedding failed; using lexical search: %s", exc)
                trace["embedding_error"] = exc.__class__.__name__
                trace["retrieval_mode"] = "lexical_fallback"
        else:
            trace["retrieval_mode"] = "lexical"

        plans = [
            _SearchQuery(
                kind="original",
                text=query_text,
                embedding=embeddings.get(query_text),
            ),
            _SearchQuery(
                kind="rewritten",
                text=rewritten,
                embedding=embeddings.get(rewritten),
                search=rewrite_error is None and rewritten != query_text,
            ),
        ]
        if hyde_text is not None:
            plans.append(
                _SearchQuery(
                    kind="hyde",
                    text=hyde_text,
                    embedding=embeddings.get(hyde_text),
                    search=hyde_usable and hyde_text in embeddings,
                )
            )
        searched_plans = [plan for plan in plans if plan.search]
        try:
            result_sets = await asyncio.gather(
                *(
                    self._repository.search_records(
                        scope_id=scope_id,
                        query_text=plan.text,
                        query_embedding=plan.embedding,
                        limit=self._search_limit,
                        tiers=_RECALLED_TIERS,
                    )
                    for plan in searched_plans
                )
            )
        except Exception as exc:
            logger.warning("memory search failed: %s", exc)
            trace["search_error"] = exc.__class__.__name__
            query_variants = _query_variants(plans, {})
            return rewritten, hyde_text, (), False, trace, query_variants

        results_by_kind = dict(
            zip((plan.kind for plan in searched_plans), result_sets, strict=True)
        )
        query_variants = _query_variants(plans, results_by_kind)

        merged = _merge_hits(result_sets)
        candidates = tuple(
            hit
            for hit in sorted(merged.values(), key=lambda item: item.final_score, reverse=True)
            if hit.final_score >= self._score_threshold
        )[: self._search_limit]
        if not candidates:
            return rewritten, hyde_text, (), False, trace, query_variants

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
        return rewritten, hyde_text, candidates, sufficient, trace, query_variants


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


def _query_variants(
    plans: list[_SearchQuery],
    results_by_kind: dict[MemoryQueryKind, list[MemorySearchHit]],
) -> tuple[MemoryQueryVariant, ...]:
    return tuple(
        MemoryQueryVariant(
            kind=plan.kind,
            text=plan.text,
            semantic=plan.search and plan.embedding is not None,
            lexical=plan.search,
            hit_record_ids=tuple(hit.record_id for hit in results_by_kind.get(plan.kind, ())),
        )
        for plan in plans
    )


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
