from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jasi.adapters.persistence.postgres.db import Conversation
from jasi.adapters.persistence.postgres.memory_tables import (
    EMBEDDING_DIMENSIONS,
    ConversationMemoryScope,
    MemoryDocument,
    MemoryEvidence,
    MemoryJob,
    MemoryRecord,
    MemoryRetrieval,
    MemoryRetrievalHit,
    MemoryScope,
)
from jasi.domain.memory import (
    MemoryDocumentSnapshot,
    MemoryDocumentState,
    MemoryJobLeaseLost,
    MemoryJobRecord,
    MemoryRecordDraft,
    MemoryRetrievalAudit,
    MemoryScopeRecord,
    MemorySearchHit,
)

MEMORY_JOB_BACKOFF_SECONDS = (2, 10, 30, 120, 300)


class SQLAlchemyMemoryRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def resolve_scope(self, conversation_id: int) -> MemoryScopeRecord:
        async with self._session_factory.begin() as session:
            mapping = await session.get(ConversationMemoryScope, conversation_id)
            if mapping is not None:
                row = await session.get(MemoryScope, mapping.scope_id)
                if row is None:
                    raise RuntimeError("conversation references a missing memory scope")
                return _scope_record(row)

            conversation = await session.get(Conversation, conversation_id)
            if conversation is None:
                raise KeyError(f"conversation not found: {conversation_id}")
            scope_key = _scope_key(conversation)
            directory_name = uuid5(NAMESPACE_URL, f"jasi-memory:{scope_key}").hex
            statement = (
                pg_insert(MemoryScope)
                .values(scope_key=scope_key, directory_name=directory_name)
                .on_conflict_do_nothing(index_elements=[MemoryScope.scope_key])
                .returning(MemoryScope.id)
            )
            scope_id = (await session.execute(statement)).scalar_one_or_none()
            if scope_id is None:
                scope_id = await session.scalar(
                    select(MemoryScope.id).where(MemoryScope.scope_key == scope_key)
                )
            if scope_id is None:
                raise RuntimeError("memory scope disappeared after upsert")
            await session.execute(
                pg_insert(ConversationMemoryScope)
                .values(conversation_id=conversation_id, scope_id=scope_id)
                .on_conflict_do_nothing(index_elements=[ConversationMemoryScope.conversation_id])
            )
            mapping = await session.get(ConversationMemoryScope, conversation_id)
            if mapping is None:
                raise RuntimeError("memory scope mapping disappeared after upsert")
            row = await session.get(MemoryScope, mapping.scope_id)
            if row is None:
                raise RuntimeError("memory scope disappeared after mapping")
            return _scope_record(row)

    async def list_scopes(self) -> list[MemoryScopeRecord]:
        async with self._session_factory() as session:
            rows = list((await session.scalars(select(MemoryScope).order_by(MemoryScope.id))).all())
            return [_scope_record(row) for row in rows]

    async def load_document_states(self, scope_id: int) -> list[MemoryDocumentState]:
        async with self._session_factory() as session:
            rows = list(
                (
                    await session.scalars(
                        select(MemoryDocument)
                        .where(MemoryDocument.scope_id == scope_id)
                        .order_by(MemoryDocument.name)
                    )
                ).all()
            )
            return [_document_state(row) for row in rows]

    async def replace_document_index(
        self,
        scope: MemoryScopeRecord,
        document: MemoryDocumentSnapshot,
        records: tuple[MemoryRecordDraft, ...],
    ) -> MemoryDocumentState:
        _validate_records(records)
        async with self._session_factory.begin() as session:
            await session.execute(
                pg_insert(MemoryDocument)
                .values(scope_id=scope.id, name=document.name)
                .on_conflict_do_nothing(
                    index_elements=[MemoryDocument.scope_id, MemoryDocument.name]
                )
            )
            row = (
                await session.scalars(
                    select(MemoryDocument)
                    .where(
                        MemoryDocument.scope_id == scope.id,
                        MemoryDocument.name == document.name,
                    )
                    .with_for_update()
                )
            ).one()
            now = datetime.now(UTC)
            await session.execute(
                update(MemoryRecord)
                .where(MemoryRecord.document_id == row.id)
                .values(status="stale", updated_at=now)
            )

            for draft in records:
                values = {
                    "scope_id": scope.id,
                    "document_id": row.id,
                    "record_key": draft.record_key,
                    "tier": draft.tier,
                    "status": "active",
                    "ordinal": draft.ordinal,
                    "heading": draft.heading,
                    "content": draft.content,
                    "content_hash": draft.content_hash,
                    "tags": list(draft.tags),
                    "embedding": list(draft.embedding) if draft.embedding is not None else None,
                    "embedding_model": draft.embedding_model,
                    "happened_at": draft.happened_at,
                    "updated_at": now,
                }
                statement = (
                    pg_insert(MemoryRecord)
                    .values(**values)
                    .on_conflict_do_update(
                        index_elements=[MemoryRecord.scope_id, MemoryRecord.record_key],
                        set_=values,
                    )
                    .returning(MemoryRecord.id)
                )
                record_id = int((await session.execute(statement)).scalar_one())
                await session.execute(
                    delete(MemoryEvidence).where(MemoryEvidence.record_id == record_id)
                )
                for message_id in dict.fromkeys(draft.evidence_message_ids):
                    session.add(MemoryEvidence(record_id=record_id, message_id=message_id))

            row.content_hash = document.content_hash
            row.indexed_hash = document.content_hash
            row.version += 1
            row.updated_at = now
            await session.flush()
            return _document_state(row)

    async def search_records(
        self,
        *,
        scope_id: int,
        query_text: str,
        query_embedding: tuple[float, ...] | None,
        limit: int,
    ) -> list[MemorySearchHit]:
        if limit <= 0 or not query_text.strip():
            return []
        if query_embedding is not None and len(query_embedding) != EMBEDDING_DIMENSIONS:
            raise ValueError(
                f"query embedding has {len(query_embedding)} dimensions; "
                f"expected {EMBEDDING_DIMENSIONS}"
            )

        lexical = func.similarity(MemoryRecord.content, query_text).label("lexical_score")
        if query_embedding is None:
            score = lexical
            columns = (MemoryRecord, lexical)
        else:
            distance = MemoryRecord.embedding.cosine_distance(list(query_embedding))
            semantic = func.coalesce(1.0 - distance, 0.0).label("semantic_score")
            score = (semantic * 0.75 + lexical * 0.25).label("final_score")
            columns = (MemoryRecord, semantic, lexical, score)

        async with self._session_factory() as session:
            statement = (
                select(*columns)
                .where(
                    MemoryRecord.scope_id == scope_id,
                    MemoryRecord.status == "active",
                )
                .order_by(score.desc(), MemoryRecord.id)
                .limit(limit)
            )
            rows = (await session.execute(statement)).all()

        hits: list[MemorySearchHit] = []
        for result in rows:
            record = result[0]
            if query_embedding is None:
                semantic_score = 0.0
                lexical_score = float(result[1] or 0.0)
                final_score = lexical_score
            else:
                semantic_score = float(result[1] or 0.0)
                lexical_score = float(result[2] or 0.0)
                final_score = float(result[3] or 0.0)
            hits.append(
                MemorySearchHit(
                    record_id=record.id,
                    record_key=record.record_key,
                    tier=record.tier,
                    content=record.content,
                    tags=tuple(record.tags or ()),
                    semantic_score=semantic_score,
                    lexical_score=lexical_score,
                    final_score=final_score,
                )
            )
        return hits

    async def record_retrieval(self, audit: MemoryRetrievalAudit) -> None:
        async with self._session_factory.begin() as session:
            statement = (
                pg_insert(MemoryRetrieval)
                .values(
                    turn_id=audit.turn_id,
                    scope_id=audit.scope_id,
                    query=audit.query,
                    rewritten_query=audit.rewritten_query,
                    hyde_text=audit.hyde_text,
                    gate_decision=audit.gate_decision,
                    sufficient=audit.sufficient,
                    trace=audit.trace,
                )
                .on_conflict_do_update(
                    index_elements=[MemoryRetrieval.turn_id],
                    set_={
                        "scope_id": audit.scope_id,
                        "query": audit.query,
                        "rewritten_query": audit.rewritten_query,
                        "hyde_text": audit.hyde_text,
                        "gate_decision": audit.gate_decision,
                        "sufficient": audit.sufficient,
                        "trace": audit.trace,
                    },
                )
                .returning(MemoryRetrieval.id)
            )
            retrieval_id = int((await session.execute(statement)).scalar_one())
            await session.execute(
                delete(MemoryRetrievalHit).where(MemoryRetrievalHit.retrieval_id == retrieval_id)
            )
            for rank, hit in enumerate(audit.hits, 1):
                session.add(
                    MemoryRetrievalHit(
                        retrieval_id=retrieval_id,
                        record_id=hit.record_id,
                        rank=rank,
                        semantic_score=hit.semantic_score,
                        lexical_score=hit.lexical_score,
                        rerank_score=hit.rerank_score,
                        final_score=hit.final_score,
                        injected=hit.injected,
                        content_snapshot=hit.content,
                    )
                )

    async def enqueue_reindex(
        self,
        scope_id: int,
        dedupe_key: str,
        payload: dict,
    ) -> bool:
        async with self._session_factory.begin() as session:
            job_id = (
                await session.execute(
                    pg_insert(MemoryJob)
                    .values(
                        scope_id=scope_id,
                        kind="reindex",
                        dedupe_key=dedupe_key,
                        status="pending",
                        payload=payload,
                    )
                    .on_conflict_do_nothing(index_elements=[MemoryJob.dedupe_key])
                    .returning(MemoryJob.id)
                )
            ).scalar_one_or_none()
            return job_id is not None

    async def complete_jobs(self, jobs: tuple[MemoryJobRecord, ...]) -> None:
        if not jobs:
            return
        async with self._session_factory.begin() as session:
            now = datetime.now(UTC)
            for job in jobs:
                result = await session.execute(
                    update(MemoryJob)
                    .where(
                        MemoryJob.id == job.id,
                        MemoryJob.status == "running",
                        MemoryJob.lease_token == job.lease_token,
                    )
                    .values(
                        status="succeeded",
                        lease_token=None,
                        lease_until=None,
                        completed_at=now,
                        updated_at=now,
                    )
                )
                if result.rowcount != 1:
                    raise MemoryJobLeaseLost(f"memory job lease is no longer owned: {job.id}")

    async def fail_jobs(
        self,
        jobs: tuple[MemoryJobRecord, ...],
        error: str,
        now: datetime,
    ) -> None:
        if not jobs:
            return
        safe_error = error[:500]
        async with self._session_factory.begin() as session:
            for job in jobs:
                row = (
                    await session.scalars(
                        select(MemoryJob).where(MemoryJob.id == job.id).with_for_update()
                    )
                ).one_or_none()
                if row is None or row.status != "running" or row.lease_token != job.lease_token:
                    raise MemoryJobLeaseLost(f"memory job lease is no longer owned: {job.id}")
                row.lease_token = None
                row.lease_until = None
                row.last_error = safe_error
                row.updated_at = now
                if row.attempts >= row.max_attempts:
                    row.status = "failed"
                    row.completed_at = now
                else:
                    index = min(row.attempts - 1, len(MEMORY_JOB_BACKOFF_SECONDS) - 1)
                    row.status = "pending"
                    row.available_at = now + timedelta(seconds=MEMORY_JOB_BACKOFF_SECONDS[index])


def _scope_key(conversation: Conversation) -> str:
    metadata = dict(conversation.meta or {})
    configured = str(metadata.get("memory_scope_key") or "").strip()
    if configured:
        return configured
    external_user_id = str(metadata.get("last_external_user_id") or "").strip()
    identity = external_user_id or conversation.external_chat_id
    return f"{conversation.channel}:{identity}"


def _scope_record(row: MemoryScope) -> MemoryScopeRecord:
    return MemoryScopeRecord(
        id=row.id,
        scope_key=row.scope_key,
        directory_name=row.directory_name,
    )


def _document_state(row: MemoryDocument) -> MemoryDocumentState:
    return MemoryDocumentState(
        id=row.id,
        scope_id=row.scope_id,
        name=row.name,
        content_hash=row.content_hash,
        indexed_hash=row.indexed_hash,
        version=row.version,
    )


def _validate_records(records: tuple[MemoryRecordDraft, ...]) -> None:
    seen: set[str] = set()
    for record in records:
        if not record.record_key.strip() or record.record_key in seen:
            raise ValueError(f"memory record key is empty or duplicated: {record.record_key!r}")
        seen.add(record.record_key)
        if not record.content.strip():
            raise ValueError("memory record content cannot be empty")
        if record.embedding is not None and len(record.embedding) != EMBEDDING_DIMENSIONS:
            raise ValueError(
                f"memory embedding has {len(record.embedding)} dimensions; "
                f"expected {EMBEDDING_DIMENSIONS}"
            )
