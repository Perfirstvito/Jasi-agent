from __future__ import annotations

from datetime import UTC, datetime, timedelta
from math import ceil
from uuid import NAMESPACE_URL, uuid4, uuid5

from sqlalchemy import and_, delete, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jasi.adapters.persistence.postgres.db import Conversation, Message
from jasi.adapters.persistence.postgres.memory_tables import (
    EMBEDDING_DIMENSIONS,
    ConversationMemoryScope,
    MemoryCheckpoint,
    MemoryDocument,
    MemoryEvidence,
    MemoryJob,
    MemoryRecord,
    MemoryRetrieval,
    MemoryRetrievalHit,
    MemoryScope,
)
from jasi.domain.memory import (
    MemoryConsolidationBatch,
    MemoryDocumentSnapshot,
    MemoryDocumentState,
    MemoryJobLeaseLost,
    MemoryJobRecord,
    MemoryMessage,
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
            return _scope_record(await ensure_conversation_memory_scope(session, conversation))

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
            await _validate_evidence(session, scope.id, records)
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

    async def claim_jobs(
        self,
        *,
        kind: str,
        limit: int,
        lease_seconds: float,
        consolidation_batch_messages: int,
        now: datetime,
    ) -> list[MemoryJobRecord]:
        if kind not in {"consolidate", "reindex"}:
            raise ValueError(f"unsupported memory job kind: {kind}")
        if limit <= 0:
            return []
        if lease_seconds <= 0:
            raise ValueError("memory job lease must be positive")
        if not 4 <= consolidation_batch_messages <= 8:
            raise ValueError("memory consolidation batch must contain 4 to 8 messages")
        required_jobs = ceil(consolidation_batch_messages / 2) if kind == "consolidate" else 1
        lease_until = now + timedelta(seconds=lease_seconds)

        async with self._session_factory.begin() as session:
            await session.execute(
                update(MemoryJob)
                .where(
                    MemoryJob.kind == kind,
                    MemoryJob.status == "running",
                    MemoryJob.lease_until <= now,
                    MemoryJob.attempts >= MemoryJob.max_attempts,
                )
                .values(
                    status="failed",
                    lease_token=None,
                    lease_until=None,
                    last_error="memory job lease expired after maximum attempts",
                    completed_at=now,
                    updated_at=now,
                )
            )
            await session.execute(
                update(MemoryJob)
                .where(
                    MemoryJob.kind == kind,
                    MemoryJob.status == "running",
                    MemoryJob.lease_until <= now,
                    MemoryJob.attempts < MemoryJob.max_attempts,
                )
                .values(
                    status="pending",
                    lease_token=None,
                    lease_until=None,
                    updated_at=now,
                )
            )
            grouped = (
                select(
                    MemoryJob.scope_id.label("scope_id"),
                    MemoryJob.conversation_id.label("conversation_id"),
                    func.min(MemoryJob.created_at).label("oldest"),
                    func.count(MemoryJob.id).label("job_count"),
                )
                .where(
                    MemoryJob.kind == kind,
                    MemoryJob.status == "pending",
                    MemoryJob.available_at <= now,
                    MemoryJob.attempts < MemoryJob.max_attempts,
                )
                .group_by(MemoryJob.scope_id, MemoryJob.conversation_id)
                .having(func.count(MemoryJob.id) >= required_jobs)
                .subquery()
            )
            pairs = (
                await session.execute(
                    select(
                        grouped.c.scope_id,
                        grouped.c.conversation_id,
                        grouped.c.oldest,
                    )
                    .order_by(grouped.c.oldest, grouped.c.scope_id)
                    .limit(limit * 3)
                )
            ).all()

            claimed: list[MemoryJobRecord] = []
            claimed_groups = 0
            for pair in pairs:
                if claimed_groups >= limit:
                    break
                scope = (
                    await session.scalars(
                        select(MemoryScope)
                        .where(MemoryScope.id == pair.scope_id)
                        .with_for_update(skip_locked=True)
                    )
                ).one_or_none()
                if scope is None:
                    continue
                running = await session.scalar(
                    select(func.count())
                    .select_from(MemoryJob)
                    .where(
                        MemoryJob.scope_id == scope.id,
                        MemoryJob.status == "running",
                    )
                )
                if running:
                    continue
                filters = [
                    MemoryJob.scope_id == scope.id,
                    MemoryJob.kind == kind,
                    MemoryJob.status == "pending",
                    MemoryJob.available_at <= now,
                    MemoryJob.attempts < MemoryJob.max_attempts,
                ]
                if pair.conversation_id is None:
                    filters.append(MemoryJob.conversation_id.is_(None))
                else:
                    filters.append(MemoryJob.conversation_id == pair.conversation_id)
                rows = list(
                    (
                        await session.scalars(
                            select(MemoryJob)
                            .where(*filters)
                            .order_by(MemoryJob.created_at, MemoryJob.id)
                            .limit(required_jobs)
                            .with_for_update(skip_locked=True)
                        )
                    ).all()
                )
                if len(rows) < required_jobs:
                    continue
                lease_token = uuid4().hex
                for row in rows:
                    row.status = "running"
                    row.lease_token = lease_token
                    row.lease_until = lease_until
                    row.attempts += 1
                    row.updated_at = now
                    claimed.append(_job_record(row, scope.directory_name))
                claimed_groups += 1
            await session.flush()
            return claimed

    async def load_consolidation_batch(
        self,
        jobs: tuple[MemoryJobRecord, ...],
        *,
        history_keep_count: int,
    ) -> MemoryConsolidationBatch:
        if not jobs or history_keep_count <= 0:
            raise ValueError("consolidation batch and history keep count are required")
        scope_id = jobs[0].scope_id
        conversation_id = jobs[0].conversation_id
        lease_token = jobs[0].lease_token
        if conversation_id is None or lease_token is None:
            raise ValueError("consolidation jobs require a conversation and lease")
        if any(
            job.scope_id != scope_id
            or job.conversation_id != conversation_id
            or job.lease_token != lease_token
            for job in jobs
        ):
            raise ValueError("consolidation jobs must share one scope, conversation, and lease")

        async with self._session_factory.begin() as session:
            await _verify_job_rows(session, jobs)
            scope = await session.get(MemoryScope, scope_id)
            if scope is None:
                raise RuntimeError("memory job references a missing scope")
            await session.execute(
                pg_insert(MemoryCheckpoint)
                .values(scope_id=scope_id, conversation_id=conversation_id)
                .on_conflict_do_nothing(
                    index_elements=[
                        MemoryCheckpoint.scope_id,
                        MemoryCheckpoint.conversation_id,
                    ]
                )
            )
            checkpoint = await session.get(
                MemoryCheckpoint,
                {"scope_id": scope_id, "conversation_id": conversation_id},
            )
            if checkpoint is None:
                raise RuntimeError("memory checkpoint disappeared after upsert")
            trigger_ids = [
                job.trigger_message_id for job in jobs if job.trigger_message_id is not None
            ]
            through_sequence = await session.scalar(
                select(func.max(Message.sequence)).where(
                    Message.conversation_id == conversation_id,
                    Message.id.in_(trigger_ids),
                )
            )
            if through_sequence is None:
                raise RuntimeError("memory jobs reference missing trigger messages")
            eligible = _eligible_messages(conversation_id, int(through_sequence))
            message_rows = list(
                (
                    await session.scalars(
                        select(Message)
                        .where(
                            *eligible,
                            Message.sequence > checkpoint.consolidated_through_sequence,
                        )
                        .order_by(Message.sequence)
                    )
                ).all()
            )

            recent_sequences = list(
                (
                    await session.scalars(
                        select(Message.sequence)
                        .where(*eligible)
                        .order_by(Message.sequence.desc())
                        .limit(history_keep_count)
                    )
                ).all()
            )
            summary_through = None
            summary_rows: list[Message] = []
            if len(recent_sequences) == history_keep_count:
                summary_cutoff = min(recent_sequences) - 1
                if summary_cutoff > checkpoint.summarized_through_sequence:
                    summary_through = summary_cutoff
                    summary_rows = list(
                        (
                            await session.scalars(
                                select(Message)
                                .where(
                                    *_eligible_messages(conversation_id, summary_cutoff),
                                    Message.sequence > checkpoint.summarized_through_sequence,
                                )
                                .order_by(Message.sequence)
                            )
                        ).all()
                    )

            return MemoryConsolidationBatch(
                scope=_scope_record(scope),
                conversation_id=conversation_id,
                jobs=jobs,
                messages=tuple(_memory_message(row) for row in message_rows),
                through_sequence=int(through_sequence),
                summary_messages=tuple(_memory_message(row) for row in summary_rows),
                summary_through_sequence=summary_through,
            )

    async def commit_consolidation(self, batch: MemoryConsolidationBatch) -> None:
        async with self._session_factory.begin() as session:
            await _verify_job_rows(session, batch.jobs)
            now = datetime.now(UTC)
            await session.execute(
                pg_insert(MemoryCheckpoint)
                .values(
                    scope_id=batch.scope.id,
                    conversation_id=batch.conversation_id,
                    consolidated_through_sequence=batch.through_sequence,
                    summarized_through_sequence=batch.summary_through_sequence or 0,
                    updated_at=now,
                )
                .on_conflict_do_update(
                    index_elements=[
                        MemoryCheckpoint.scope_id,
                        MemoryCheckpoint.conversation_id,
                    ],
                    set_={
                        "consolidated_through_sequence": func.greatest(
                            MemoryCheckpoint.consolidated_through_sequence,
                            batch.through_sequence,
                        ),
                        "summarized_through_sequence": func.greatest(
                            MemoryCheckpoint.summarized_through_sequence,
                            batch.summary_through_sequence or 0,
                        ),
                        "updated_at": now,
                    },
                )
            )
            for job in batch.jobs:
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


async def ensure_conversation_memory_scope(
    session: AsyncSession,
    conversation: Conversation,
) -> MemoryScope:
    mapping = await session.get(ConversationMemoryScope, conversation.id)
    if mapping is not None:
        scope = await session.get(MemoryScope, mapping.scope_id)
        if scope is None:
            raise RuntimeError("conversation references a missing memory scope")
        return scope

    scope_key = _scope_key(conversation)
    directory_name = uuid5(NAMESPACE_URL, f"jasi-memory:{scope_key}").hex
    scope_id = (
        await session.execute(
            pg_insert(MemoryScope)
            .values(scope_key=scope_key, directory_name=directory_name)
            .on_conflict_do_nothing(index_elements=[MemoryScope.scope_key])
            .returning(MemoryScope.id)
        )
    ).scalar_one_or_none()
    if scope_id is None:
        scope_id = await session.scalar(
            select(MemoryScope.id).where(MemoryScope.scope_key == scope_key)
        )
    if scope_id is None:
        raise RuntimeError("memory scope disappeared after upsert")
    await session.execute(
        pg_insert(ConversationMemoryScope)
        .values(conversation_id=conversation.id, scope_id=scope_id)
        .on_conflict_do_nothing(index_elements=[ConversationMemoryScope.conversation_id])
    )
    mapping = await session.get(ConversationMemoryScope, conversation.id)
    if mapping is None:
        raise RuntimeError("memory scope mapping disappeared after upsert")
    scope = await session.get(MemoryScope, mapping.scope_id)
    if scope is None:
        raise RuntimeError("memory scope disappeared after mapping")
    return scope


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


def _job_record(row: MemoryJob, scope_directory: str) -> MemoryJobRecord:
    return MemoryJobRecord(
        id=row.id,
        scope_id=row.scope_id,
        scope_directory=scope_directory,
        conversation_id=row.conversation_id,
        kind=row.kind,
        trigger_message_id=row.trigger_message_id,
        status=row.status,
        attempts=row.attempts,
        lease_token=row.lease_token,
        payload=dict(row.payload or {}),
    )


def _memory_message(row: Message) -> MemoryMessage:
    return MemoryMessage(
        id=row.id,
        conversation_id=row.conversation_id,
        sequence=row.sequence,
        role=row.role,
        origin=row.origin,
        content=row.content,
        created_at=row.created_at,
    )


def _eligible_messages(conversation_id: int, through_sequence: int) -> tuple:
    return (
        Message.conversation_id == conversation_id,
        Message.sequence <= through_sequence,
        or_(
            and_(Message.role == "user", Message.delivery_status == "sent"),
            and_(
                Message.role == "assistant",
                Message.delivery_status == "sent",
                Message.origin != "system_error",
            ),
        ),
    )


async def _verify_job_rows(
    session: AsyncSession,
    jobs: tuple[MemoryJobRecord, ...],
) -> None:
    rows = list(
        (
            await session.scalars(
                select(MemoryJob)
                .where(MemoryJob.id.in_([job.id for job in jobs]))
                .with_for_update()
            )
        ).all()
    )
    by_id = {row.id: row for row in rows}
    for job in jobs:
        row = by_id.get(job.id)
        if (
            row is None
            or row.status != "running"
            or not job.lease_token
            or row.lease_token != job.lease_token
        ):
            raise MemoryJobLeaseLost(f"memory job lease is no longer owned: {job.id}")


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


async def _validate_evidence(
    session: AsyncSession,
    scope_id: int,
    records: tuple[MemoryRecordDraft, ...],
) -> None:
    evidence_ids = {message_id for record in records for message_id in record.evidence_message_ids}
    if not evidence_ids:
        return
    valid_ids = set(
        (
            await session.scalars(
                select(Message.id)
                .join(
                    ConversationMemoryScope,
                    ConversationMemoryScope.conversation_id == Message.conversation_id,
                )
                .where(
                    Message.id.in_(evidence_ids),
                    Message.role == "user",
                    ConversationMemoryScope.scope_id == scope_id,
                )
            )
        ).all()
    )
    if valid_ids != evidence_ids:
        raise ValueError("memory evidence must reference user messages in the same scope")
