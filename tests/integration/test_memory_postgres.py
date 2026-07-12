from __future__ import annotations

import asyncio
import os
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("JASI_TEST_DATABASE_URL"),
        reason="set JASI_TEST_DATABASE_URL to run PostgreSQL integration tests",
    ),
]


@pytest.mark.asyncio
async def test_alembic_upgrades_empty_database_with_memory_extensions() -> None:
    from alembic import command
    from alembic.config import Config
    from sqlalchemy import text
    from sqlalchemy.engine import make_url
    from sqlalchemy.ext.asyncio import create_async_engine

    source_url = make_url(os.environ["JASI_TEST_DATABASE_URL"])
    database_name = f"jasi_migration_{uuid.uuid4().hex}"
    admin_url = source_url.set(database="postgres")
    target_url = source_url.set(database=database_name)
    admin_engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
    target_engine = None
    previous_database_url = os.environ.get("JASI_DATABASE_URL")
    try:
        async with admin_engine.connect() as connection:
            await connection.execute(text(f'CREATE DATABASE "{database_name}"'))

        os.environ["JASI_DATABASE_URL"] = target_url.render_as_string(hide_password=False)
        await asyncio.to_thread(command.upgrade, Config("alembic.ini"), "head")

        target_engine = create_async_engine(target_url)
        async with target_engine.connect() as connection:
            assert (
                await connection.scalar(text("SELECT version_num FROM alembic_version"))
                == "0010_memory_job_batching"
            )
            extensions = set(
                (await connection.execute(text("SELECT extname FROM pg_extension"))).scalars()
            )
            assert {"vector", "pg_trgm"} <= extensions
            assert await connection.scalar(text("SELECT to_regclass('memory_records')"))
            assert await connection.scalar(text("SELECT to_regclass('memory_jobs')"))
    finally:
        if previous_database_url is None:
            os.environ.pop("JASI_DATABASE_URL", None)
        else:
            os.environ["JASI_DATABASE_URL"] = previous_database_url
        if target_engine is not None:
            await target_engine.dispose()
        async with admin_engine.connect() as connection:
            await connection.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = :database_name AND pid <> pg_backend_pid()"
                ),
                {"database_name": database_name},
            )
            await connection.execute(text(f'DROP DATABASE IF EXISTS "{database_name}"'))
        await admin_engine.dispose()


@pytest.mark.asyncio
async def test_memory_scope_index_evidence_search_and_retrieval_audit(tmp_path: Path) -> None:
    from alembic import command
    from alembic.config import Config
    from sqlalchemy import func, select

    from jasi.adapters.persistence.markdown.memory_store import MarkdownMemoryStore
    from jasi.adapters.persistence.postgres.db import (
        Conversation,
        Message,
        Turn,
        create_engine,
        create_session_factory,
    )
    from jasi.adapters.persistence.postgres.memory_repository import (
        SQLAlchemyMemoryRepository,
    )
    from jasi.adapters.persistence.postgres.memory_tables import (
        EMBEDDING_DIMENSIONS,
        MemoryEvidence,
        MemoryRecord,
        MemoryRetrieval,
        MemoryRetrievalHit,
    )
    from jasi.domain.memory import (
        MemoryRecordDraft,
        MemoryRetrievalAudit,
        content_hash,
    )

    database_url = os.environ["JASI_TEST_DATABASE_URL"]
    os.environ["JASI_DATABASE_URL"] = database_url
    await asyncio.to_thread(command.upgrade, Config("alembic.ini"), "head")

    engine = create_engine(database_url)
    session_factory = create_session_factory(engine)
    repository = SQLAlchemyMemoryRepository(session_factory)
    suffix = uuid.uuid4().hex
    try:
        async with session_factory.begin() as session:
            telegram = Conversation(
                channel="telegram",
                external_chat_id=f"memory-telegram-{suffix}",
                meta={"memory_scope_key": f"owner-{suffix}"},
            )
            feishu = Conversation(
                channel="feishu",
                external_chat_id=f"memory-feishu-{suffix}",
                meta={"memory_scope_key": f"owner-{suffix}"},
            )
            isolated = Conversation(
                channel="telegram",
                external_chat_id=f"memory-isolated-{suffix}",
                meta={"memory_scope_key": f"other-{suffix}"},
            )
            session.add_all((telegram, feishu, isolated))
            await session.flush()
            user_message = Message(
                conversation_id=telegram.id,
                role="user",
                origin="telegram",
                sequence=1,
                content="I prefer PostgreSQL for durable systems.",
                delivery_status="sent",
            )
            session.add(user_message)
            await session.flush()
            assistant_message = Message(
                conversation_id=telegram.id,
                role="assistant",
                origin="model",
                sequence=2,
                content="An assistant assertion is not user evidence.",
                delivery_status="sent",
            )
            session.add(assistant_message)
            await session.flush()
            turn = Turn(
                conversation_id=telegram.id,
                profile="passive",
                model="test-model",
                status="succeeded",
                final_text="Noted.",
            )
            session.add(turn)
            await session.flush()
            telegram_id = telegram.id
            feishu_id = feishu.id
            isolated_id = isolated.id
            user_message_id = user_message.id
            assistant_message_id = assistant_message.id
            turn_id = turn.id

        telegram_scope = await repository.resolve_scope(telegram_id)
        feishu_scope = await repository.resolve_scope(feishu_id)
        isolated_scope = await repository.resolve_scope(isolated_id)
        assert telegram_scope.id == feishu_scope.id
        assert telegram_scope.id != isolated_scope.id

        store = MarkdownMemoryStore(tmp_path)
        workspace = store.ensure_workspace(telegram_scope.directory_name)
        original = workspace.document("MEMORY.md")
        updated = store.write_document(
            telegram_scope.directory_name,
            "MEMORY.md",
            "# Long-term Memory\n\n- The user prefers PostgreSQL for durable systems.",
            expected_hash=original.content_hash,
        )
        first_embedding = (1.0,) + (0.0,) * (EMBEDDING_DIMENSIONS - 1)
        state = await repository.replace_document_index(
            telegram_scope,
            updated,
            (
                MemoryRecordDraft(
                    record_key=f"stable-{suffix}",
                    tier="stable",
                    ordinal=0,
                    heading="Long-term Memory",
                    content="The user prefers PostgreSQL for durable systems.",
                    content_hash=content_hash("The user prefers PostgreSQL for durable systems."),
                    tags=("database", "preference"),
                    evidence_message_ids=(user_message_id,),
                    embedding=first_embedding,
                    embedding_model="test-embedding",
                ),
            ),
        )
        assert state.content_hash == updated.content_hash
        assert state.indexed_hash == updated.content_hash
        assert state.version == 1

        with pytest.raises(ValueError, match="user messages in the same scope"):
            await repository.replace_document_index(
                telegram_scope,
                updated,
                (
                    MemoryRecordDraft(
                        record_key=f"invalid-{suffix}",
                        tier="stable",
                        ordinal=0,
                        heading="Long-term Memory",
                        content="The assistant asserted this.",
                        content_hash=content_hash("The assistant asserted this."),
                        evidence_message_ids=(assistant_message_id,),
                        embedding=first_embedding,
                        embedding_model="test-embedding",
                    ),
                ),
            )

        hits = await repository.search_records(
            scope_id=telegram_scope.id,
            query_text="PostgreSQL database preference",
            query_embedding=first_embedding,
            limit=5,
        )
        assert [hit.record_key for hit in hits] == [f"stable-{suffix}"]
        assert hits[0].semantic_score > 0.99
        assert (
            await repository.search_records(
                scope_id=telegram_scope.id,
                query_text="PostgreSQL database preference",
                query_embedding=first_embedding,
                limit=5,
                tiers=frozenset({"episodic"}),
            )
            == []
        )
        assert (
            await repository.search_records(
                scope_id=isolated_scope.id,
                query_text="PostgreSQL",
                query_embedding=first_embedding,
                limit=5,
            )
            == []
        )

        injected = replace(hits[0], rerank_score=0.9, final_score=0.95, injected=True)
        await repository.record_retrieval(
            MemoryRetrievalAudit(
                turn_id=turn_id,
                scope_id=telegram_scope.id,
                query="What database do I prefer?",
                rewritten_query="user database preference",
                hyde_text="The user prefers a durable relational database.",
                gate_decision="retrieve",
                sufficient=True,
                trace={"stages": ["rewrite", "hyde", "search", "rerank"]},
                hits=(injected,),
            )
        )

        async with session_factory() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(MemoryEvidence)
                    .join(MemoryRecord, MemoryRecord.id == MemoryEvidence.record_id)
                    .where(MemoryRecord.scope_id == telegram_scope.id)
                )
                == 1
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(MemoryRetrieval)
                    .where(MemoryRetrieval.turn_id == turn_id)
                )
                == 1
            )
            audit_hit = (
                await session.scalars(
                    select(MemoryRetrievalHit)
                    .join(
                        MemoryRetrieval,
                        MemoryRetrieval.id == MemoryRetrievalHit.retrieval_id,
                    )
                    .where(
                        MemoryRetrieval.turn_id == turn_id,
                        MemoryRetrievalHit.injected,
                    )
                )
            ).one()
            assert audit_hit.content_snapshot == injected.content
            indexed = (
                await session.scalars(
                    select(MemoryRecord).where(MemoryRecord.scope_id == telegram_scope.id)
                )
            ).one()
            assert indexed.status == "active"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_passive_delivery_enqueues_batched_recoverable_memory_jobs(
    tmp_path: Path,
) -> None:
    from alembic import command
    from alembic.config import Config
    from sqlalchemy import func, select

    from jasi.adapters.persistence.markdown.memory_store import MarkdownMemoryStore
    from jasi.adapters.persistence.postgres.db import (
        Conversation,
        Message,
        OutboxMessage,
        Turn,
        WorkItem,
        create_engine,
        create_session_factory,
    )
    from jasi.adapters.persistence.postgres.memory_repository import (
        SQLAlchemyMemoryRepository,
    )
    from jasi.adapters.persistence.postgres.memory_tables import (
        EMBEDDING_DIMENSIONS,
        MemoryCheckpoint,
        MemoryEvidence,
        MemoryJob,
        MemoryRecord,
    )
    from jasi.adapters.persistence.postgres.repository import SQLAlchemyRepository
    from jasi.application.memory.indexing import MarkdownMemoryIndexer
    from jasi.application.memory.maintenance import MemoryConsolidator
    from jasi.domain.memory import MemoryCandidate, StableMemoryDecision

    database_url = os.environ["JASI_TEST_DATABASE_URL"]
    os.environ["JASI_DATABASE_URL"] = database_url
    await asyncio.to_thread(command.upgrade, Config("alembic.ini"), "head")

    engine = create_engine(database_url)
    session_factory = create_session_factory(engine)
    outbox_repository = SQLAlchemyRepository(session_factory)
    suffix = uuid.uuid4().hex
    try:
        async with session_factory.begin() as session:
            conversation = Conversation(
                channel="telegram",
                external_chat_id=f"memory-jobs-{suffix}",
                meta={"memory_scope_key": f"jobs-owner-{suffix}"},
            )
            session.add(conversation)
            await session.flush()

            async def add_assistant(
                *,
                sequence: int,
                kind: str,
                status: str = "pending",
            ) -> int:
                turn = Turn(
                    conversation_id=conversation.id,
                    profile=kind,
                    model="test-model",
                    status="succeeded",
                    final_text=f"{kind} assistant {sequence}",
                )
                session.add(turn)
                await session.flush()
                message = Message(
                    conversation_id=conversation.id,
                    role="assistant",
                    origin="model",
                    sequence=sequence,
                    content=f"{kind} assistant {sequence}",
                    delivery_status=status,
                    turn_id=turn.id,
                )
                session.add(message)
                await session.flush()
                work = WorkItem(
                    kind=kind,
                    action="agent",
                    dedupe_key=f"memory-job:{suffix}:{sequence}",
                    session_id=f"telegram:memory-jobs-{suffix}",
                    conversation_id=conversation.id,
                    profile=kind,
                    input_text="input",
                    status="succeeded",
                    output_message_id=message.id,
                )
                session.add(work)
                await session.flush()
                outbox = OutboxMessage(
                    conversation_id=conversation.id,
                    message_id=message.id,
                    channel="telegram",
                    external_chat_id=conversation.external_chat_id,
                    segment_index=0,
                    segment_count=1,
                    text=message.content,
                    status="delivering" if status == "pending" else "sent",
                )
                session.add(outbox)
                await session.flush()
                return outbox.id

            passive_outbox_ids: list[int] = []
            for sequence in (1, 4, 6):
                session.add(
                    Message(
                        conversation_id=conversation.id,
                        role="user",
                        origin="telegram",
                        sequence=sequence,
                        content=f"user message {sequence}",
                        delivery_status="sent",
                    )
                )
                passive_outbox_ids.append(
                    await add_assistant(sequence=sequence + 1, kind="passive")
                )
            proactive_outbox_id = await add_assistant(
                sequence=3,
                kind="proactive",
            )
            await add_assistant(sequence=8, kind="passive", status="failed")
            conversation_id = conversation.id

        for outbox_id in passive_outbox_ids:
            await outbox_repository.mark_outbox_sent(outbox_id, f"sent-{outbox_id}")
        await outbox_repository.mark_outbox_sent(proactive_outbox_id, "sent-proactive")
        await outbox_repository.mark_outbox_sent(passive_outbox_ids[0], "sent-again")

        memory = SQLAlchemyMemoryRepository(session_factory)
        now = datetime.now(UTC)
        first, second = await asyncio.gather(
            memory.claim_jobs(
                kind="consolidate",
                limit=1,
                lease_seconds=60,
                consolidation_batch_messages=6,
                now=now,
            ),
            memory.claim_jobs(
                kind="consolidate",
                limit=1,
                lease_seconds=60,
                consolidation_batch_messages=6,
                now=now,
            ),
        )
        claimed = first or second
        assert len(claimed) == 3
        assert (first == []) != (second == [])
        assert len({job.lease_token for job in claimed}) == 1

        batch = await memory.load_consolidation_batch(
            tuple(claimed),
            history_keep_count=30,
        )
        assert batch.conversation_id == conversation_id
        assert [message.sequence for message in batch.messages] == [1, 2, 3, 4, 5, 6, 7]
        assert (
            next(message for message in batch.messages if message.sequence == 3).origin == "model"
        )
        assert batch.through_sequence == 7

        stable_key = f"stable-{suffix}"
        episodic_key = f"episodic-{suffix}"

        class FakeEmbedding:
            model_name = "test-embedding"

            async def embed(self, texts: tuple[str, ...]) -> tuple[tuple[float, ...], ...]:
                vector = (1.0,) + (0.0,) * (EMBEDDING_DIMENSIONS - 1)
                return tuple(vector for _ in texts)

        class FakeReasoner:
            async def extract(self, messages):
                user_ids = [message.id for message in messages if message.role == "user"]
                return (
                    MemoryCandidate(
                        record_key=stable_key,
                        tier="stable",
                        content="The user prefers concise durable systems.",
                        tags=("preference",),
                        evidence_message_ids=(user_ids[0],),
                    ),
                    MemoryCandidate(
                        record_key=episodic_key,
                        tier="episodic",
                        content="The user tested the passive memory pipeline.",
                        tags=("project",),
                        evidence_message_ids=(user_ids[-1],),
                    ),
                )

            async def reconcile_stable(self, _existing, candidates):
                return tuple(
                    StableMemoryDecision(candidate_index=index, action="add")
                    for index in range(len(candidates))
                )

            async def summarize(self, _existing, _messages):
                raise AssertionError("summary should not run inside the raw history window")

        store = MarkdownMemoryStore(tmp_path)
        indexer = MarkdownMemoryIndexer(
            store=store,
            repository=memory,
            embedding=FakeEmbedding(),
        )
        consolidator = MemoryConsolidator(
            store=store,
            repository=memory,
            indexer=indexer,
            reasoner=FakeReasoner(),
        )
        await consolidator.run(tuple(claimed))

        workspace = store.read_workspace(batch.scope.directory_name)
        assert stable_key in workspace.document("MEMORY.md").content
        assert episodic_key in workspace.document("HISTORY.md").content
        assert stable_key not in workspace.document("PENDING.md").content

        restarted = SQLAlchemyMemoryRepository(session_factory)
        assert (
            await restarted.claim_jobs(
                kind="consolidate",
                limit=1,
                lease_seconds=60,
                consolidation_batch_messages=6,
                now=datetime.now(UTC),
            )
            == []
        )
        async with session_factory() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(MemoryJob)
                    .where(
                        MemoryJob.conversation_id == conversation_id,
                        MemoryJob.status == "succeeded",
                    )
                )
                == 3
            )
            checkpoint = (
                await session.scalars(
                    select(MemoryCheckpoint).where(
                        MemoryCheckpoint.conversation_id == conversation_id
                    )
                )
            ).one()
            assert checkpoint.consolidated_through_sequence == 7
            records = list(
                (
                    await session.scalars(
                        select(MemoryRecord)
                        .where(
                            MemoryRecord.scope_id == batch.scope.id,
                            MemoryRecord.status == "active",
                        )
                        .order_by(MemoryRecord.record_key)
                    )
                ).all()
            )
            assert {record.record_key for record in records} == {stable_key, episodic_key}
            assert all(record.embedding is not None for record in records)
            evidence = list(
                (
                    await session.scalars(
                        select(MemoryEvidence).where(
                            MemoryEvidence.record_id.in_([record.id for record in records])
                        )
                    )
                ).all()
            )
            assert {item.message_id for item in evidence} == {
                message.id for message in batch.messages if message.role == "user"
            } - {next(message.id for message in batch.messages if message.sequence == 4)}
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_memory_worker_reindexes_manual_edits_and_recovers_stale_lease(
    tmp_path: Path,
) -> None:
    from alembic import command
    from alembic.config import Config
    from sqlalchemy import delete, select, update

    from jasi.adapters.persistence.markdown.memory_store import MarkdownMemoryStore
    from jasi.adapters.persistence.postgres.db import (
        Conversation,
        create_engine,
        create_session_factory,
    )
    from jasi.adapters.persistence.postgres.memory_repository import (
        SQLAlchemyMemoryRepository,
    )
    from jasi.adapters.persistence.postgres.memory_tables import (
        EMBEDDING_DIMENSIONS,
        MemoryJob,
        MemoryRecord,
    )
    from jasi.application.memory.indexing import MarkdownMemoryIndexer
    from jasi.application.memory.maintenance import MemoryWorker

    database_url = os.environ["JASI_TEST_DATABASE_URL"]
    os.environ["JASI_DATABASE_URL"] = database_url
    await asyncio.to_thread(command.upgrade, Config("alembic.ini"), "head")

    engine = create_engine(database_url)
    session_factory = create_session_factory(engine)
    repository = SQLAlchemyMemoryRepository(session_factory)
    suffix = uuid.uuid4().hex

    class FakeEmbedding:
        model_name = "test-embedding"

        async def embed(self, texts: tuple[str, ...]) -> tuple[tuple[float, ...], ...]:
            vector = (1.0,) + (0.0,) * (EMBEDDING_DIMENSIONS - 1)
            return tuple(vector for _ in texts)

    class UnexpectedConsolidator:
        async def run(self, _jobs):
            raise AssertionError("manual reindex must not run consolidation")

    try:
        async with session_factory.begin() as session:
            await session.execute(delete(MemoryJob).where(MemoryJob.kind == "reindex"))
            conversation = Conversation(
                channel="telegram",
                external_chat_id=f"memory-reindex-{suffix}",
                meta={"memory_scope_key": f"reindex-owner-{suffix}"},
            )
            session.add(conversation)
            await session.flush()
            conversation_id = conversation.id

        scope = await repository.resolve_scope(conversation_id)
        store = MarkdownMemoryStore(tmp_path)
        workspace = store.ensure_workspace(scope.directory_name)
        document = workspace.document("MEMORY.md")
        first_text = "The user manually recorded a preference for PostgreSQL."
        store.write_document(
            scope.directory_name,
            "MEMORY.md",
            f"# Long-term Memory\n\n- {first_text}",
            expected_hash=document.content_hash,
        )
        indexer = MarkdownMemoryIndexer(
            store=store,
            repository=repository,
            embedding=FakeEmbedding(),
        )

        class ScopedIndexRepository:
            async def list_scopes(self):
                return [scope]

            async def load_document_states(self, scope_id):
                return await repository.load_document_states(scope_id)

        worker = MemoryWorker(
            repository=repository,
            index_repository=ScopedIndexRepository(),
            store=store,
            indexer=indexer,
            consolidator=UnexpectedConsolidator(),
            wakeup=asyncio.Event(),
            batch_size=2,
            consolidation_batch_messages=6,
            lease_seconds=30,
            reconcile_seconds=30,
        )

        assert await worker.reconcile_once() == 1
        assert await worker.drain_once() == 1

        restarted = MemoryWorker(
            repository=repository,
            index_repository=ScopedIndexRepository(),
            store=store,
            indexer=indexer,
            consolidator=UnexpectedConsolidator(),
            wakeup=asyncio.Event(),
            batch_size=2,
            consolidation_batch_messages=6,
            lease_seconds=30,
            reconcile_seconds=30,
        )
        workspace = store.read_workspace(scope.directory_name)
        document = workspace.document("MEMORY.md")
        second_text = "The user manually changed the preference to SQLite."
        store.write_document(
            scope.directory_name,
            "MEMORY.md",
            f"# Long-term Memory\n\n- {second_text}",
            expected_hash=document.content_hash,
        )
        assert await restarted.reconcile_once() == 1

        claimed_at = datetime.now(UTC)
        abandoned = await repository.claim_jobs(
            kind="reindex",
            limit=1,
            lease_seconds=1,
            consolidation_batch_messages=6,
            now=claimed_at,
        )
        assert len(abandoned) == 1
        recovered = await repository.claim_jobs(
            kind="reindex",
            limit=1,
            lease_seconds=30,
            consolidation_batch_messages=6,
            now=claimed_at + timedelta(seconds=2),
        )
        assert len(recovered) == 1
        assert recovered[0].id == abandoned[0].id
        assert recovered[0].lease_token != abandoned[0].lease_token
        assert recovered[0].attempts == 2

        await indexer.sync_scope(scope)
        await repository.complete_jobs(tuple(recovered))

        async with session_factory() as session:
            records = list(
                (
                    await session.scalars(
                        select(MemoryRecord)
                        .where(MemoryRecord.scope_id == scope.id)
                        .order_by(MemoryRecord.id)
                    )
                ).all()
            )
            assert {record.content for record in records if record.status == "active"} == {
                second_text
            }
            assert {record.content for record in records if record.status == "stale"} == {
                first_text
            }
            job = await session.get(MemoryJob, recovered[0].id)
            assert job is not None
            assert job.status == "succeeded"
            assert job.attempts == 2

        workspace = store.read_workspace(scope.directory_name)
        document = workspace.document("MEMORY.md")
        store.write_document(
            scope.directory_name,
            "MEMORY.md",
            "# Long-term Memory\n\n- A final edit used to exercise lease exhaustion.",
            expected_hash=document.content_hash,
        )
        assert await restarted.reconcile_once() == 1
        exhausted_at = datetime.now(UTC)
        exhausted = await repository.claim_jobs(
            kind="reindex",
            limit=1,
            lease_seconds=1,
            consolidation_batch_messages=6,
            now=exhausted_at,
        )
        assert len(exhausted) == 1
        async with session_factory.begin() as session:
            await session.execute(
                update(MemoryJob)
                .where(MemoryJob.id == exhausted[0].id)
                .values(
                    attempts=5,
                    lease_until=exhausted_at - timedelta(seconds=1),
                )
            )
        assert (
            await repository.claim_jobs(
                kind="reindex",
                limit=1,
                lease_seconds=30,
                consolidation_batch_messages=6,
                now=exhausted_at,
            )
            == []
        )
        async with session_factory() as session:
            failed_job = await session.get(MemoryJob, exhausted[0].id)
            assert failed_job is not None
            assert failed_job.status == "failed"
            assert failed_job.lease_token is None
    finally:
        await engine.dispose()
