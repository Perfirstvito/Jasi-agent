from __future__ import annotations

import asyncio
import os
import uuid
from dataclasses import replace
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
            assert await session.scalar(select(func.count()).select_from(MemoryEvidence)) == 1
            assert await session.scalar(select(func.count()).select_from(MemoryRetrieval)) == 1
            audit_hit = (
                await session.scalars(select(MemoryRetrievalHit).where(MemoryRetrievalHit.injected))
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
