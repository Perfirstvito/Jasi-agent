from __future__ import annotations

import asyncio
import os
import uuid

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("JASI_TEST_DATABASE_URL"),
        reason="set JASI_TEST_DATABASE_URL to run PostgreSQL integration tests",
    ),
]


@pytest.mark.asyncio
async def test_repository_crud_idempotency_and_outbox_claim() -> None:
    from alembic import command
    from alembic.config import Config
    from sqlalchemy.exc import StatementError

    from jasi.adapters.persistence.postgres.db import (
        check_database_ready,
        create_engine,
        create_session_factory,
    )
    from jasi.adapters.persistence.postgres.repository import SQLAlchemyRepository
    from jasi.domain.models import InboundMessage, OutboundPart
    from jasi.runtime.models import ToolExecutionRecord, Usage

    database_url = os.environ["JASI_TEST_DATABASE_URL"]
    os.environ["JASI_DATABASE_URL"] = database_url

    alembic_config = Config("alembic.ini")
    await asyncio.to_thread(command.upgrade, alembic_config, "head")

    engine = create_engine(database_url)
    try:
        await check_database_ready(engine)
        repo = SQLAlchemyRepository(create_session_factory(engine))

        suffix = uuid.uuid4().hex
        inbound = InboundMessage(
            channel="feishu",
            external_update_id=f"update-{suffix}",
            external_chat_id=f"chat-{suffix}",
            external_user_id="42",
            text="hello",
        )
        claim = await repo.claim_inbound_message(inbound)
        resumed = await repo.claim_inbound_message(inbound)

        assert claim is not None
        assert resumed is not None
        assert resumed.event_id == claim.event_id
        assert resumed.message.id == claim.message.id
        assert claim.message.origin == inbound.channel

        turn_start = await repo.start_turn(
            conversation_id=claim.conversation.id,
            inbound_message_id=claim.message.id,
            profile="passive",
            model="test-model",
            metadata={},
        )
        turn_id = turn_start.turn_id
        await repo.record_tool_execution(
            turn_id,
            ToolExecutionRecord(
                name="get_current_time",
                arguments={},
                result={"iso8601": "2026-07-11T00:00:00+08:00"},
                risk="low",
                status="succeeded",
                duration_ms=1,
            ),
        )
        await repo.finish_turn(
            turn_id,
            "succeeded",
            "reply",
            2,
            Usage(total_tokens=3),
        )
        cached_turn = await repo.start_turn(
            conversation_id=claim.conversation.id,
            inbound_message_id=claim.message.id,
            profile="passive",
            model="test-model",
            metadata={},
        )

        assert cached_turn.cached_result is not None
        assert cached_turn.cached_result.final_text == "reply"
        assert len(cached_turn.cached_result.tool_records) == 1

        _, outbox = await repo.complete_inbound_response(
            inbound_event_id=claim.event_id,
            conversation_id=claim.conversation.id,
            channel=inbound.channel,
            external_chat_id=inbound.external_chat_id,
            turn_id=turn_id,
            text="reply",
            parts=(OutboundPart(text="reply"),),
            origin="model",
            metadata={},
        )
        duplicate = await repo.claim_inbound_message(inbound)

        assert duplicate is None

        first_claim, second_claim = await asyncio.gather(
            repo.claim_outbox_batch(10),
            repo.claim_outbox_batch(10),
        )
        claimed_once = first_claim + second_claim
        claimed_twice = await repo.claim_outbox_batch(10)

        claimed_ids = [row.id for row in claimed_once]
        assert claimed_ids.count(outbox[0].id) == 1
        assert outbox[0].id not in {row.id for row in claimed_twice}

        for row in claimed_once:
            await repo.mark_outbox_sent(row.id, "test-message-id")
        sent = await repo.get_outbox(outbox[0].id)
        assert sent is not None
        assert sent.status == "sent"

        rollback_inbound = InboundMessage(
            channel="telegram",
            external_update_id=f"rollback-{suffix}",
            external_chat_id=f"rollback-chat-{suffix}",
            external_user_id="42",
            text="rollback",
        )
        rollback_claim = await repo.claim_inbound_message(rollback_inbound)
        assert rollback_claim is not None
        with pytest.raises(StatementError):
            await repo.complete_inbound_response(
                inbound_event_id=rollback_claim.event_id,
                conversation_id=rollback_claim.conversation.id,
                channel=rollback_inbound.channel,
                external_chat_id=rollback_inbound.external_chat_id,
                turn_id=turn_id,
                text="must roll back",
                parts=(OutboundPart(text="must roll back"),),
                origin="model",
                metadata={"not_json": object()},
            )

        reclaimed = await repo.claim_inbound_message(rollback_inbound)
        assert reclaimed is not None
        assert reclaimed.message.id == rollback_claim.message.id
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_work_queue_claim_fencing_retry_and_atomic_completion() -> None:
    from alembic import command
    from alembic.config import Config
    from sqlalchemy import func, select, update
    from sqlalchemy.exc import StatementError

    from jasi.adapters.persistence.postgres.db import (
        Message,
        OutboxMessage,
        WorkItem,
        create_engine,
        create_session_factory,
    )
    from jasi.adapters.persistence.postgres.repository import SQLAlchemyRepository
    from jasi.adapters.persistence.postgres.work_repository import SQLAlchemyWorkRepository
    from jasi.domain.models import InboundMessage, OutboundPart
    from jasi.domain.work import (
        OutboundDraft,
        WorkExecutionResult,
        WorkLeaseLost,
        WorkSpec,
    )

    database_url = os.environ["JASI_TEST_DATABASE_URL"]
    os.environ["JASI_DATABASE_URL"] = database_url
    await asyncio.to_thread(command.upgrade, Config("alembic.ini"), "head")

    engine = create_engine(database_url)
    session_factory = create_session_factory(engine)
    work_repo = SQLAlchemyWorkRepository(session_factory)
    try:
        suffix = uuid.uuid4().hex

        def work_spec(
            name: str,
            *,
            session_id: str,
            priority: int,
            conversation_id: int | None = None,
            max_attempts: int = 5,
        ) -> WorkSpec:
            return WorkSpec(
                kind="passive",
                action="agent",
                dedupe_key=f"integration:{suffix}:{name}",
                session_id=session_id,
                conversation_id=conversation_id,
                profile="passive",
                input_text=name,
                priority=priority,
                max_attempts=max_attempts,
            )

        low = await work_repo.enqueue_work(
            work_spec("low", session_id=f"session-a:{suffix}", priority=10)
        )
        duplicate = await work_repo.enqueue_work(
            work_spec("low", session_id=f"session-a:{suffix}", priority=999)
        )
        high = await work_repo.enqueue_work(
            work_spec("high", session_id=f"session-a:{suffix}", priority=100)
        )
        other = await work_repo.enqueue_work(
            work_spec("other", session_id=f"session-b:{suffix}", priority=50)
        )

        first_claims, second_claims = await asyncio.gather(
            work_repo.claim_work_batch(10, lease_seconds=60),
            work_repo.claim_work_batch(10, lease_seconds=60),
        )
        claimed = first_claims + second_claims

        assert duplicate.created is False
        assert duplicate.work.id == low.work.id
        assert {row.id for row in claimed} == {high.work.id, other.work.id}
        assert len({row.id for row in claimed}) == len(claimed)
        assert len({row.session_id for row in claimed}) == len(claimed)

        for row in claimed:
            await work_repo.complete_work(
                row.id,
                row.lease_token or "",
                WorkExecutionResult(),
                (),
            )
        next_claim = await work_repo.claim_work_batch(10, lease_seconds=60)
        assert [row.id for row in next_claim] == [low.work.id]
        await work_repo.complete_work(
            next_claim[0].id,
            next_claim[0].lease_token or "",
            WorkExecutionResult(),
            (),
        )

        stale = await work_repo.enqueue_work(
            work_spec("stale", session_id=f"stale:{suffix}", priority=1000)
        )
        old_lease = (await work_repo.claim_work_batch(1, lease_seconds=0.01))[0]
        await asyncio.sleep(0.02)
        new_lease = (await work_repo.claim_work_batch(1, lease_seconds=60))[0]
        assert new_lease.id == stale.work.id
        assert new_lease.lease_token != old_lease.lease_token
        with pytest.raises(WorkLeaseLost):
            await work_repo.complete_work(
                old_lease.id,
                old_lease.lease_token or "",
                WorkExecutionResult(),
                (),
            )
        await work_repo.complete_work(
            new_lease.id,
            new_lease.lease_token or "",
            WorkExecutionResult(),
            (),
        )

        retry = await work_repo.enqueue_work(
            work_spec(
                "retry",
                session_id=f"retry:{suffix}",
                priority=1000,
                max_attempts=2,
            )
        )
        first_retry = (await work_repo.claim_work_batch(1, lease_seconds=60))[0]
        await work_repo.mark_work_failed_attempt(
            first_retry.id,
            first_retry.lease_token or "",
            "temporary",
        )
        async with session_factory.begin() as session:
            await session.execute(
                update(WorkItem)
                .where(WorkItem.id == retry.work.id)
                .values(available_at=func.now())
            )
        second_retry = (await work_repo.claim_work_batch(1, lease_seconds=60))[0]
        await work_repo.mark_work_failed_attempt(
            second_retry.id,
            second_retry.lease_token or "",
            "exhausted",
        )
        exhausted = await work_repo.get_work(retry.work.id)
        assert exhausted is not None
        assert exhausted.status == "failed"
        assert exhausted.attempts == 2

        chat_repo = SQLAlchemyRepository(session_factory)
        inbound = await chat_repo.claim_inbound_message(
            InboundMessage(
                channel="telegram",
                external_update_id=f"atomic-{suffix}",
                external_chat_id=f"atomic-chat-{suffix}",
                external_user_id="42",
                text="request",
            )
        )
        assert inbound is not None
        outbound_work = await work_repo.enqueue_work(
            work_spec(
                "outbound",
                session_id=f"outbound:{suffix}",
                priority=1000,
                conversation_id=inbound.conversation.id,
            )
        )
        outbound_claim = (await work_repo.claim_work_batch(1, lease_seconds=60))[0]
        invalid_result = WorkExecutionResult(
            turn_id=None,
            outbound=OutboundDraft(
                channel="telegram",
                external_chat_id=inbound.conversation.external_chat_id,
                text="reply",
                origin="model",
                metadata={"not_json": object()},
            ),
        )
        with pytest.raises(StatementError):
            await work_repo.complete_work(
                outbound_claim.id,
                outbound_claim.lease_token or "",
                invalid_result,
                (OutboundPart(text="reply"),),
            )

        after_rollback = await work_repo.get_work(outbound_work.work.id)
        assert after_rollback is not None
        assert after_rollback.status == "running"
        assert after_rollback.output_message_id is None
        async with session_factory() as session:
            assistant_count = await session.scalar(
                select(func.count())
                .select_from(Message)
                .where(
                    Message.conversation_id == inbound.conversation.id,
                    Message.role == "assistant",
                )
            )
            outbox_count = await session.scalar(
                select(func.count())
                .select_from(OutboxMessage)
                .where(OutboxMessage.conversation_id == inbound.conversation.id)
            )
        assert assistant_count == 0
        assert outbox_count == 0

        completion = await work_repo.complete_work(
            outbound_claim.id,
            outbound_claim.lease_token or "",
            WorkExecutionResult(
                outbound=OutboundDraft(
                    channel="telegram",
                    external_chat_id=inbound.conversation.external_chat_id,
                    text="reply",
                    origin="model",
                    metadata={"work_id": -1},
                )
            ),
            (OutboundPart(text="reply"),),
        )
        assert completion.message is not None
        assert completion.message.metadata["work_id"] == outbound_work.work.id
        assert len(completion.outbox) == 1
        for row in completion.outbox:
            await chat_repo.mark_outbox_sent(row.id, "test-message-id")
        completed = await work_repo.get_work(outbound_work.work.id)
        assert completed is not None
        assert completed.status == "succeeded"
    finally:
        await engine.dispose()
