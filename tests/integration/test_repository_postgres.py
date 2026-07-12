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
async def test_concurrent_passive_ingress_serializes_messages_and_deduplicates() -> None:
    from alembic import command
    from alembic.config import Config
    from sqlalchemy import func, select

    from jasi.adapters.persistence.postgres.db import (
        InboundEvent,
        Message,
        WorkItem,
        create_engine,
        create_session_factory,
    )
    from jasi.adapters.persistence.postgres.work_repository import SQLAlchemyWorkRepository
    from jasi.domain.models import InboundMessage
    from jasi.domain.work import WorkExecutionResult

    database_url = os.environ["JASI_TEST_DATABASE_URL"]
    os.environ["JASI_DATABASE_URL"] = database_url
    await asyncio.to_thread(command.upgrade, Config("alembic.ini"), "head")

    engine = create_engine(database_url)
    session_factory = create_session_factory(engine)
    repo = SQLAlchemyWorkRepository(session_factory)
    try:
        suffix = uuid.uuid4().hex
        chat_id = f"concurrent-chat-{suffix}"

        def inbound(update_id: str) -> InboundMessage:
            return InboundMessage(
                channel="telegram",
                external_update_id=f"{suffix}:{update_id}",
                external_chat_id=chat_id,
                external_user_id="42",
                text=f"message-{update_id}",
            )

        initial = await asyncio.gather(
            *(repo.enqueue_passive(inbound(str(index))) for index in range(10))
        )
        raced = await asyncio.gather(
            repo.enqueue_passive(inbound("same")),
            repo.enqueue_passive(inbound("same")),
        )

        assert all(result is not None and result.created for result in initial)
        assert sorted(result.created for result in raced if result is not None) == [False, True]
        conversation_ids = {
            result.work.conversation_id for result in [*initial, *raced] if result is not None
        }
        assert len(conversation_ids) == 1
        conversation_id = conversation_ids.pop()
        assert conversation_id is not None

        async with session_factory() as session:
            sequences = list(
                await session.scalars(
                    select(Message.sequence)
                    .where(Message.conversation_id == conversation_id)
                    .order_by(Message.sequence)
                )
            )
            event_count = await session.scalar(
                select(func.count())
                .select_from(InboundEvent)
                .where(InboundEvent.conversation_id == conversation_id)
            )
            work_count = await session.scalar(
                select(func.count())
                .select_from(WorkItem)
                .where(WorkItem.conversation_id == conversation_id)
            )
        assert sequences == list(range(1, 12))
        assert event_count == 11
        assert work_count == 11

        claimed = await repo.claim_work_batch(20, lease_seconds=60)
        session_claims = [row for row in claimed if row.conversation_id == conversation_id]
        assert len(session_claims) == 1
        for row in claimed:
            await repo.complete_work(
                row.id,
                row.lease_token or "",
                WorkExecutionResult(),
                (),
            )
        while True:
            claimed = await repo.claim_work_batch(20, lease_seconds=60)
            if not claimed:
                break
            for row in claimed:
                await repo.complete_work(
                    row.id,
                    row.lease_token or "",
                    WorkExecutionResult(),
                    (),
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_passive_ingress_turn_recovery_and_ordered_outbox() -> None:
    from alembic import command
    from alembic.config import Config
    from sqlalchemy import func, select
    from sqlalchemy.exc import StatementError

    from jasi.adapters.persistence.postgres.db import (
        InboundEvent,
        Message,
        check_database_ready,
        create_engine,
        create_session_factory,
    )
    from jasi.adapters.persistence.postgres.repository import SQLAlchemyRepository
    from jasi.adapters.persistence.postgres.work_repository import SQLAlchemyWorkRepository
    from jasi.domain.models import InboundMessage, OutboundPart
    from jasi.domain.work import OutboundDraft, WorkExecutionResult
    from jasi.runtime.models import ToolExecutionRecord, Usage

    database_url = os.environ["JASI_TEST_DATABASE_URL"]
    os.environ["JASI_DATABASE_URL"] = database_url

    alembic_config = Config("alembic.ini")
    await asyncio.to_thread(command.upgrade, alembic_config, "head")

    engine = create_engine(database_url)
    try:
        await check_database_ready(engine)
        session_factory = create_session_factory(engine)
        repo = SQLAlchemyRepository(session_factory)
        work_repo = SQLAlchemyWorkRepository(session_factory)

        suffix = uuid.uuid4().hex
        inbound = InboundMessage(
            channel="feishu",
            external_update_id=f"update-{suffix}",
            external_chat_id=f"chat-{suffix}",
            external_user_id="42",
            text="hello",
        )
        queued = await work_repo.enqueue_passive(inbound)
        duplicate = await work_repo.enqueue_passive(inbound)

        assert queued is not None
        assert duplicate is not None
        assert queued.created is True
        assert duplicate.created is False
        assert duplicate.work.id == queued.work.id
        assert queued.work.inbound_event_id is not None
        assert queued.work.payload["history_before_sequence"] == 1
        history = await repo.load_history(
            queued.work.conversation_id or 0,
            before_sequence=None,
            limit=30,
        )
        assert [(row.role, row.origin, row.content) for row in history] == [
            ("user", inbound.channel, inbound.text)
        ]

        work_claims = await work_repo.claim_work_batch(100, lease_seconds=60)
        claim = next(row for row in work_claims if row.id == queued.work.id)
        for unrelated in work_claims:
            if unrelated.id != claim.id:
                await work_repo.complete_work(
                    unrelated.id,
                    unrelated.lease_token or "",
                    WorkExecutionResult(),
                    (),
                )

        turn_start = await repo.start_turn(
            work_id=claim.id,
            conversation_id=claim.conversation_id or 0,
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
            work_id=claim.id,
            conversation_id=claim.conversation_id or 0,
            profile="passive",
            model="test-model",
            metadata={},
        )

        assert cached_turn.cached_result is not None
        assert cached_turn.cached_result.final_text == "reply"
        assert len(cached_turn.cached_result.tool_records) == 1

        completion = await work_repo.complete_work(
            claim.id,
            claim.lease_token or "",
            WorkExecutionResult(
                turn_id=turn_id,
                outbound=OutboundDraft(
                    channel=inbound.channel,
                    external_chat_id=inbound.external_chat_id,
                    text="reply-one reply-two",
                    origin="model",
                ),
            ),
            (OutboundPart(text="reply-one"), OutboundPart(text="reply-two")),
        )
        after_completion = await work_repo.enqueue_passive(inbound)

        assert after_completion is not None
        assert after_completion.created is False
        assert after_completion.work.status == "succeeded"
        assert len(completion.outbox) == 2
        before_delivery = await repo.load_history(
            claim.conversation_id or 0,
            before_sequence=None,
            limit=30,
        )
        assert [row.role for row in before_delivery] == ["user"]
        assert completion.message is not None
        pending_search, pending_total = await repo.search_messages(
            claim.conversation_id or 0,
            "reply-one",
            limit=10,
            offset=0,
        )
        pending_fetch = await repo.fetch_messages(
            claim.conversation_id or 0,
            (completion.message.id,),
        )
        assert pending_search == []
        assert pending_total == 0
        assert pending_fetch == []

        first_claim, second_claim = await asyncio.gather(
            repo.claim_outbox_batch(10),
            repo.claim_outbox_batch(10),
        )
        claimed_once = first_claim + second_claim
        target_ids = {row.id for row in completion.outbox}
        first_target = [row for row in claimed_once if row.id in target_ids]
        assert [row.segment_index for row in first_target] == [0]

        for row in claimed_once:
            await repo.mark_outbox_sent(row.id, "test-message-id")
        after_first_segment = await repo.load_history(
            claim.conversation_id or 0,
            before_sequence=None,
            limit=30,
        )
        assert [row.role for row in after_first_segment] == ["user"]
        next_claim = await repo.claim_outbox_batch(10)
        second_target = [row for row in next_claim if row.id in target_ids]
        assert [row.segment_index for row in second_target] == [1]
        for row in next_claim:
            await repo.mark_outbox_sent(row.id, "test-message-id")
        sent = await repo.get_outbox(completion.outbox[1].id)
        assert sent is not None
        assert sent.status == "sent"
        after_delivery = await repo.load_history(
            claim.conversation_id or 0,
            before_sequence=None,
            limit=30,
        )
        assert [(row.role, row.content) for row in after_delivery] == [
            ("user", "hello"),
            ("assistant", "reply-one reply-two"),
        ]
        sent_search, sent_total = await repo.search_messages(
            claim.conversation_id or 0,
            "reply-one",
            limit=10,
            offset=0,
        )
        sent_fetch = await repo.fetch_messages(
            claim.conversation_id or 0,
            (completion.message.id,),
        )
        assert [row.id for row in sent_search] == [completion.message.id]
        assert sent_total == 1
        assert [row.id for row in sent_fetch] == [completion.message.id]

        async with session_factory.begin() as session:
            hidden = Message(
                conversation_id=claim.conversation_id or 0,
                role="assistant",
                origin="system_error",
                sequence=10_000,
                content="hidden system error",
                delivery_status="sent",
                meta={},
            )
            session.add(hidden)
            await session.flush()
            hidden_id = hidden.id
        hidden_search, hidden_total = await repo.search_messages(
            claim.conversation_id or 0,
            "hidden system error",
            limit=10,
            offset=0,
        )
        hidden_fetch = await repo.fetch_messages(
            claim.conversation_id or 0,
            (hidden_id,),
        )
        assert hidden_search == []
        assert hidden_total == 0
        assert hidden_fetch == []

        rollback_inbound = InboundMessage(
            channel="telegram",
            external_update_id=f"rollback-{suffix}",
            external_chat_id=f"rollback-chat-{suffix}",
            external_user_id="42",
            text="rollback",
            metadata={"not_json": object()},
        )
        with pytest.raises(StatementError):
            await work_repo.enqueue_passive(rollback_inbound)
        async with session_factory() as session:
            rolled_back_events = await session.scalar(
                select(func.count())
                .select_from(InboundEvent)
                .where(
                    InboundEvent.channel == rollback_inbound.channel,
                    InboundEvent.external_update_id == rollback_inbound.external_update_id,
                )
            )
        assert rolled_back_events == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_work_queue_claim_fencing_retry_and_atomic_completion() -> None:
    from alembic import command
    from alembic.config import Config
    from sqlalchemy import func, select, update
    from sqlalchemy.exc import StatementError

    from jasi.adapters.persistence.postgres.db import (
        Conversation,
        Message,
        OutboxMessage,
        WorkItem,
        create_engine,
        create_session_factory,
    )
    from jasi.adapters.persistence.postgres.repository import SQLAlchemyRepository
    from jasi.adapters.persistence.postgres.work_repository import SQLAlchemyWorkRepository
    from jasi.domain.models import OutboundPart
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

        heartbeat = await work_repo.enqueue_work(
            work_spec("heartbeat", session_id=f"heartbeat:{suffix}", priority=1000)
        )
        heartbeat_claim = (await work_repo.claim_work_batch(1, lease_seconds=0.05))[0]
        original_lease_until = heartbeat_claim.lease_until
        await asyncio.sleep(0.02)
        renewed_until = await work_repo.renew_work_lease(
            heartbeat_claim.id,
            heartbeat_claim.lease_token or "",
            lease_seconds=0.1,
        )
        await asyncio.sleep(0.04)
        competing_claims = await work_repo.claim_work_batch(100, lease_seconds=60)
        assert heartbeat.work.id not in {row.id for row in competing_claims}
        assert original_lease_until is not None
        assert renewed_until > original_lease_until
        for row in competing_claims:
            await work_repo.complete_work(
                row.id,
                row.lease_token or "",
                WorkExecutionResult(),
                (),
            )
        await work_repo.complete_work(
            heartbeat_claim.id,
            heartbeat_claim.lease_token or "",
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

        repo = SQLAlchemyRepository(session_factory)
        async with session_factory.begin() as session:
            conversation = Conversation(
                channel="telegram",
                external_chat_id=f"atomic-chat-{suffix}",
            )
            session.add(conversation)
            await session.flush()
            conversation_id = conversation.id
        outbound_work = await work_repo.enqueue_work(
            work_spec(
                "outbound",
                session_id=f"outbound:{suffix}",
                priority=1000,
                conversation_id=conversation_id,
            )
        )
        outbound_claim = (await work_repo.claim_work_batch(1, lease_seconds=60))[0]
        invalid_result = WorkExecutionResult(
            turn_id=None,
            outbound=OutboundDraft(
                channel="telegram",
                external_chat_id=f"atomic-chat-{suffix}",
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
                    Message.conversation_id == conversation_id,
                    Message.role == "assistant",
                )
            )
            outbox_count = await session.scalar(
                select(func.count())
                .select_from(OutboxMessage)
                .where(OutboxMessage.conversation_id == conversation_id)
            )
        assert assistant_count == 0
        assert outbox_count == 0

        completion = await work_repo.complete_work(
            outbound_claim.id,
            outbound_claim.lease_token or "",
            WorkExecutionResult(
                outbound=OutboundDraft(
                    channel="telegram",
                    external_chat_id=f"atomic-chat-{suffix}",
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
            await repo.mark_outbox_sent(row.id, "test-message-id")
        completed = await work_repo.get_work(outbound_work.work.id)
        assert completed is not None
        assert completed.status == "succeeded"
    finally:
        await engine.dispose()
