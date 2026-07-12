from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("JASI_TEST_DATABASE_URL"),
        reason="set JASI_TEST_DATABASE_URL to run PostgreSQL integration tests",
    ),
]


@pytest.mark.asyncio
async def test_source_poll_planner_priority_runtime_and_outbox() -> None:
    from alembic import command
    from alembic.config import Config
    from sqlalchemy import select

    from jasi.adapters.channels.telegram import TelegramOutboundPolicy
    from jasi.adapters.persistence.postgres.db import (
        Conversation,
        InitiativeState,
        SourceItem,
        create_engine,
        create_session_factory,
    )
    from jasi.adapters.persistence.postgres.initiative_repository import (
        SQLAlchemyInitiativeRepository,
    )
    from jasi.adapters.persistence.postgres.repository import SQLAlchemyRepository
    from jasi.adapters.persistence.postgres.source_repository import SQLAlchemySourceRepository
    from jasi.adapters.persistence.postgres.work_repository import SQLAlchemyWorkRepository
    from jasi.adapters.system.processes import LocalProcessInspector
    from jasi.application.agent_work import AgentWorkHandler
    from jasi.application.context import TurnContextProvider
    from jasi.application.outbox import OutboxDispatcher, OutboxWorker
    from jasi.application.work import WorkFinalizer
    from jasi.domain.models import InboundMessage
    from jasi.domain.source import (
        SourceBatch,
        SourceItemDraft,
        SourceLeaseLost,
        SourceSubscriptionSpec,
    )
    from jasi.domain.work import WorkExecutionResult
    from jasi.runtime.models import ModelResponse
    from jasi.runtime.profile import PASSIVE_PROFILE, PROACTIVE_PROFILE
    from jasi.runtime.prompting import PromptAssembler, PromptCatalog
    from jasi.runtime.runtime import AgentRuntime
    from jasi.tools.builtin import build_builtin_tool_registry
    from tests.unit.fakes import FakeChannel, FakeModel

    database_url = os.environ["JASI_TEST_DATABASE_URL"]
    os.environ["JASI_DATABASE_URL"] = database_url
    await asyncio.to_thread(command.upgrade, Config("alembic.ini"), "head")

    engine = create_engine(database_url)
    session_factory = create_session_factory(engine)
    sources = SQLAlchemySourceRepository(session_factory)
    initiatives = SQLAlchemyInitiativeRepository(session_factory)
    work_repo = SQLAlchemyWorkRepository(session_factory)
    repository = SQLAlchemyRepository(session_factory)
    try:
        suffix = uuid.uuid4().hex
        chat_id = f"source-chat-{suffix}"
        session_id = f"telegram:{chat_id}"
        async with session_factory.begin() as session:
            conversation = Conversation(channel="telegram", external_chat_id=chat_id)
            session.add(conversation)
            await session.flush()
            conversation_id = conversation.id

        now = datetime.now(UTC).replace(microsecond=0)
        spec = SourceSubscriptionSpec(
            source="fake-feed",
            dedupe_key=f"source-test:{suffix}",
            session_id=session_id,
            conversation_id=conversation_id,
            poll_interval_seconds=60,
            item_ttl_seconds=300,
            cooldown_seconds=120,
            next_poll_at=now,
        )
        created = await sources.create_subscription(spec)
        duplicate = await sources.create_subscription(spec)
        assert created.created is True
        assert duplicate.created is False
        assert duplicate.subscription.id == created.subscription.id

        first_claims, second_claims = await asyncio.gather(
            sources.claim_due_subscriptions(now, 100, lease_seconds=120),
            sources.claim_due_subscriptions(now, 100, lease_seconds=120),
        )
        claims = first_claims + second_claims
        own_claims = [row for row in claims if row.id == created.subscription.id]
        assert len(own_claims) == 1
        for unrelated in claims:
            if unrelated.id != created.subscription.id:
                await sources.mark_source_poll_failed(
                    unrelated.id,
                    unrelated.lease_token or "",
                    "test cleanup",
                    now,
                )

        claim = own_claims[0]
        batch = SourceBatch(
            items=(
                SourceItemDraft(
                    external_id="expired",
                    text="expired item",
                    occurred_at=now - timedelta(minutes=2),
                    expires_at=now - timedelta(seconds=1),
                ),
                SourceItemDraft(
                    external_id="item-a",
                    text="source item A",
                    occurred_at=now - timedelta(seconds=10),
                    payload={"url": "https://example.test/a"},
                ),
                SourceItemDraft(
                    external_id="item-a",
                    text="duplicate source item A",
                    occurred_at=now - timedelta(seconds=9),
                ),
                SourceItemDraft(
                    external_id="item-b",
                    text="source item B",
                    occurred_at=now - timedelta(seconds=5),
                ),
            ),
            next_cursor={"offset": 4},
        )
        inserted = await sources.complete_source_poll(
            claim.id,
            claim.lease_token or "",
            batch,
            now,
        )
        assert inserted == 3

        replay_claims = await sources.claim_due_subscriptions(
            now + timedelta(seconds=61),
            100,
            lease_seconds=120,
        )
        replay = next(row for row in replay_claims if row.id == claim.id)
        replayed = await sources.complete_source_poll(
            replay.id,
            replay.lease_token or "",
            SourceBatch(items=(batch.items[1],), next_cursor={"offset": 4}),
            now + timedelta(seconds=61),
        )
        assert replayed == 0

        stale_spec = SourceSubscriptionSpec(
            source="fake-feed",
            dedupe_key=f"source-stale:{suffix}",
            session_id=f"telegram:stale-{suffix}",
            conversation_id=conversation_id,
            next_poll_at=now,
        )
        stale = await sources.create_subscription(stale_spec)
        stale_old = next(
            row
            for row in await sources.claim_due_subscriptions(now, 100, lease_seconds=1)
            if row.id == stale.subscription.id
        )
        stale_new = next(
            row
            for row in await sources.claim_due_subscriptions(
                now + timedelta(seconds=2),
                100,
                lease_seconds=120,
            )
            if row.id == stale.subscription.id
        )
        with pytest.raises(SourceLeaseLost):
            await sources.complete_source_poll(
                stale_old.id,
                stale_old.lease_token or "",
                SourceBatch(items=()),
                now + timedelta(seconds=2),
            )
        await sources.complete_source_poll(
            stale_new.id,
            stale_new.lease_token or "",
            SourceBatch(items=()),
            now + timedelta(seconds=2),
        )

        plan_time = datetime.now(UTC)
        planned_a, planned_b = await asyncio.gather(
            initiatives.materialize_initiatives("proactive", plan_time, 100),
            initiatives.materialize_initiatives("proactive", plan_time, 100),
        )
        all_planned = planned_a + planned_b
        planned = [
            row
            for row in all_planned
            if row.payload.get("source_subscription_id") == created.subscription.id
        ]
        assert len(planned) == 1
        proactive_work = planned[0]
        assert proactive_work.input_text == "source item A"
        assert proactive_work.priority == 40
        assert proactive_work.profile == "proactive"

        passive = await work_repo.enqueue_passive(
            InboundMessage(
                channel="telegram",
                external_update_id=f"passive-{suffix}",
                external_chat_id=chat_id,
                external_user_id="42",
                text="hello before the update",
            )
        )
        assert passive is not None
        claimed_work = await work_repo.claim_work_batch(100, lease_seconds=120)
        own_claimed = [row for row in claimed_work if row.session_id == session_id]
        assert [row.id for row in own_claimed] == [passive.work.id]
        for row in claimed_work:
            await work_repo.complete_work(
                row.id,
                row.lease_token or "",
                WorkExecutionResult(),
                (),
            )

        next_claimed = await work_repo.claim_work_batch(100, lease_seconds=120)
        proactive_claim = next(row for row in next_claimed if row.id == proactive_work.id)
        for row in next_claimed:
            if row.id != proactive_work.id:
                await work_repo.complete_work(
                    row.id,
                    row.lease_token or "",
                    WorkExecutionResult(),
                    (),
                )

        model = FakeModel([ModelResponse(content="A relevant update")])
        runtime = AgentRuntime(
            profiles={
                PASSIVE_PROFILE.name: PASSIVE_PROFILE,
                PROACTIVE_PROFILE.name: PROACTIVE_PROFILE,
            },
            model=model,
            repository=repository,
            context_provider=TurnContextProvider(repository=repository),
            prompt_assembler=PromptAssembler(
                PromptCatalog(
                    self_model="You are Jasi.",
                    profiles={
                        "passive": "Handle passive context.",
                        "proactive": "Handle proactive context.",
                    },
                )
            ),
            tools=build_builtin_tool_registry(
                messages=repository,
                processes=LocalProcessInspector(),
            ),
            model_name="test-model",
            model_timeout_seconds=5,
            timezone="Asia/Shanghai",
        )
        result = await AgentWorkHandler(
            runtime=runtime,
            conversations=repository,
        ).execute(proactive_claim)
        outbox_wakeup = asyncio.Event()
        await WorkFinalizer(
            repository=work_repo,
            outbound_policies={"telegram": TelegramOutboundPolicy()},
            outbox_wakeup=outbox_wakeup,
        ).complete(proactive_claim, result)
        channel = FakeChannel()
        outbox_worker = OutboxWorker(
            repository=repository,
            dispatcher=OutboxDispatcher(
                repository=repository,
                channels={"telegram": channel},
            ),
            batch_size=100,
            wakeup=outbox_wakeup,
        )
        while await outbox_worker.drain_once():
            pass

        user_prompts = [
            message.content for message in model.requests[0].messages if message.role == "user"
        ]
        assert user_prompts == ["hello before the update", "source item A"]
        assert any(message.text == "A relevant update" for message in channel.sent)
        history = await repository.load_history(
            conversation_id,
            before_sequence=None,
            limit=30,
        )
        assert [(row.role, row.content) for row in history] == [
            ("user", "hello before the update"),
            ("assistant", "A relevant update"),
        ]

        within_cooldown = await initiatives.materialize_initiatives(
            "proactive",
            plan_time + timedelta(seconds=60),
            100,
        )
        assert not [
            row
            for row in within_cooldown
            if row.payload.get("source_subscription_id") == created.subscription.id
        ]
        after_cooldown = await initiatives.materialize_initiatives(
            "proactive",
            plan_time + timedelta(seconds=121),
            100,
        )
        own_after_cooldown = [
            row
            for row in after_cooldown
            if row.payload.get("source_subscription_id") == created.subscription.id
        ]
        assert len(own_after_cooldown) == 1
        assert own_after_cooldown[0].input_text == "source item B"

        async with session_factory() as session:
            items = list(
                (
                    await session.scalars(
                        select(SourceItem)
                        .where(SourceItem.subscription_id == created.subscription.id)
                        .order_by(SourceItem.external_id)
                    )
                ).all()
            )
            state = await session.get(InitiativeState, session_id)
        assert [(item.external_id, item.status) for item in items] == [
            ("expired", "expired"),
            ("item-a", "enqueued"),
            ("item-b", "enqueued"),
        ]
        assert state is not None
        assert state.last_user_at is not None
        assert state.last_delivery_at is not None

        while True:
            cleanup = await work_repo.claim_work_batch(100, lease_seconds=120)
            if not cleanup:
                break
            for row in cleanup:
                await work_repo.complete_work(
                    row.id,
                    row.lease_token or "",
                    WorkExecutionResult(),
                    (),
                )
    finally:
        await engine.dispose()
