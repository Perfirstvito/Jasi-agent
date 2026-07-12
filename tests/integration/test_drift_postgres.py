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
async def test_drift_idle_gate_passive_cancellation_runtime_and_outbox() -> None:
    from alembic import command
    from alembic.config import Config
    from sqlalchemy import func, select, update

    from jasi.adapters.channels.telegram import TelegramOutboundPolicy
    from jasi.adapters.persistence.postgres.db import (
        Conversation,
        DriftOpportunity,
        InitiativeState,
        WorkItem,
        create_engine,
        create_session_factory,
    )
    from jasi.adapters.persistence.postgres.drift_repository import SQLAlchemyDriftRepository
    from jasi.adapters.persistence.postgres.initiative_repository import (
        SQLAlchemyInitiativeRepository,
    )
    from jasi.adapters.persistence.postgres.repository import SQLAlchemyRepository
    from jasi.adapters.persistence.postgres.work_repository import SQLAlchemyWorkRepository
    from jasi.application.agent_work import AgentWorkHandler
    from jasi.application.context import TurnContextProvider
    from jasi.application.outbox import OutboxDispatcher, OutboxWorker
    from jasi.application.work import WorkFinalizer
    from jasi.domain.drift import DriftOpportunitySpec
    from jasi.domain.models import InboundMessage
    from jasi.domain.work import WorkExecutionResult
    from jasi.runtime.models import ModelResponse
    from jasi.runtime.profile import DRIFT_PROFILE
    from jasi.runtime.prompting import PromptAssembler, PromptCatalog
    from jasi.runtime.runtime import AgentRuntime
    from jasi.tools.builtin import build_builtin_tool_registry
    from tests.unit.fakes import FakeChannel, FakeModel

    database_url = os.environ["JASI_TEST_DATABASE_URL"]
    os.environ["JASI_DATABASE_URL"] = database_url
    await asyncio.to_thread(command.upgrade, Config("alembic.ini"), "head")

    engine = create_engine(database_url)
    session_factory = create_session_factory(engine)
    drift = SQLAlchemyDriftRepository(session_factory)
    initiatives = SQLAlchemyInitiativeRepository(session_factory)
    work_repo = SQLAlchemyWorkRepository(session_factory)
    repository = SQLAlchemyRepository(session_factory)
    future_work_ids: list[int] = []
    try:
        suffix = uuid.uuid4().hex
        now = datetime.now(UTC).replace(microsecond=0)

        async def create_conversation(name: str) -> tuple[int, str, str]:
            chat_id = f"drift-{name}-{suffix}"
            session_id = f"telegram:{chat_id}"
            async with session_factory.begin() as session:
                row = Conversation(channel="telegram", external_chat_id=chat_id)
                session.add(row)
                await session.flush()
                return row.id, chat_id, session_id

        idle_conversation, _, idle_session = await create_conversation("idle")
        async with session_factory.begin() as session:
            session.add(
                InitiativeState(
                    session_id=idle_session,
                    conversation_id=idle_conversation,
                    last_user_at=now,
                )
            )
        idle_offer = await drift.offer_drift(
            DriftOpportunitySpec(
                dedupe_key=f"drift-idle:{suffix}",
                session_id=idle_session,
                conversation_id=idle_conversation,
                input_text="Bring up the saved topic naturally",
                available_at=now,
                expires_at=now + timedelta(minutes=10),
                min_idle_seconds=60,
                cooldown_seconds=600,
            )
        )
        too_early = await initiatives.materialize_initiatives(
            "drift",
            now + timedelta(seconds=30),
            100,
        )
        assert not [row for row in too_early if row.session_id == idle_session]
        after_idle = await initiatives.materialize_initiatives(
            "drift",
            now + timedelta(seconds=61),
            100,
        )
        idle_work = [row for row in after_idle if row.session_id == idle_session]
        assert len(idle_work) == 1
        assert idle_work[0].priority == 20
        future_work_ids.append(idle_work[0].id)
        assert idle_offer.opportunity.status == "new"

        cancel_conversation, cancel_chat, cancel_session = await create_conversation("cancel")
        cancel_offer = await drift.offer_drift(
            DriftOpportunitySpec(
                dedupe_key=f"drift-cancel:{suffix}",
                session_id=cancel_session,
                conversation_id=cancel_conversation,
                input_text="Start a casual check-in",
                available_at=datetime.now(UTC),
                min_idle_seconds=0,
                cooldown_seconds=600,
            )
        )
        cancel_planned = await initiatives.materialize_initiatives(
            "drift",
            datetime.now(UTC),
            100,
        )
        cancel_work = next(row for row in cancel_planned if row.session_id == cancel_session)
        passive = await work_repo.enqueue_passive(
            InboundMessage(
                channel="telegram",
                external_update_id=f"cancel-passive:{suffix}",
                external_chat_id=cancel_chat,
                external_user_id="42",
                text="I am here",
            )
        )
        assert passive is not None
        cancelled = await work_repo.get_work(cancel_work.id)
        assert cancelled is not None
        assert cancelled.status == "cancelled"
        assert cancelled.last_error == "superseded_by_user_activity"
        claimed = await work_repo.claim_work_batch(100, lease_seconds=120)
        own_claimed = [row for row in claimed if row.session_id == cancel_session]
        assert [row.id for row in own_claimed] == [passive.work.id]
        for row in claimed:
            await work_repo.complete_work(
                row.id,
                row.lease_token or "",
                WorkExecutionResult(),
                (),
            )
        assert cancel_offer.opportunity.status == "new"

        run_conversation, run_chat, run_session = await create_conversation("run")
        run_spec = DriftOpportunitySpec(
            dedupe_key=f"drift-run:{suffix}",
            session_id=run_session,
            conversation_id=run_conversation,
            input_text="Ask how the user's side project is going",
            available_at=datetime.now(UTC),
            min_idle_seconds=0,
            cooldown_seconds=600,
        )
        run_offer = await drift.offer_drift(run_spec)
        run_duplicate = await drift.offer_drift(run_spec)
        assert run_offer.created is True
        assert run_duplicate.created is False

        planned_a, planned_b = await asyncio.gather(
            initiatives.materialize_initiatives("drift", datetime.now(UTC), 100),
            initiatives.materialize_initiatives("drift", datetime.now(UTC), 100),
        )
        run_planned = [row for row in [*planned_a, *planned_b] if row.session_id == run_session]
        assert len(run_planned) == 1
        run_work = run_planned[0]
        assert run_work.kind == "drift"
        assert run_work.profile == "drift"

        claimed = await work_repo.claim_work_batch(100, lease_seconds=120)
        run_claim = next(row for row in claimed if row.id == run_work.id)
        for row in claimed:
            if row.id != run_work.id:
                await work_repo.complete_work(
                    row.id,
                    row.lease_token or "",
                    WorkExecutionResult(),
                    (),
                )

        model = FakeModel([ModelResponse(content="How is your side project going?")])
        runtime = AgentRuntime(
            profiles={DRIFT_PROFILE.name: DRIFT_PROFILE},
            model=model,
            repository=repository,
            context_provider=TurnContextProvider(repository=repository),
            prompt_assembler=PromptAssembler(
                PromptCatalog(
                    self_model="You are Jasi.",
                    profiles={"drift": "Handle drift context."},
                )
            ),
            tools=build_builtin_tool_registry(),
            model_name="test-model",
            model_timeout_seconds=5,
            timezone="Asia/Shanghai",
        )
        result = await AgentWorkHandler(
            runtime=runtime,
            conversations=repository,
        ).execute(run_claim)
        outbox_wakeup = asyncio.Event()
        await WorkFinalizer(
            repository=work_repo,
            outbound_policies={"telegram": TelegramOutboundPolicy()},
            outbox_wakeup=outbox_wakeup,
        ).complete(run_claim, result)
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

        model_inputs = [
            message.content for message in model.requests[0].messages if message.role == "user"
        ]
        assert model_inputs == ["Ask how the user's side project is going"]
        assert any(
            message.external_chat_id == run_chat
            and message.text == "How is your side project going?"
            for message in channel.sent
        )
        history = await repository.load_history(
            run_conversation,
            before_sequence=None,
            limit=30,
        )
        assert [(row.role, row.content) for row in history] == [
            ("assistant", "How is your side project going?")
        ]

        second_offer = await drift.offer_drift(
            DriftOpportunitySpec(
                dedupe_key=f"drift-run-second:{suffix}",
                session_id=run_session,
                conversation_id=run_conversation,
                input_text="Ask another natural question",
                available_at=now,
                expires_at=now + timedelta(hours=1),
                min_idle_seconds=0,
                cooldown_seconds=600,
            )
        )
        within_cooldown = await initiatives.materialize_initiatives(
            "drift",
            datetime.now(UTC) + timedelta(seconds=300),
            100,
        )
        assert not [row for row in within_cooldown if row.session_id == run_session]
        after_cooldown = await initiatives.materialize_initiatives(
            "drift",
            datetime.now(UTC) + timedelta(seconds=601),
            100,
        )
        second_work = [row for row in after_cooldown if row.session_id == run_session]
        assert len(second_work) == 1
        assert second_work[0].input_text == "Ask another natural question"
        future_work_ids.append(second_work[0].id)

        async with session_factory() as session:
            state = await session.get(InitiativeState, run_session)
            stored_opportunities = list(
                (
                    await session.scalars(
                        select(DriftOpportunity)
                        .where(DriftOpportunity.session_id == run_session)
                        .order_by(DriftOpportunity.id)
                    )
                ).all()
            )
        assert state is not None and state.last_delivery_at is not None
        assert [row.status for row in stored_opportunities] == ["enqueued", "enqueued"]
        assert second_offer.created

        async with session_factory.begin() as session:
            await session.execute(
                update(WorkItem)
                .where(WorkItem.id.in_(future_work_ids))
                .values(available_at=func.now())
            )
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
