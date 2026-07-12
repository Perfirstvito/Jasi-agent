from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("JASI_TEST_DATABASE_URL"),
        reason="set JASI_TEST_DATABASE_URL to run PostgreSQL integration tests",
    ),
]


@pytest.mark.asyncio
async def test_source_effect_outbox_is_atomic_deduplicated_and_retryable() -> None:
    from alembic import command
    from alembic.config import Config
    from sqlalchemy import func, select, update
    from sqlalchemy.exc import StatementError

    from jasi.adapters.persistence.postgres.db import (
        Conversation,
        EffectOutbox,
        SourceItem,
        SourceSubscription,
        create_engine,
        create_session_factory,
    )
    from jasi.adapters.persistence.postgres.effect_repository import SQLAlchemyEffectRepository
    from jasi.adapters.persistence.postgres.source_repository import SQLAlchemySourceRepository
    from jasi.application.effect import EffectDispatcher, EffectWorker
    from jasi.domain.effect import EffectDraft, EffectResult
    from jasi.domain.source import SourceBatch, SourceItemDraft, SourceSubscriptionSpec

    database_url = os.environ["JASI_TEST_DATABASE_URL"]
    os.environ["JASI_DATABASE_URL"] = database_url
    await asyncio.to_thread(command.upgrade, Config("alembic.ini"), "head")

    engine = create_engine(database_url)
    session_factory = create_session_factory(engine)
    sources = SQLAlchemySourceRepository(session_factory)
    effects = SQLAlchemyEffectRepository(session_factory)
    try:
        suffix = uuid.uuid4().hex
        async with session_factory.begin() as session:
            conversation = Conversation(
                channel="telegram",
                external_chat_id=f"effect-chat-{suffix}",
            )
            session.add(conversation)
            await session.flush()
            conversation_id = conversation.id

        now = datetime.now(UTC)
        subscription = await sources.create_subscription(
            SourceSubscriptionSpec(
                source="feed",
                dedupe_key=f"effect-source:{suffix}",
                session_id=f"telegram:effect-chat-{suffix}",
                conversation_id=conversation_id,
                next_poll_at=now,
                poll_interval_seconds=60,
            )
        )
        claim = next(
            row
            for row in await sources.claim_due_subscriptions(now, 100, lease_seconds=120)
            if row.id == subscription.subscription.id
        )
        effect_draft = EffectDraft(
            adapter="feed",
            operation="ack",
            dedupe_key=f"ack:{suffix}:item-1",
            payload={"external_id": "item-1"},
        )
        inserted = await sources.complete_source_poll(
            claim.id,
            claim.lease_token or "",
            SourceBatch(
                items=(
                    SourceItemDraft(
                        external_id="item-1",
                        text="item",
                        occurred_at=now,
                    ),
                ),
                next_cursor={"offset": 1},
                effects=(effect_draft, effect_draft),
            ),
            now,
        )
        assert inserted == 1

        async with session_factory() as session:
            stored_subscription = await session.get(
                SourceSubscription,
                subscription.subscription.id,
            )
            item_count = await session.scalar(
                select(func.count())
                .select_from(SourceItem)
                .where(SourceItem.subscription_id == subscription.subscription.id)
            )
            effect_count = await session.scalar(
                select(func.count())
                .select_from(EffectOutbox)
                .where(EffectOutbox.dedupe_key == effect_draft.dedupe_key)
            )
        assert stored_subscription is not None
        assert stored_subscription.cursor == {"offset": 1}
        assert item_count == 1
        assert effect_count == 1

        first_claim, second_claim = await asyncio.gather(
            effects.claim_effect_batch(100),
            effects.claim_effect_batch(100),
        )
        claimed = [
            row
            for row in [*first_claim, *second_claim]
            if row.dedupe_key == effect_draft.dedupe_key
        ]
        assert len(claimed) == 1

        class Adapter:
            def __init__(self) -> None:
                self.calls = 0

            async def execute(self, _effect):
                self.calls += 1
                if self.calls == 1:
                    return EffectResult(
                        success=False,
                        error="temporary",
                        retryable=True,
                    )
                return EffectResult(success=True, result={"acked": True})

        adapter = Adapter()
        dispatcher = EffectDispatcher(
            repository=effects,
            adapters={"feed": adapter},
        )
        await dispatcher.execute(claimed[0])
        pending = await effects.get_effect(claimed[0].id)
        assert pending is not None
        assert pending.status == "pending"
        assert pending.attempts == 1

        async with session_factory.begin() as session:
            await session.execute(
                update(EffectOutbox)
                .where(EffectOutbox.id == claimed[0].id)
                .values(next_attempt_at=func.now())
            )
        restarted_worker = EffectWorker(
            repository=SQLAlchemyEffectRepository(session_factory),
            dispatcher=dispatcher,
            batch_size=100,
            wakeup=asyncio.Event(),
        )
        await restarted_worker.drain_once()
        succeeded = await effects.get_effect(claimed[0].id)
        assert succeeded is not None
        assert succeeded.status == "succeeded"
        assert succeeded.result == {"acked": True}
        assert adapter.calls == 2

        rollback_subscription = await sources.create_subscription(
            SourceSubscriptionSpec(
                source="feed",
                dedupe_key=f"effect-rollback:{suffix}",
                session_id=f"telegram:effect-rollback-{suffix}",
                conversation_id=conversation_id,
                next_poll_at=now,
            )
        )
        rollback_claim = next(
            row
            for row in await sources.claim_due_subscriptions(now, 100, lease_seconds=120)
            if row.id == rollback_subscription.subscription.id
        )
        with pytest.raises(StatementError):
            await sources.complete_source_poll(
                rollback_claim.id,
                rollback_claim.lease_token or "",
                SourceBatch(
                    items=(
                        SourceItemDraft(
                            external_id="must-rollback",
                            text="rollback",
                            occurred_at=now,
                        ),
                    ),
                    next_cursor={"offset": 99},
                    effects=(
                        EffectDraft(
                            adapter="feed",
                            operation="ack",
                            dedupe_key=f"ack:{suffix}:rollback",
                            payload={"not_json": object()},
                        ),
                    ),
                ),
                now,
            )
        async with session_factory() as session:
            rollback_row = await session.get(
                SourceSubscription,
                rollback_subscription.subscription.id,
            )
            rollback_items = await session.scalar(
                select(func.count())
                .select_from(SourceItem)
                .where(SourceItem.subscription_id == rollback_subscription.subscription.id)
            )
            rollback_effects = await session.scalar(
                select(func.count())
                .select_from(EffectOutbox)
                .where(EffectOutbox.dedupe_key == f"ack:{suffix}:rollback")
            )
        assert rollback_row is not None and rollback_row.cursor == {}
        assert rollback_items == 0
        assert rollback_effects == 0
    finally:
        await engine.dispose()
