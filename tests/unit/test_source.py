from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from jasi.application.source import (
    InitiativePlanner,
    SourceDispatcher,
    SourceService,
    SourceWorker,
)
from jasi.domain.effect import EffectDraft
from jasi.domain.source import (
    SourceBatch,
    SourceCreateResult,
    SourceItemDraft,
    SourceSubscriptionRecord,
    SourceSubscriptionSpec,
)
from jasi.domain.work import WorkSpec
from tests.unit.fakes import FakeWorkRepository


def subscription() -> SourceSubscriptionRecord:
    now = datetime.now(UTC)
    return SourceSubscriptionRecord(
        id=1,
        source="fake",
        dedupe_key="fake:1",
        session_id="telegram:1",
        conversation_id=1,
        profile="proactive",
        config={"topic": "agents"},
        cursor={"offset": 2},
        poll_interval_seconds=60,
        item_ttl_seconds=300,
        cooldown_seconds=120,
        priority=40,
        next_poll_at=now,
        enabled=True,
        poll_attempts=1,
        lease_token="lease-1",
        lease_until=now + timedelta(seconds=120),
    )


def test_source_models_reject_unstable_identity_and_time() -> None:
    with pytest.raises(ValueError, match="dedupe key"):
        SourceSubscriptionSpec(
            source="fake",
            dedupe_key=" ",
            session_id="telegram:1",
            conversation_id=1,
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        SourceItemDraft(
            external_id="1",
            text="item",
            occurred_at=datetime(2026, 7, 11),
        )


@pytest.mark.asyncio
async def test_source_worker_polls_by_registry_and_wakes_planner() -> None:
    record = subscription()

    class Repository:
        def __init__(self) -> None:
            self.claimed = False
            self.completed: SourceBatch | None = None

        async def claim_due_subscriptions(self, _now, _limit, _lease_seconds):
            if self.claimed:
                return []
            self.claimed = True
            return [record]

        async def complete_source_poll(self, _id, _token, batch, _now):
            self.completed = batch
            return len(batch.items)

        async def mark_source_poll_failed(self, *_args):
            raise AssertionError("successful poll must not fail")

    class Source:
        async def poll(self, claimed: SourceSubscriptionRecord) -> SourceBatch:
            assert claimed.cursor == {"offset": 2}
            return SourceBatch(
                items=(
                    SourceItemDraft(
                        external_id="item-3",
                        text="new item",
                        occurred_at=datetime.now(UTC),
                    ),
                ),
                next_cursor={"offset": 3},
                effects=(
                    EffectDraft(
                        adapter="fake",
                        operation="ack",
                        dedupe_key="ack:item-3",
                    ),
                ),
            )

    repository = Repository()
    initiative_wakeup = asyncio.Event()
    effect_wakeup = asyncio.Event()
    worker = SourceWorker(
        repository=repository,
        dispatcher=SourceDispatcher({"fake": Source()}),
        batch_size=10,
        source_wakeup=asyncio.Event(),
        initiative_wakeup=initiative_wakeup,
        effect_wakeup=effect_wakeup,
    )

    assert await worker.drain_once() == 1
    assert repository.completed is not None
    assert repository.completed.next_cursor == {"offset": 3}
    assert initiative_wakeup.is_set()
    assert effect_wakeup.is_set()


@pytest.mark.asyncio
async def test_source_worker_records_safe_failure_for_unknown_source() -> None:
    record = subscription()

    class Repository:
        def __init__(self) -> None:
            self.error: str | None = None

        async def claim_due_subscriptions(self, _now, _limit, _lease_seconds):
            return [record] if self.error is None else []

        async def complete_source_poll(self, *_args):
            raise AssertionError("failed poll must not complete")

        async def mark_source_poll_failed(self, _id, _token, error, _now):
            self.error = error

    repository = Repository()
    worker = SourceWorker(
        repository=repository,
        dispatcher=SourceDispatcher({}),
        batch_size=1,
        source_wakeup=asyncio.Event(),
        initiative_wakeup=asyncio.Event(),
        effect_wakeup=asyncio.Event(),
    )

    assert await worker.drain_once() == 1
    assert repository.error == "ValueError"


@pytest.mark.asyncio
async def test_source_service_and_initiative_planner_wake_each_stage() -> None:
    record = subscription()

    class SourceRepository:
        async def create_subscription(self, _spec):
            return SourceCreateResult(subscription=record, created=True)

        async def disable_subscription(self, _subscription_id):
            return True

    source_wakeup = asyncio.Event()
    service = SourceService(
        repository=SourceRepository(),
        source_wakeup=source_wakeup,
    )
    result = await service.subscribe(
        SourceSubscriptionSpec(
            source="fake",
            dedupe_key="fake:1",
            session_id="telegram:1",
            conversation_id=1,
        )
    )
    assert result.created
    assert source_wakeup.is_set()

    work_repo = FakeWorkRepository()
    work = await work_repo.enqueue_work(
        WorkSpec(
            kind="proactive",
            action="agent",
            dedupe_key="source:1:item-3",
            session_id="telegram:1",
            conversation_id=1,
            profile="proactive",
            input_text="new item",
            priority=40,
        )
    )

    class InitiativeRepository:
        async def materialize_initiatives(self, kind, _now, _limit):
            assert kind == "proactive"
            return [work.work]

    work_wakeup = asyncio.Event()
    planner = InitiativePlanner(
        kind="proactive",
        repository=InitiativeRepository(),
        batch_size=10,
        initiative_wakeup=asyncio.Event(),
        work_wakeup=work_wakeup,
    )
    assert await planner.drain_once() == 1
    assert work_wakeup.is_set()
