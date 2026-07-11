from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from jasi.application.drift import DriftOpportunityProducer
from jasi.application.source import InitiativePlanner
from jasi.domain.drift import DriftOfferResult, DriftOpportunityRecord, DriftOpportunitySpec
from jasi.domain.work import WorkSpec
from tests.unit.fakes import FakeWorkRepository


def opportunity_record() -> DriftOpportunityRecord:
    now = datetime.now(UTC)
    return DriftOpportunityRecord(
        id=1,
        dedupe_key="drift:topic:1",
        session_id="telegram:1",
        conversation_id=1,
        input_text="Ask about the topic naturally",
        available_at=now,
        expires_at=now + timedelta(hours=1),
        profile="drift",
        min_idle_seconds=60,
        cooldown_seconds=600,
        priority=20,
        payload={},
        status="new",
    )


def test_drift_opportunity_requires_stable_window_and_input() -> None:
    now = datetime.now(UTC)
    with pytest.raises(ValueError, match="input"):
        DriftOpportunitySpec(
            dedupe_key="drift:1",
            session_id="telegram:1",
            conversation_id=1,
            input_text=" ",
        )
    with pytest.raises(ValueError, match="after availability"):
        DriftOpportunitySpec(
            dedupe_key="drift:1",
            session_id="telegram:1",
            conversation_id=1,
            input_text="topic",
            available_at=now,
            expires_at=now,
        )


@pytest.mark.asyncio
async def test_drift_producer_persists_before_waking_planner() -> None:
    record = opportunity_record()

    class Repository:
        async def offer_drift(self, _spec):
            return DriftOfferResult(opportunity=record, created=True)

        async def dismiss_drift(self, _opportunity_id):
            return True

    wakeup = asyncio.Event()
    producer = DriftOpportunityProducer(
        repository=Repository(),
        initiative_wakeup=wakeup,
    )
    result = await producer.offer(
        DriftOpportunitySpec(
            dedupe_key="drift:topic:1",
            session_id="telegram:1",
            conversation_id=1,
            input_text="topic",
        )
    )

    assert result.created
    assert wakeup.is_set()


@pytest.mark.asyncio
async def test_shared_initiative_planner_materializes_drift_work() -> None:
    work_repo = FakeWorkRepository()
    queued = await work_repo.enqueue_work(
        WorkSpec(
            kind="drift",
            action="agent",
            dedupe_key="drift:1",
            session_id="telegram:1",
            conversation_id=1,
            profile="drift",
            input_text="topic",
            priority=20,
        )
    )

    class Repository:
        async def materialize_initiatives(self, kind, _now, _limit):
            assert kind == "drift"
            return [queued.work]

    work_wakeup = asyncio.Event()
    planner = InitiativePlanner(
        kind="drift",
        repository=Repository(),
        batch_size=5,
        initiative_wakeup=asyncio.Event(),
        work_wakeup=work_wakeup,
    )

    assert await planner.drain_once() == 1
    assert work_wakeup.is_set()
