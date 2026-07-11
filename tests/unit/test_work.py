from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from jasi.application.work import WorkDispatcher, WorkFinalizer, WorkWorker
from jasi.domain.models import OutboundPart
from jasi.domain.work import (
    OutboundDraft,
    WorkExecutionResult,
    WorkLeaseLost,
    WorkRecord,
    WorkSpec,
)
from tests.unit.fakes import FakeWorkRepository


def spec(
    dedupe_key: str,
    *,
    session_id: str = "telegram:1",
    kind: str = "passive",
    action: str = "agent",
    priority: int = 100,
    max_attempts: int = 5,
) -> WorkSpec:
    return WorkSpec(
        kind=kind,
        action=action,
        dedupe_key=dedupe_key,
        session_id=session_id,
        conversation_id=1,
        profile="passive" if action == "agent" else None,
        input_text="hello",
        priority=priority,
        max_attempts=max_attempts,
    )


class ResultHandler:
    def __init__(self, result: WorkExecutionResult | None = None) -> None:
        self.result = result or WorkExecutionResult()
        self.records: list[WorkRecord] = []

    async def execute(self, work: WorkRecord) -> WorkExecutionResult:
        self.records.append(work)
        return self.result


@pytest.mark.asyncio
async def test_enqueue_is_idempotent_and_claims_priority_once_per_session() -> None:
    repo = FakeWorkRepository()
    first = await repo.enqueue_work(spec("same", priority=10))
    duplicate = await repo.enqueue_work(spec("same", priority=999))
    high = await repo.enqueue_work(spec("high", priority=100))
    other = await repo.enqueue_work(spec("other", session_id="telegram:2", priority=50))

    claimed = await repo.claim_work_batch(limit=10, lease_seconds=60)

    assert first.created is True
    assert duplicate.created is False
    assert duplicate.work.id == first.work.id
    assert [row.id for row in claimed] == [high.work.id, other.work.id]
    assert repo.work[first.work.id].status == "pending"


@pytest.mark.asyncio
async def test_dispatcher_routes_by_action_not_trigger_kind() -> None:
    agent = ResultHandler()
    direct = ResultHandler()
    dispatcher = WorkDispatcher({"agent": agent, "direct": direct})
    repo = FakeWorkRepository()
    proactive = await repo.enqueue_work(
        spec("proactive", kind="proactive", action="agent", priority=40)
    )
    scheduled = await repo.enqueue_work(
        spec("scheduled", kind="scheduled", action="direct", priority=90)
    )

    await dispatcher.execute(proactive.work)
    await dispatcher.execute(scheduled.work)

    assert [row.kind for row in agent.records] == ["proactive"]
    assert [row.kind for row in direct.records] == ["scheduled"]


@pytest.mark.asyncio
async def test_worker_serializes_same_session_and_runs_other_sessions_concurrently() -> None:
    class SlowHandler:
        def __init__(self) -> None:
            self.active = 0
            self.max_active = 0

        async def execute(self, _work: WorkRecord) -> WorkExecutionResult:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            await asyncio.sleep(0.02)
            self.active -= 1
            return WorkExecutionResult()

    repo = FakeWorkRepository()
    same_first = await repo.enqueue_work(spec("same-1"))
    same_second = await repo.enqueue_work(spec("same-2", priority=90))
    other = await repo.enqueue_work(spec("other", session_id="telegram:2"))
    handler = SlowHandler()
    worker = WorkWorker(
        repository=repo,
        dispatcher=WorkDispatcher({"agent": handler}),
        finalizer=WorkFinalizer(
            repository=repo,
            outbound_policies={},
            outbox_wakeup=asyncio.Event(),
        ),
        batch_size=10,
        wakeup=asyncio.Event(),
    )

    assert await worker.drain_once() == 2
    assert handler.max_active == 2
    assert repo.work[same_first.work.id].status == "succeeded"
    assert repo.work[same_second.work.id].status == "pending"
    assert repo.work[other.work.id].status == "succeeded"

    assert await worker.drain_once() == 1
    assert repo.work[same_second.work.id].status == "succeeded"


@pytest.mark.asyncio
async def test_worker_retries_then_exhausts_without_losing_error_state() -> None:
    class BrokenHandler:
        async def execute(self, _work: WorkRecord) -> WorkExecutionResult:
            raise RuntimeError("secret details must not be stored")

    repo = FakeWorkRepository()
    queued = await repo.enqueue_work(spec("broken", max_attempts=2))
    worker = WorkWorker(
        repository=repo,
        dispatcher=WorkDispatcher({"agent": BrokenHandler()}),
        finalizer=WorkFinalizer(
            repository=repo,
            outbound_policies={},
            outbox_wakeup=asyncio.Event(),
        ),
        batch_size=1,
        wakeup=asyncio.Event(),
    )

    assert await worker.drain_once() == 1
    first = repo.work[queued.work.id]
    assert first.status == "pending"
    assert first.attempts == 1
    assert first.last_error == "RuntimeError"

    repo.make_ready(queued.work.id)
    assert await worker.drain_once() == 1
    exhausted = repo.work[queued.work.id]
    assert exhausted.status == "failed"
    assert exhausted.attempts == 2
    assert exhausted.completed_at is not None


@pytest.mark.asyncio
async def test_stale_lease_is_reclaimed_and_old_token_is_fenced() -> None:
    repo = FakeWorkRepository()
    queued = await repo.enqueue_work(spec("stale"))
    first = (await repo.claim_work_batch(1, lease_seconds=60))[0]
    repo.work[first.id] = replace(
        repo.work[first.id],
        lease_until=datetime.now(UTC) - timedelta(seconds=1),
    )

    second = (await repo.claim_work_batch(1, lease_seconds=60))[0]

    assert second.id == queued.work.id
    assert second.lease_token != first.lease_token
    with pytest.raises(WorkLeaseLost):
        await repo.complete_work(first.id, first.lease_token or "", WorkExecutionResult(), ())
    await repo.complete_work(second.id, second.lease_token or "", WorkExecutionResult(), ())
    assert repo.work[second.id].status == "succeeded"


@pytest.mark.asyncio
async def test_finalizer_prepares_channel_parts_and_persists_outbox() -> None:
    class TwoPartPolicy:
        def prepare(self, text: str) -> tuple[OutboundPart, ...]:
            midpoint = len(text) // 2
            return (OutboundPart(text[:midpoint]), OutboundPart(text[midpoint:]))

    repo = FakeWorkRepository()
    queued = await repo.enqueue_work(spec("outbound"))
    claimed = (await repo.claim_work_batch(1, lease_seconds=60))[0]
    wakeup = asyncio.Event()
    finalizer = WorkFinalizer(
        repository=repo,
        outbound_policies={"telegram": TwoPartPolicy()},
        outbox_wakeup=wakeup,
    )

    await finalizer.complete(
        claimed,
        WorkExecutionResult(
            turn_id=7,
            outbound=OutboundDraft(
                channel="telegram",
                external_chat_id="1",
                text="abcdefgh",
                origin="model",
                metadata={"work_id": -1},
            ),
        ),
    )

    completion = repo.completions[queued.work.id]
    assert completion.message is not None
    assert completion.message.metadata["work_id"] == queued.work.id
    assert [row.text for row in completion.outbox] == ["abcd", "efgh"]
    assert wakeup.is_set()


@pytest.mark.asyncio
async def test_worker_renews_lease_while_handler_is_running() -> None:
    class SlowHandler:
        async def execute(self, _work: WorkRecord) -> WorkExecutionResult:
            await asyncio.sleep(0.08)
            return WorkExecutionResult()

    repo = FakeWorkRepository()
    queued = await repo.enqueue_work(
        spec("heartbeat", kind="proactive", priority=40)
    )
    worker = WorkWorker(
        repository=repo,
        dispatcher=WorkDispatcher({"agent": SlowHandler()}),
        finalizer=WorkFinalizer(
            repository=repo,
            outbound_policies={},
            outbox_wakeup=asyncio.Event(),
        ),
        batch_size=1,
        wakeup=asyncio.Event(),
        lease_seconds=0.06,
        heartbeat_seconds=0.015,
    )

    assert await worker.drain_once() == 1
    assert repo.work[queued.work.id].status == "succeeded"
    assert len(repo.renew_calls) >= 2


@pytest.mark.asyncio
async def test_lease_loss_cancels_stale_handler_before_finalization() -> None:
    cancelled = asyncio.Event()

    class BlockingHandler:
        async def execute(self, _work: WorkRecord) -> WorkExecutionResult:
            try:
                await asyncio.sleep(10)
            finally:
                cancelled.set()

    repo = FakeWorkRepository()
    queued = await repo.enqueue_work(spec("lost-lease"))
    repo.fail_lease_renewal = True
    worker = WorkWorker(
        repository=repo,
        dispatcher=WorkDispatcher({"agent": BlockingHandler()}),
        finalizer=WorkFinalizer(
            repository=repo,
            outbound_policies={},
            outbox_wakeup=asyncio.Event(),
        ),
        batch_size=1,
        wakeup=asyncio.Event(),
        lease_seconds=0.06,
        heartbeat_seconds=0.01,
    )

    assert await worker.drain_once() == 1
    assert cancelled.is_set()
    assert repo.work[queued.work.id].status == "running"
    assert queued.work.id not in repo.completions


@pytest.mark.asyncio
async def test_running_background_work_does_not_block_new_passive_claims() -> None:
    background_started = asyncio.Event()
    release_background = asyncio.Event()
    passive_finished = asyncio.Event()

    class LaneHandler:
        def __init__(self) -> None:
            self.background_count = 0

        async def execute(self, work: WorkRecord) -> WorkExecutionResult:
            if work.kind == "passive":
                passive_finished.set()
                return WorkExecutionResult()
            self.background_count += 1
            if self.background_count == 2:
                background_started.set()
            await release_background.wait()
            return WorkExecutionResult()

    repo = FakeWorkRepository()
    await repo.enqueue_work(
        spec("background-1", kind="proactive", session_id="telegram:1", priority=40)
    )
    await repo.enqueue_work(
        spec("background-2", kind="drift", session_id="telegram:2", priority=20)
    )
    handler = LaneHandler()
    wakeup = asyncio.Event()
    stop_event = asyncio.Event()
    worker = WorkWorker(
        repository=repo,
        dispatcher=WorkDispatcher({"agent": handler}),
        finalizer=WorkFinalizer(
            repository=repo,
            outbound_policies={},
            outbox_wakeup=asyncio.Event(),
        ),
        batch_size=3,
        wakeup=wakeup,
        background_limit=2,
        idle_sleep_seconds=1,
    )
    task = asyncio.create_task(worker.run(stop_event))

    try:
        await asyncio.wait_for(background_started.wait(), timeout=1)
        await repo.enqueue_work(
            spec("passive", session_id="telegram:3", kind="passive", priority=100)
        )
        wakeup.set()

        await asyncio.wait_for(passive_finished.wait(), timeout=0.5)
    finally:
        release_background.set()
        stop_event.set()
        wakeup.set()
        await task
