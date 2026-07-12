from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest

from jasi.application.outbox import OutboxDispatcher, OutboxWorker
from jasi.domain.models import DeliveryResult, OutboundPart
from tests.unit.fakes import FakeChannel, FakeRepository


@pytest.mark.asyncio
async def test_outbox_retry_and_exhaustion() -> None:
    repo = FakeRepository()
    _, records = repo.create_outbox_response()
    channel = FakeChannel([DeliveryResult(success=False, error="temporary", retryable=True)])
    dispatcher = OutboxDispatcher(repository=repo, channels={"telegram": channel})

    claimed = await repo.claim_outbox_batch(1)
    await dispatcher.deliver(claimed[0])

    first = await repo.get_outbox(records[0].id)
    assert first is not None
    assert first.status == "pending"
    assert first.attempts == 1

    channel.results = [DeliveryResult(success=False, error="bad request", retryable=False)]
    repo.outbox[first.id] = replace(
        first,
        next_attempt_at=first.next_attempt_at - timedelta(seconds=3),
    )
    claimed = await repo.claim_outbox_batch(1)
    await dispatcher.deliver(claimed[0])

    failed = await repo.get_outbox(records[0].id)
    assert failed is not None
    assert failed.status == "failed"
    assert failed.attempts == 2


@pytest.mark.asyncio
async def test_outbox_worker_claims_pending_messages() -> None:
    repo = FakeRepository()
    _, records = repo.create_outbox_response()
    channel = FakeChannel()
    worker = OutboxWorker(
        repository=repo,
        dispatcher=OutboxDispatcher(repository=repo, channels={"telegram": channel}),
        batch_size=10,
        wakeup=asyncio.Event(),
    )

    assert await worker.drain_once() == 1
    sent = await repo.get_outbox(records[0].id)
    assert sent is not None
    assert sent.status == "sent"


@pytest.mark.asyncio
async def test_outbox_worker_wakes_immediately_for_new_messages() -> None:
    repo = FakeRepository()
    channel = FakeChannel()
    wakeup = asyncio.Event()
    stop_event = asyncio.Event()
    worker = OutboxWorker(
        repository=repo,
        dispatcher=OutboxDispatcher(repository=repo, channels={"telegram": channel}),
        batch_size=10,
        wakeup=wakeup,
        idle_sleep_seconds=60,
    )
    task = asyncio.create_task(worker.run(stop_event))
    await asyncio.sleep(0)

    try:
        repo.create_outbox_response()
        wakeup.set()

        await asyncio.wait_for(channel.sent_event.wait(), timeout=1)
        assert len(channel.sent) == 1
    finally:
        stop_event.set()
        wakeup.set()
        await task


@pytest.mark.asyncio
async def test_dispatcher_routes_each_record_to_its_channel() -> None:
    repo = FakeRepository()
    repo.create_outbox_response(channel="feishu")
    telegram = FakeChannel()
    feishu = FakeChannel()
    worker = OutboxWorker(
        repository=repo,
        dispatcher=OutboxDispatcher(
            repository=repo,
            channels={"telegram": telegram, "feishu": feishu},
        ),
        batch_size=10,
        wakeup=asyncio.Event(),
    )

    assert await worker.drain_once() == 1
    assert telegram.sent == []
    assert len(feishu.sent) == 1


@pytest.mark.asyncio
async def test_segments_are_delivered_in_order() -> None:
    repo = FakeRepository()
    _, records = repo.create_outbox_response(
        text="firstsecond",
        parts=(OutboundPart("first"), OutboundPart("second")),
    )
    channel = FakeChannel()
    dispatcher = OutboxDispatcher(repository=repo, channels={"telegram": channel})

    first_claim = await repo.claim_outbox_batch(10)
    assert [row.id for row in first_claim] == [records[0].id]
    await dispatcher.deliver(first_claim[0])

    second_claim = await repo.claim_outbox_batch(10)
    assert [row.id for row in second_claim] == [records[1].id]
    await dispatcher.deliver(second_claim[0])
    assert [message.text for message in channel.sent] == ["first", "second"]


@pytest.mark.asyncio
async def test_terminal_segment_failure_cancels_remaining_segments() -> None:
    repo = FakeRepository()
    message, records = repo.create_outbox_response(
        text="firstsecond",
        parts=(OutboundPart("first"), OutboundPart("second")),
    )
    channel = FakeChannel([DeliveryResult(success=False, error="rejected", retryable=False)])
    dispatcher = OutboxDispatcher(repository=repo, channels={"telegram": channel})

    claimed = await repo.claim_outbox_batch(10)
    await dispatcher.deliver(claimed[0])

    assert repo.outbox[records[0].id].status == "failed"
    assert repo.outbox[records[1].id].status == "failed"
    stored_message = next(row for row in repo.messages if row.id == message.id)
    assert stored_message.delivery_status == "failed"
    assert await repo.claim_outbox_batch(10) == []
