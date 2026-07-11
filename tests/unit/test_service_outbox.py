from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest

from jasi.application.outbox import OutboxDispatcher, OutboxWorker
from jasi.application.passive_service import PassiveChatService
from jasi.domain.models import DeliveryResult, InboundMessage, OutboundPart
from jasi.runtime.models import ModelResponse, TurnResult
from tests.unit.fakes import FakeChannel, FakeModel, FakeOutboundPolicy, FakeRepository
from tests.unit.test_runtime import make_runtime


def inbound(update_id: str, chat_id: str = "1", text: str = "hello") -> InboundMessage:
    return InboundMessage(
        channel="telegram",
        external_update_id=update_id,
        external_chat_id=chat_id,
        external_user_id="42",
        text=text,
    )


async def create_response(
    repo: FakeRepository,
    *,
    channel: str = "telegram",
) -> tuple:
    update_id = f"outbox-{len(repo.inbound_events) + 1}"
    claim = await repo.claim_inbound_message(
        InboundMessage(
            channel=channel,
            external_update_id=update_id,
            external_chat_id="1",
            external_user_id="42",
            text="request",
        )
    )
    assert claim is not None
    return await repo.complete_inbound_response(
        inbound_event_id=claim.event_id,
        conversation_id=claim.conversation.id,
        channel=channel,
        external_chat_id="1",
        turn_id=len(repo.turns) + 1,
        text="reply",
        parts=(OutboundPart(text="reply"),),
        origin="model",
        metadata={},
    )


@pytest.mark.asyncio
async def test_passive_service_skips_duplicate_update_without_reexecuting_model() -> None:
    repo = FakeRepository()
    channel = FakeChannel()
    wakeup = asyncio.Event()
    dispatcher = OutboxDispatcher(repository=repo, channels={"telegram": channel})
    model = FakeModel([ModelResponse(content="reply")])
    service = PassiveChatService(
        repository=repo,
        runtime=make_runtime(model, repo),
        outbound_policies={"telegram": FakeOutboundPolicy()},
        outbox_wakeup=wakeup,
    )

    await service.handle(inbound("100"))
    await service.handle(inbound("100"))

    assert len(model.requests) == 1
    assert wakeup.is_set()
    assert channel.sent == []

    worker = OutboxWorker(
        repository=repo,
        dispatcher=dispatcher,
        batch_size=10,
        wakeup=wakeup,
    )
    assert await worker.drain_once() == 1
    assert len(channel.sent) == 1
    assistant_messages = [row for row in repo.messages if row.role == "assistant"]
    assert assistant_messages[0].delivery_status == "sent"


@pytest.mark.asyncio
async def test_incomplete_response_reuses_committed_turn_without_reexecuting_model() -> None:
    repo = FakeRepository()
    repo.fail_next_completion = True
    model = FakeModel([ModelResponse(content="reply")])
    service = PassiveChatService(
        repository=repo,
        runtime=make_runtime(model, repo),
        outbound_policies={"telegram": FakeOutboundPolicy()},
        outbox_wakeup=asyncio.Event(),
    )
    message = inbound("recoverable")

    with pytest.raises(RuntimeError, match="simulated response transaction failure"):
        await service.handle(message)

    event = repo.inbound_events[("telegram", "recoverable")]
    assert event["status"] == "pending"
    assert len(model.requests) == 1
    assert not [row for row in repo.messages if row.role == "assistant"]

    await service.handle(message)
    await service.handle(message)

    assert event["status"] == "completed"
    assert event["attempts"] == 2
    assert len(model.requests) == 1
    assert len([row for row in repo.messages if row.role == "assistant"]) == 1
    assert len(repo.outbox) == 1


class SlowRuntime:
    def __init__(self) -> None:
        self.active = 0
        self.max_active = 0

    async def run(self, _request) -> TurnResult:
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0.05)
        self.active -= 1
        return TurnResult(turn_id=1, status="succeeded", final_text="ok")


@pytest.mark.asyncio
async def test_same_session_is_serialized() -> None:
    repo = FakeRepository()
    runtime = SlowRuntime()
    service = PassiveChatService(
        repository=repo,
        runtime=runtime,
        outbound_policies={"telegram": FakeOutboundPolicy()},
        outbox_wakeup=asyncio.Event(),
    )

    await asyncio.gather(service.handle(inbound("1")), service.handle(inbound("2")))

    assert runtime.max_active == 1


@pytest.mark.asyncio
async def test_different_sessions_can_run_concurrently() -> None:
    repo = FakeRepository()
    runtime = SlowRuntime()
    service = PassiveChatService(
        repository=repo,
        runtime=runtime,
        outbound_policies={"telegram": FakeOutboundPolicy()},
        outbox_wakeup=asyncio.Event(),
    )

    await asyncio.gather(
        service.handle(inbound("1", chat_id="1")),
        service.handle(inbound("2", chat_id="2")),
    )

    assert runtime.max_active == 2


@pytest.mark.asyncio
async def test_outbox_retry_and_exhaustion_do_not_rerun_runtime() -> None:
    repo = FakeRepository()
    _, records = await create_response(repo)
    channel = FakeChannel([DeliveryResult(success=False, error="temporary", retryable=True)])
    dispatcher = OutboxDispatcher(repository=repo, channels={"telegram": channel})

    claimed = await repo.claim_outbox_batch(1)
    await dispatcher.deliver(claimed[0])

    first = await repo.get_outbox(records[0].id)
    assert first is not None
    assert first.status == "pending"
    assert first.attempts == 1

    channel.results = [DeliveryResult(success=False, error="bad request", retryable=False)]
    ready_again = replace(first, next_attempt_at=first.next_attempt_at - timedelta(seconds=3))
    repo.outbox[ready_again.id] = ready_again
    claimed = await repo.claim_outbox_batch(1)
    await dispatcher.deliver(claimed[0])

    failed = await repo.get_outbox(records[0].id)
    assert failed is not None
    assert failed.status == "failed"
    assert failed.attempts == 2


@pytest.mark.asyncio
async def test_outbox_worker_claims_pending_messages() -> None:
    repo = FakeRepository()
    _, records = await create_response(repo)
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
async def test_outbox_worker_wakes_immediately_for_new_work() -> None:
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
        await create_response(repo)
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
    await create_response(repo, channel="feishu")
    telegram = FakeChannel()
    feishu = FakeChannel()
    dispatcher = OutboxDispatcher(
        repository=repo,
        channels={"telegram": telegram, "feishu": feishu},
    )
    worker = OutboxWorker(
        repository=repo,
        dispatcher=dispatcher,
        batch_size=10,
        wakeup=asyncio.Event(),
    )

    assert await worker.drain_once() == 1

    assert telegram.sent == []
    assert len(feishu.sent) == 1
