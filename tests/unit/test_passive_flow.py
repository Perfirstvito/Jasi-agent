from __future__ import annotations

import asyncio

import pytest

from jasi.application.agent_work import AgentWorkHandler
from jasi.application.passive_service import PassiveIngressService
from jasi.application.work import WorkDispatcher, WorkFinalizer, WorkWorker
from jasi.domain.models import InboundMessage
from jasi.domain.work import WorkEnqueueResult, WorkSpec
from jasi.runtime.models import ModelResponse
from tests.unit.fakes import (
    FakeModel,
    FakeOutboundPolicy,
    FakeRepository,
    FakeWorkRepository,
)
from tests.unit.test_runtime import make_runtime


def inbound(update_id: str = "100") -> InboundMessage:
    return InboundMessage(
        channel="telegram",
        external_update_id=update_id,
        external_chat_id="1",
        external_user_id="42",
        text="hello",
    )


@pytest.mark.asyncio
async def test_passive_ingress_only_persists_and_wakes_work_worker() -> None:
    work_repo = FakeWorkRepository()
    queued = await work_repo.enqueue_work(
        WorkSpec(
            kind="passive",
            action="agent",
            dedupe_key="inbound:telegram:100",
            session_id="telegram:1",
            conversation_id=1,
            profile="passive",
            input_text="hello",
            priority=100,
        )
    )

    class IngressRepository:
        def __init__(self) -> None:
            self.calls = 0

        async def enqueue_passive(self, _message: InboundMessage):
            self.calls += 1
            return WorkEnqueueResult(work=queued.work, created=self.calls == 1)

    repository = IngressRepository()
    wakeup = asyncio.Event()
    service = PassiveIngressService(repository=repository, work_wakeup=wakeup)

    await service.handle(inbound())
    await service.handle(inbound())

    assert repository.calls == 2
    assert wakeup.is_set()


@pytest.mark.asyncio
async def test_finalizer_retry_reuses_committed_turn_without_reexecuting_model() -> None:
    work_repo = FakeWorkRepository()
    queued = await work_repo.enqueue_work(
        WorkSpec(
            kind="passive",
            action="agent",
            dedupe_key="inbound:telegram:recoverable",
            session_id="telegram:1",
            conversation_id=1,
            profile="passive",
            input_text="hello",
            payload={"history_before_sequence": 1},
            priority=100,
        )
    )
    runtime_repo = FakeRepository()
    runtime_repo.add_conversation()
    runtime_repo.add_message(
        role="user",
        origin="telegram",
        content="hello",
        sequence=1,
    )
    model = FakeModel([ModelResponse(content="reply")])
    work_repo.fail_next_completion = True
    outbox_wakeup = asyncio.Event()
    worker = WorkWorker(
        repository=work_repo,
        dispatcher=WorkDispatcher(
            {
                "agent": AgentWorkHandler(
                    runtime=make_runtime(model, runtime_repo),
                    conversations=runtime_repo,
                )
            }
        ),
        finalizer=WorkFinalizer(
            repository=work_repo,
            outbound_policies={"telegram": FakeOutboundPolicy()},
            outbox_wakeup=outbox_wakeup,
        ),
        batch_size=1,
        wakeup=asyncio.Event(),
    )

    assert await worker.drain_once() == 1
    assert work_repo.work[queued.work.id].status == "pending"
    assert len(model.requests) == 1
    assert outbox_wakeup.is_set() is False

    work_repo.make_ready(queued.work.id)
    assert await worker.drain_once() == 1

    assert len(model.requests) == 1
    assert work_repo.work[queued.work.id].status == "succeeded"
    completion = work_repo.completions[queued.work.id]
    assert completion.message is not None
    assert completion.message.content == "reply"
    assert len(completion.outbox) == 1
    assert outbox_wakeup.is_set()
