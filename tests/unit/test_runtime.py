from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from jasi.application.context import TurnContextProvider
from jasi.ports.model import ModelPort
from jasi.runtime.hooks import HookSpec
from jasi.runtime.models import ModelResponse, ToolCall, TurnRequest
from jasi.runtime.profile import PASSIVE_PROFILE, SCHEDULED_PROFILE
from jasi.runtime.prompting import PromptAssembler, PromptCatalog
from jasi.runtime.runtime import FIXED_ERROR_REPLY, AgentRuntime
from jasi.tools.registry import ToolRegistry
from jasi.tools.time import get_current_time_tool
from tests.unit.fakes import FakeModel, FakeRepository


def make_runtime(
    model: ModelPort,
    repo: FakeRepository,
    profile=PASSIVE_PROFILE,
    model_timeout_seconds: float = 5,
) -> AgentRuntime:
    catalog = PromptCatalog(
        persona="You are Jasi.",
        profiles={
            "passive": "Handle a passive conversation.",
            "scheduled": "Execute a scheduled instruction.",
        },
    )
    return AgentRuntime(
        profiles={profile.name: profile},
        model=model,
        repository=repo,
        context_provider=TurnContextProvider(repository=repo),
        prompt_assembler=PromptAssembler(catalog),
        tools=ToolRegistry([get_current_time_tool]),
        model_name="test-model",
        model_timeout_seconds=model_timeout_seconds,
        timezone="Asia/Shanghai",
    )


def make_request(text: str = "current") -> TurnRequest:
    return TurnRequest(
        work_id=2,
        session_id="telegram:1",
        conversation_id=1,
        input_text=text,
        profile="passive",
        history_before_sequence=2,
    )


@pytest.mark.asyncio
async def test_runtime_plain_text_turn_excludes_current_message_from_history() -> None:
    repo = FakeRepository()
    repo.add_message(role="user", content="old", sequence=1)
    repo.add_message(role="user", content="current should not appear", sequence=2)
    model = FakeModel([ModelResponse(content="hello")])

    result = await make_runtime(model, repo).run(make_request("current"))

    assert result.status == "succeeded"
    assert result.final_text == "hello"
    user_messages = [m.content for m in model.requests[0].messages if m.role == "user"]
    assert user_messages == ["old", "current"]
    assert repo.turns[result.turn_id]["status"] == "succeeded"


@pytest.mark.asyncio
async def test_runtime_can_load_latest_history_for_internal_agent_work() -> None:
    repo = FakeRepository()
    repo.add_message(role="user", content="latest user", sequence=3)
    repo.add_message(role="assistant", content="delivered", sequence=4)
    model = FakeModel([ModelResponse(content="hello")])
    request = replace(
        make_request("internal context"),
        history_before_sequence=None,
    )

    await make_runtime(model, repo).run(request)

    messages = [(item.role, item.content) for item in model.requests[0].messages]
    assert messages[-3:] == [
        ("user", "latest user"),
        ("assistant", "delivered"),
        ("user", "internal context"),
    ]


@pytest.mark.asyncio
async def test_runtime_selects_profile_without_changing_execution_flow() -> None:
    repo = FakeRepository()
    model = FakeModel([ModelResponse(content="scheduled reply")])
    runtime = AgentRuntime(
        profiles={
            PASSIVE_PROFILE.name: PASSIVE_PROFILE,
            SCHEDULED_PROFILE.name: SCHEDULED_PROFILE,
        },
        model=model,
        repository=repo,
        context_provider=TurnContextProvider(repository=repo),
        prompt_assembler=PromptAssembler(
            PromptCatalog(
                persona="You are Jasi.",
                profiles={
                    "passive": "Handle a passive conversation.",
                    "scheduled": "Execute a scheduled instruction.",
                },
            )
        ),
        tools=ToolRegistry([get_current_time_tool]),
        model_name="test-model",
        model_timeout_seconds=5,
        timezone="Asia/Shanghai",
    )

    result = await runtime.run(
        replace(
            make_request("prepare the reminder"),
            work_id=3,
            profile="scheduled",
            history_before_sequence=None,
        )
    )

    assert result.final_text == "scheduled reply"
    assert "scheduled instruction" in (model.requests[0].messages[0].content or "")


@pytest.mark.asyncio
async def test_runtime_tool_call_then_final_reply() -> None:
    repo = FakeRepository()
    model = FakeModel(
        [
            ModelResponse(
                tool_calls=[
                    ToolCall(id="call_1", name="get_current_time", arguments={"timezone": "UTC"})
                ]
            ),
            ModelResponse(content="It is time."),
        ]
    )

    result = await make_runtime(model, repo).run(make_request("what time is it?"))

    assert result.status == "succeeded"
    assert result.final_text == "It is time."
    assert len(model.requests) == 2
    assert model.requests[1].messages[-1].role == "tool"
    assert repo.tool_records[0][1].name == "get_current_time"
    assert repo.tool_records[0][1].status == "succeeded"


@pytest.mark.asyncio
async def test_runtime_fails_when_tool_requested_at_step_limit() -> None:
    repo = FakeRepository()
    profile = replace(PASSIVE_PROFILE, max_model_steps=1)
    model = FakeModel(
        [ModelResponse(tool_calls=[ToolCall(id="call_1", name="get_current_time", arguments={})])]
    )

    result = await make_runtime(model, repo, profile=profile).run(make_request())

    assert result.status == "failed"
    assert result.final_text == FIXED_ERROR_REPLY
    assert result.error_code == "max_steps_exceeded"
    assert repo.turns[result.turn_id]["status"] == "failed"


@pytest.mark.asyncio
async def test_runtime_rejects_illegal_tool_and_records_it() -> None:
    repo = FakeRepository()
    model = FakeModel(
        [ModelResponse(tool_calls=[ToolCall(id="call_1", name="delete_everything", arguments={})])]
    )

    result = await make_runtime(model, repo).run(make_request())

    assert result.status == "failed"
    assert result.error_code == "hook_guard_rejected"
    assert repo.tool_records[0][1].name == "delete_everything"
    assert repo.tool_records[0][1].status == "rejected"


@pytest.mark.asyncio
async def test_observer_hook_failure_does_not_fail_turn() -> None:
    repo = FakeRepository()
    events: list[str] = []

    def bad_observer(_context, _payload) -> None:
        events.append("after_commit")
        raise RuntimeError("audit sink down")

    profile = replace(
        PASSIVE_PROFILE,
        hooks=[HookSpec("after_commit", "observer", "bad_observer", bad_observer)],
    )
    model = FakeModel([ModelResponse(content="ok")])

    result = await make_runtime(model, repo, profile=profile).run(make_request())

    assert result.status == "succeeded"
    assert events == ["after_commit"]
    assert repo.turns[result.turn_id]["status"] == "succeeded"


@pytest.mark.asyncio
async def test_guard_and_transform_hook_failures_mark_turn_failed() -> None:
    def guard(_context, _payload) -> None:
        raise RuntimeError("no")

    def transform(_context, _payload) -> None:
        raise RuntimeError("bad transform")

    for hook, expected_code in [
        (HookSpec("before_model", "guard", "guard", guard), "hook_guard_rejected"),
        (HookSpec("before_model", "transform", "transform", transform), "hook_transform_failed"),
    ]:
        repo = FakeRepository()
        model = FakeModel([ModelResponse(content="unused")])
        profile = replace(PASSIVE_PROFILE, hooks=[hook])

        result = await make_runtime(model, repo, profile=profile).run(make_request())

        assert result.status == "failed"
        assert result.error_code == expected_code
        assert model.requests == []


@pytest.mark.asyncio
async def test_runtime_enforces_model_timeout_for_any_adapter() -> None:
    class SlowModel:
        async def complete(self, _request):
            await asyncio.sleep(1)
            return ModelResponse(content="too late")

    repo = FakeRepository()
    runtime = make_runtime(SlowModel(), repo, model_timeout_seconds=0.01)

    result = await runtime.run(make_request())

    assert result.status == "failed"
    assert result.error_code == "model_timeout"
    assert result.final_text == FIXED_ERROR_REPLY
