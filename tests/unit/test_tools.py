from __future__ import annotations

from typing import Any

import pytest

from jasi.runtime.errors import ToolFailure, ToolRejected
from jasi.tools.builtin import build_builtin_tool_registry
from jasi.tools.registry import ToolExecutionContext, ToolRegistry, ToolSpec


def context(
    *,
    allowed: frozenset[str] = frozenset({"sample"}),
    visible: frozenset[str] = frozenset({"sample"}),
) -> ToolExecutionContext:
    return ToolExecutionContext(
        work_id=1,
        session_id="telegram:1",
        conversation_id=1,
        profile="passive",
        timezone="Asia/Shanghai",
        allowed_tools=allowed,
        visible_tools=visible,
    )


def sample_tool(
    *,
    name: str = "sample",
    handler=None,
    parameters: dict[str, Any] | None = None,
) -> ToolSpec:
    return ToolSpec(
        name=name,
        description=f"Read sample data with {name}.",
        parameters=parameters
        or {
            "type": "object",
            "properties": {"value": {"type": "integer"}},
            "required": ["value"],
            "additionalProperties": False,
        },
        risk="read-only",
        handler=handler or (lambda arguments, _context: {"value": arguments["value"]}),
        search_terms=("sample data",),
    )


def test_registry_rejects_duplicate_names_and_invalid_schemas() -> None:
    registry = ToolRegistry([sample_tool()])

    with pytest.raises(ValueError, match="duplicate tool"):
        registry.register(sample_tool())

    with pytest.raises(ValueError, match="object schema"):
        sample_tool(parameters={"type": "array"})

    with pytest.raises(ValueError, match="invalid tool schema"):
        sample_tool(parameters={"type": "object", "properties": "invalid"})


@pytest.mark.asyncio
async def test_registry_validates_arguments_before_calling_handler() -> None:
    calls: list[dict[str, Any]] = []
    registry = ToolRegistry(
        [sample_tool(handler=lambda arguments, _context: calls.append(arguments) or {})]
    )

    with pytest.raises(ToolRejected, match="invalid arguments"):
        await registry.execute("sample", {"value": "not-an-integer"}, context())

    assert calls == []


@pytest.mark.asyncio
async def test_registry_hides_handler_error_details() -> None:
    def fail(_arguments, _context):
        raise RuntimeError("secret from external system")

    registry = ToolRegistry([sample_tool(handler=fail)])

    with pytest.raises(ToolFailure) as raised:
        await registry.execute("sample", {"value": 1}, context())

    assert str(raised.value) == "sample failed"
    assert "secret" not in str(raised.value)


@pytest.mark.asyncio
async def test_tool_search_only_reveals_authorized_hidden_tools() -> None:
    registry = build_builtin_tool_registry()
    registry.register(sample_tool(name="list_issues"))
    registry.register(sample_tool(name="close_issue"))
    allowed = frozenset({"tool_search", "list_issues"})

    outcome = await registry.execute(
        "tool_search",
        {"query": "issues"},
        context(allowed=allowed, visible=frozenset({"tool_search"})),
    )

    assert outcome.reveal_tools == ("list_issues",)
    assert [item["name"] for item in outcome.content["matched"]] == ["list_issues"]


@pytest.mark.asyncio
async def test_tool_search_matches_chinese_alias_inside_a_natural_phrase() -> None:
    registry = build_builtin_tool_registry()
    allowed = frozenset({"tool_search", "shell"})

    outcome = await registry.execute(
        "tool_search",
        {"query": "帮我运行一个命令"},
        context(allowed=allowed, visible=frozenset({"tool_search"})),
    )

    assert outcome.reveal_tools == ("shell",)
