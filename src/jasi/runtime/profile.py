from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from jasi.runtime.hooks import HookContext, HookSpec

logger = logging.getLogger(__name__)

_CORE_TOOLS = frozenset({"get_current_time", "tool_search"})
_WEB_TOOLS = frozenset({"web_search", "web_fetch"})
_FILE_READ_TOOLS = frozenset({"read_file", "list_dir"})
_MESSAGE_TOOLS = frozenset({"search_messages", "fetch_messages"})
_OPERATOR_TOOLS = frozenset({"write_file", "edit_file", "shell", "task_output", "task_stop"})


@dataclass(frozen=True)
class RuntimeProfile:
    name: str
    history_limit: int
    max_model_steps: int
    allowed_tools: frozenset[str]
    base_tools: frozenset[str]
    include_memory: bool = False
    hooks: list[HookSpec] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.base_tools <= self.allowed_tools:
            raise ValueError(f"profile base tools must be allowed: {self.name}")


def _tool_safety_guard(context: HookContext, payload: Any) -> None:
    tool_name = context.metadata.get("tool_name")
    allowed_tools = set(context.metadata.get("allowed_tools", []))
    if tool_name not in allowed_tools:
        raise ValueError(f"tool is not allowed: {tool_name}")


def _commit_audit_observer(context: HookContext, payload: Any) -> None:
    status = getattr(payload, "status", None)
    logger.info(
        "turn committed profile=%s turn_id=%s session=%s status=%s",
        context.profile,
        context.turn_id,
        context.session_id,
        status,
    )


def _default_hooks(profile: str) -> list[HookSpec]:
    return [
        HookSpec(
            phase="before_tool",
            kind="guard",
            name=f"{profile}_tool_allowlist",
            handler=_tool_safety_guard,
        ),
        HookSpec(
            phase="after_commit",
            kind="observer",
            name=f"{profile}_commit_audit",
            handler=_commit_audit_observer,
        ),
    ]


PASSIVE_PROFILE = RuntimeProfile(
    name="passive",
    history_limit=30,
    max_model_steps=4,
    allowed_tools=(_CORE_TOOLS | _WEB_TOOLS | _FILE_READ_TOOLS | _MESSAGE_TOOLS | _OPERATOR_TOOLS),
    base_tools=_CORE_TOOLS,
    include_memory=True,
    hooks=_default_hooks("passive"),
)


SCHEDULED_PROFILE = RuntimeProfile(
    name="scheduled",
    history_limit=30,
    max_model_steps=4,
    allowed_tools=_CORE_TOOLS | _WEB_TOOLS | _FILE_READ_TOOLS | _MESSAGE_TOOLS,
    base_tools=_CORE_TOOLS,
    hooks=_default_hooks("scheduled"),
)


PROACTIVE_PROFILE = RuntimeProfile(
    name="proactive",
    history_limit=30,
    max_model_steps=4,
    allowed_tools=_CORE_TOOLS | _WEB_TOOLS,
    base_tools=_CORE_TOOLS,
    hooks=_default_hooks("proactive"),
)


DRIFT_PROFILE = RuntimeProfile(
    name="drift",
    history_limit=30,
    max_model_steps=4,
    allowed_tools=_CORE_TOOLS | _WEB_TOOLS | _MESSAGE_TOOLS,
    base_tools=_CORE_TOOLS,
    hooks=_default_hooks("drift"),
)
