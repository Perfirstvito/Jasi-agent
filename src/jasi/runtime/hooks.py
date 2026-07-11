from __future__ import annotations

import inspect
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Literal

from jasi.runtime.errors import HookGuardRejected, HookTransformFailed

logger = logging.getLogger(__name__)

HookPhase = Literal[
    "before_model",
    "after_model",
    "before_tool",
    "after_tool",
    "before_commit",
    "after_commit",
]
HookKind = Literal["guard", "transform", "observer"]


@dataclass(frozen=True)
class HookContext:
    session_id: str
    turn_id: int
    profile: str
    metadata: dict[str, Any] = field(default_factory=dict)


HookCallable = Callable[[HookContext, Any], Any | Awaitable[Any]]


@dataclass(frozen=True)
class HookSpec:
    phase: HookPhase
    kind: HookKind
    name: str
    handler: HookCallable


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


class HookManager:
    def __init__(self, hooks: list[HookSpec]) -> None:
        self._hooks = hooks

    async def run(self, phase: HookPhase, context: HookContext, payload: Any) -> Any:
        current = payload
        for hook in self._hooks:
            if hook.phase != phase:
                continue
            try:
                result = await _maybe_await(hook.handler(context, current))
                if hook.kind == "transform" and result is not None:
                    current = result
            except Exception as exc:
                if hook.kind == "guard":
                    raise HookGuardRejected(f"{hook.name}: {exc}") from exc
                if hook.kind == "transform":
                    raise HookTransformFailed(f"{hook.name}: {exc}") from exc
                logger.exception("observer hook failed: %s phase=%s", hook.name, phase)
        return current
