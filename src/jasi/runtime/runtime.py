from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from jasi.domain.models import MessageRecord
from jasi.ports.model import ModelPort
from jasi.ports.repository import RuntimeRepositoryPort
from jasi.runtime.errors import (
    HookGuardRejected,
    HookTransformFailed,
    MaxStepsExceeded,
    ModelFailure,
    ModelTimeout,
    RuntimeFailure,
    ToolFailure,
    ToolRejected,
)
from jasi.runtime.hooks import HookContext, HookManager
from jasi.runtime.models import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    ToolCall,
    ToolExecutionRecord,
    TurnRequest,
    TurnResult,
    Usage,
)
from jasi.runtime.profile import RuntimeProfile
from jasi.tools.registry import ToolRegistry

logger = logging.getLogger(__name__)

FIXED_ERROR_REPLY = "抱歉，我刚才处理失败了，请稍后再试。"


class AgentRuntime:
    def __init__(
        self,
        *,
        profile: RuntimeProfile,
        model: ModelPort,
        repository: RuntimeRepositoryPort,
        tools: ToolRegistry,
        model_name: str,
        model_timeout_seconds: float,
        timezone: str,
    ) -> None:
        self._profile = profile
        self._model = model
        self._repository = repository
        self._tools = tools
        self._model_name = model_name
        self._model_timeout_seconds = model_timeout_seconds
        self._timezone = timezone
        self._hooks = HookManager(profile.hooks)

    async def run(self, request: TurnRequest) -> TurnResult:
        if request.profile != self._profile.name:
            raise ValueError(f"unsupported profile: {request.profile}")

        conversation_id = int(request.metadata["conversation_id"])
        before_sequence = int(request.metadata["message_sequence"])
        turn_start = await self._repository.start_turn(
            conversation_id=conversation_id,
            inbound_message_id=request.inbound_message_id,
            profile=request.profile,
            model=self._model_name,
            metadata={"session_id": request.session_id},
        )
        if turn_start.cached_result is not None:
            logger.info(
                "resuming completed turn turn_id=%s session=%s",
                turn_start.turn_id,
                request.session_id,
            )
            return turn_start.cached_result
        turn_id = turn_start.turn_id

        usage = Usage()
        tool_records: list[ToolExecutionRecord] = []
        steps = 0

        try:
            history = await self._repository.load_history_before(
                conversation_id=conversation_id,
                before_sequence=before_sequence,
                limit=self._profile.history_limit,
            )
            messages = self._build_messages(history, request.inbound_text)
            tools = self._tools.definitions(self._profile.allowed_tools)

            while steps < self._profile.max_model_steps:
                steps += 1
                model_request = ModelRequest(
                    model=self._model_name,
                    messages=list(messages),
                    tools=tools,
                    timeout_seconds=self._model_timeout_seconds,
                )
                model_request = await self._hooks.run(
                    "before_model",
                    self._hook_context(request, turn_id),
                    model_request,
                )
                try:
                    async with asyncio.timeout(model_request.timeout_seconds):
                        response = await self._model.complete(model_request)
                except TimeoutError as exc:
                    raise ModelTimeout("model request timed out") from exc
                response = await self._hooks.run(
                    "after_model",
                    self._hook_context(request, turn_id),
                    response,
                )
                if not isinstance(response, ModelResponse):
                    raise ModelFailure("model adapter returned an invalid response")
                usage = usage.add(response.usage)

                if response.tool_calls:
                    if steps >= self._profile.max_model_steps:
                        raise MaxStepsExceeded(
                            "model requested a tool after the final allowed step"
                        )
                    messages.append(
                        ModelMessage(
                            role="assistant",
                            content=response.content,
                            tool_calls=response.tool_calls,
                        )
                    )
                    for tool_call in response.tool_calls:
                        record = await self._execute_tool(request, turn_id, tool_call)
                        tool_records.append(record)
                        await self._repository.record_tool_execution(turn_id, record)
                        messages.append(
                            ModelMessage(
                                role="tool",
                                tool_call_id=tool_call.id,
                                content=json.dumps(record.result, ensure_ascii=False, default=str),
                            )
                        )
                    continue

                final_text = (response.content or "").strip()
                if not final_text:
                    raise ModelFailure("model returned empty content")
                result = TurnResult(
                    turn_id=turn_id,
                    status="succeeded",
                    final_text=final_text,
                    tool_records=tool_records,
                    usage=usage,
                )
                return await self._commit_result(request, result, steps)

            raise MaxStepsExceeded("model did not produce a final answer within the step limit")
        except RuntimeFailure as exc:
            result = TurnResult(
                turn_id=turn_id,
                status="failed",
                final_text=FIXED_ERROR_REPLY,
                tool_records=tool_records,
                usage=usage,
                error_code=exc.code,
                error_message=exc.safe_message(),
            )
            return await self._commit_result(request, result, steps)
        except Exception as exc:
            logger.exception("unexpected runtime failure turn_id=%s", turn_id)
            result = TurnResult(
                turn_id=turn_id,
                status="failed",
                final_text=FIXED_ERROR_REPLY,
                tool_records=tool_records,
                usage=usage,
                error_code="unexpected_runtime_failure",
                error_message=str(exc)[:500],
            )
            return await self._commit_result(request, result, steps)

    def _build_messages(
        self, history: list[MessageRecord], inbound_text: str
    ) -> list[ModelMessage]:
        messages = [ModelMessage(role="system", content=self._profile.system_prompt)]
        for item in history:
            if item.role == "user":
                messages.append(ModelMessage(role="user", content=item.content))
            elif item.role == "assistant":
                messages.append(ModelMessage(role="assistant", content=item.content))
        messages.append(ModelMessage(role="user", content=inbound_text))
        return messages

    async def _execute_tool(
        self, request: TurnRequest, turn_id: int, tool_call: ToolCall
    ) -> ToolExecutionRecord:
        started = time.monotonic()
        context = self._hook_context(
            request,
            turn_id,
            {
                "tool_name": tool_call.name,
                "allowed_tools": sorted(self._profile.allowed_tools),
            },
        )
        try:
            await self._hooks.run("before_tool", context, tool_call)
            if tool_call.name not in self._profile.allowed_tools:
                raise ToolRejected(f"tool not allowed: {tool_call.name}")
            result = await self._tools.execute(
                tool_call.name,
                tool_call.arguments,
                {"timezone": self._timezone, "session_id": request.session_id},
            )
            record = ToolExecutionRecord(
                name=tool_call.name,
                arguments=tool_call.arguments,
                result=result,
                risk=self._tools.risk(tool_call.name),
                status="succeeded",
                duration_ms=self._elapsed_ms(started),
            )
            record = await self._hooks.run("after_tool", context, record)
            if not isinstance(record, ToolExecutionRecord):
                raise HookTransformFailed("after_tool returned an invalid tool record")
            return record
        except HookGuardRejected:
            record = ToolExecutionRecord(
                name=tool_call.name,
                arguments=tool_call.arguments,
                result={},
                risk="unknown",
                status="rejected",
                duration_ms=self._elapsed_ms(started),
                error_message="tool rejected by hook",
            )
            await self._repository.record_tool_execution(turn_id, record)
            raise
        except (ToolFailure, ToolRejected) as exc:
            try:
                risk = self._tools.risk(tool_call.name)
            except Exception:
                risk = "unknown"
            record = ToolExecutionRecord(
                name=tool_call.name,
                arguments=tool_call.arguments,
                result={},
                risk=risk,
                status="rejected" if isinstance(exc, ToolRejected) else "failed",
                duration_ms=self._elapsed_ms(started),
                error_message=exc.safe_message(),
            )
            await self._repository.record_tool_execution(turn_id, record)
            raise

    async def _commit_result(
        self, request: TurnRequest, result: TurnResult, steps: int
    ) -> TurnResult:
        try:
            result = await self._hooks.run(
                "before_commit",
                self._hook_context(request, result.turn_id),
                result,
            )
            if not isinstance(result, TurnResult):
                raise HookTransformFailed("before_commit returned an invalid turn result")
        except (HookGuardRejected, HookTransformFailed) as exc:
            result = TurnResult(
                turn_id=result.turn_id,
                status="failed",
                final_text=FIXED_ERROR_REPLY,
                tool_records=result.tool_records,
                usage=result.usage,
                error_code=exc.code,
                error_message=exc.safe_message(),
            )
        await self._repository.finish_turn(
            turn_id=result.turn_id,
            status=result.status,
            final_text=result.final_text,
            step_count=steps,
            usage=result.usage,
            error_code=result.error_code,
            error_message=result.error_message,
        )
        await self._hooks.run(
            "after_commit",
            self._hook_context(request, result.turn_id),
            result,
        )
        return result

    def _hook_context(
        self, request: TurnRequest, turn_id: int, extra_metadata: dict[str, Any] | None = None
    ) -> HookContext:
        metadata = dict(request.metadata)
        if extra_metadata:
            metadata.update(extra_metadata)
        return HookContext(
            session_id=request.session_id,
            turn_id=turn_id,
            profile=request.profile,
            metadata=metadata,
        )

    @staticmethod
    def _elapsed_ms(started: float) -> int:
        return max(0, int((time.monotonic() - started) * 1000))
