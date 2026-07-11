from __future__ import annotations

import json
from typing import Any

import httpx

from jasi.runtime.errors import ModelFailure, ModelTimeout
from jasi.runtime.models import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    ToolCall,
    ToolDefinition,
    Usage,
)


class OpenAICompatibleModel:
    def __init__(self, *, base_url: str, api_key: str, timeout_seconds: float = 60.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._timeout_seconds = timeout_seconds

    async def complete(self, request: ModelRequest) -> ModelResponse:
        timeout = min(request.timeout_seconds, self._timeout_seconds)
        payload: dict[str, Any] = {
            "model": request.model,
            "messages": [_message_to_payload(message) for message in request.messages],
        }
        if request.tools:
            payload["tools"] = [_tool_to_payload(tool) for tool in request.tools]
            payload["tool_choice"] = "auto"

        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(
                    f"{self._base_url}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self._api_key}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )
                response.raise_for_status()
        except httpx.TimeoutException as exc:
            raise ModelTimeout("model request timed out") from exc
        except httpx.HTTPStatusError as exc:
            message = _safe_http_error(exc.response)
            raise ModelFailure(f"model request failed: {message}") from exc
        except httpx.HTTPError as exc:
            raise ModelFailure(f"model transport failed: {exc.__class__.__name__}") from exc

        try:
            body = response.json()
            choice = body["choices"][0]
            message = choice["message"]
        except (ValueError, KeyError, IndexError, TypeError) as exc:
            raise ModelFailure("model response was not a valid chat completion") from exc

        return ModelResponse(
            content=message.get("content"),
            tool_calls=_parse_tool_calls(message.get("tool_calls") or []),
            usage=_parse_usage(body.get("usage") or {}),
            raw=body,
        )


def _message_to_payload(message: ModelMessage) -> dict[str, Any]:
    payload: dict[str, Any] = {"role": message.role}
    if message.content is not None:
        payload["content"] = message.content
    elif message.role == "assistant" and message.tool_calls:
        payload["content"] = None
    if message.tool_call_id is not None:
        payload["tool_call_id"] = message.tool_call_id
    if message.tool_calls:
        payload["tool_calls"] = [
            {
                "id": call.id,
                "type": "function",
                "function": {
                    "name": call.name,
                    "arguments": json.dumps(call.arguments, ensure_ascii=False),
                },
            }
            for call in message.tool_calls
        ]
    return payload


def _tool_to_payload(tool: ToolDefinition) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.parameters,
        },
    }


def _parse_tool_calls(raw_calls: list[dict[str, Any]]) -> list[ToolCall]:
    calls: list[ToolCall] = []
    for raw in raw_calls:
        function = raw.get("function") or {}
        arguments = function.get("arguments") or "{}"
        try:
            parsed_arguments = json.loads(arguments)
        except ValueError as exc:
            raise ModelFailure("model returned invalid tool arguments") from exc
        if not isinstance(parsed_arguments, dict):
            raise ModelFailure("model returned non-object tool arguments")
        calls.append(
            ToolCall(
                id=str(raw.get("id") or ""),
                name=str(function.get("name") or ""),
                arguments=parsed_arguments,
            )
        )
    return calls


def _parse_usage(raw_usage: dict[str, Any]) -> Usage:
    return Usage(
        prompt_tokens=int(raw_usage.get("prompt_tokens") or 0),
        completion_tokens=int(raw_usage.get("completion_tokens") or 0),
        total_tokens=int(raw_usage.get("total_tokens") or 0),
    )


def _safe_http_error(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return f"HTTP {response.status_code}"
    error = body.get("error") if isinstance(body, dict) else None
    if isinstance(error, dict):
        message = str(error.get("message") or f"HTTP {response.status_code}")
        return message[:500]
    return f"HTTP {response.status_code}"
