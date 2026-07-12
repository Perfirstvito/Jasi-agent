from __future__ import annotations

from typing import Any

from jasi.domain.models import MessageRecord
from jasi.ports.tools import MessageLookupPort
from jasi.runtime.errors import ToolRejected
from jasi.tools.registry import ToolExecutionContext, ToolSpec

_MAX_FETCH_IDS = 20
_MAX_SEARCH_RESULTS = 20
_MAX_PREVIEW_CHARS = 500


def create_message_tools(messages: MessageLookupPort) -> list[ToolSpec]:
    async def fetch_messages(
        arguments: dict[str, Any], context: ToolExecutionContext
    ) -> dict[str, Any]:
        ids = tuple(dict.fromkeys(int(value) for value in arguments["message_ids"]))
        rows = await messages.fetch_messages(context.conversation_id, ids)
        by_id = {row.id: row for row in rows}
        return {
            "messages": [_full_message(by_id[item_id]) for item_id in ids if item_id in by_id],
            "missing_ids": [item_id for item_id in ids if item_id not in by_id],
        }

    async def search_messages(
        arguments: dict[str, Any], context: ToolExecutionContext
    ) -> dict[str, Any]:
        query = arguments["query"].strip()
        if not query:
            raise ToolRejected("message search query cannot be blank")
        limit = int(arguments.get("limit", 10))
        offset = int(arguments.get("offset", 0))
        rows, total = await messages.search_messages(
            context.conversation_id,
            query,
            limit,
            offset,
        )
        next_offset = offset + len(rows)
        return {
            "query": query,
            "messages": [_message_preview(row) for row in rows],
            "count": len(rows),
            "total": total,
            "next_offset": next_offset if next_offset < total else None,
        }

    return [
        ToolSpec(
            name="fetch_messages",
            description=(
                "Fetch exact delivered messages by message ID from the current conversation. "
                "Use IDs returned by search_messages or memory evidence."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "message_ids": {
                        "type": "array",
                        "items": {"type": "integer", "minimum": 1},
                        "minItems": 1,
                        "maxItems": _MAX_FETCH_IDS,
                    },
                },
                "required": ["message_ids"],
                "additionalProperties": False,
            },
            risk="read-only",
            handler=fetch_messages,
            search_terms=("message details", "conversation evidence", "消息原文", "历史原文"),
        ),
        ToolSpec(
            name="search_messages",
            description=(
                "Search delivered user and assistant text in the current conversation. "
                "Returns short previews and message IDs for exact follow-up retrieval."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "minLength": 1, "maxLength": 500},
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": _MAX_SEARCH_RESULTS,
                        "default": 10,
                    },
                    "offset": {"type": "integer", "minimum": 0, "default": 0},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
            risk="read-only",
            handler=search_messages,
            search_terms=("search conversation", "find old message", "搜索消息", "历史对话"),
        ),
    ]


def _full_message(row: MessageRecord) -> dict[str, Any]:
    return {
        "id": row.id,
        "sequence": row.sequence,
        "role": row.role,
        "content": row.content,
        "created_at": row.created_at.isoformat(),
    }


def _message_preview(row: MessageRecord) -> dict[str, Any]:
    content = row.content
    truncated = len(content) > _MAX_PREVIEW_CHARS
    return {
        "id": row.id,
        "sequence": row.sequence,
        "role": row.role,
        "preview": content[:_MAX_PREVIEW_CHARS],
        "truncated": truncated,
        "created_at": row.created_at.isoformat(),
    }
