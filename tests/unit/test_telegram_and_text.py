from __future__ import annotations

import asyncio

import pytest

from jasi.adapters.channels.telegram import TelegramLongPollingAdapter, TelegramOutboundPolicy


def test_telegram_adapter_accepts_allowed_private_text() -> None:
    adapter = TelegramLongPollingAdapter(
        bot_token="token",
        allowed_user_ids=frozenset({42}),
        poll_timeout_seconds=1,
    )

    inbound = adapter._to_inbound(
        {
            "update_id": 123,
            "message": {
                "message_id": 7,
                "date": 1,
                "text": "hello",
                "chat": {"id": 99, "type": "private"},
                "from": {"id": 42, "is_bot": False},
            },
        }
    )

    assert inbound is not None
    assert inbound.external_update_id == "123"
    assert inbound.external_chat_id == "99"
    assert inbound.text == "hello"


def test_telegram_adapter_rejects_group_or_unlisted_user() -> None:
    adapter = TelegramLongPollingAdapter(
        bot_token="token",
        allowed_user_ids=frozenset({42}),
        poll_timeout_seconds=1,
    )

    assert (
        adapter._to_inbound(
            {
                "update_id": 1,
                "message": {
                    "text": "hello",
                    "chat": {"id": 99, "type": "group"},
                    "from": {"id": 42, "is_bot": False},
                },
            }
        )
        is None
    )
    assert (
        adapter._to_inbound(
            {
                "update_id": 2,
                "message": {
                    "text": "hello",
                    "chat": {"id": 99, "type": "private"},
                    "from": {"id": 7, "is_bot": False},
                },
            }
        )
        is None
    )


def test_telegram_outbound_policy_respects_limit() -> None:
    text = "a" * 4100

    parts = TelegramOutboundPolicy(limit=1000).prepare(text)

    assert len(parts) == 5
    assert all(0 < len(part.text) <= 1000 for part in parts)
    assert "".join(part.text for part in parts) == text


def telegram_update(update_id: int) -> dict:
    return {
        "update_id": update_id,
        "message": {
            "message_id": update_id,
            "date": 1,
            "text": "hello",
            "chat": {"id": update_id, "type": "private"},
            "from": {"id": 42, "is_bot": False},
        },
    }


class ConcurrentHandler:
    def __init__(self, failed_update_id: str | None = None) -> None:
        self.failed_update_id = failed_update_id
        self.calls: list[str] = []
        self.active = 0
        self.max_active = 0

    async def handle(self, message) -> None:
        self.calls.append(message.external_update_id)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(0.01)
            if message.external_update_id == self.failed_update_id:
                raise RuntimeError("failed")
        finally:
            self.active -= 1


@pytest.mark.asyncio
async def test_telegram_batch_processing_is_bounded_and_concurrent() -> None:
    adapter = TelegramLongPollingAdapter(
        bot_token="token",
        allowed_user_ids=frozenset({42}),
        poll_timeout_seconds=1,
        max_concurrency=2,
    )
    handler = ConcurrentHandler()

    succeeded = await adapter._handle_updates(
        [telegram_update(update_id) for update_id in range(1, 5)],
        handler,
    )

    assert succeeded
    assert handler.max_active == 2
    assert set(handler.calls) == {"1", "2", "3", "4"}


@pytest.mark.asyncio
async def test_telegram_batch_failure_prevents_acknowledgement() -> None:
    adapter = TelegramLongPollingAdapter(
        bot_token="token",
        allowed_user_ids=frozenset({42}),
        poll_timeout_seconds=1,
        max_concurrency=2,
    )
    handler = ConcurrentHandler(failed_update_id="2")

    succeeded = await adapter._handle_updates(
        [telegram_update(1), telegram_update(2), telegram_update(3)],
        handler,
    )

    assert not succeeded
    assert set(handler.calls) == {"1", "2", "3"}
