from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from jasi.domain.models import DeliveryResult, InboundMessage, OutboundMessage, OutboundPart
from jasi.ports.channel import InboundHandler

logger = logging.getLogger(__name__)
TELEGRAM_TEXT_LIMIT = 4096


class TelegramOutboundPolicy:
    def __init__(self, limit: int = TELEGRAM_TEXT_LIMIT) -> None:
        if limit <= 0:
            raise ValueError("Telegram text limit must be positive")
        self._limit = limit

    def prepare(self, text: str) -> tuple[OutboundPart, ...]:
        return tuple(OutboundPart(text=part) for part in _split_text(text, self._limit))


def _split_text(text: str, limit: int) -> list[str]:
    normalized = text.strip()
    if not normalized:
        return [" "]
    parts: list[str] = []
    remaining = normalized
    while len(remaining) > limit:
        split_at = remaining.rfind("\n", 0, limit + 1)
        if split_at < max(1, limit // 2):
            split_at = remaining.rfind(" ", 0, limit + 1)
        if split_at < max(1, limit // 2):
            split_at = limit
        part = remaining[:split_at].strip()
        if part:
            parts.append(part)
        remaining = remaining[split_at:].strip()
    if remaining:
        parts.append(remaining)
    return parts or [" "]


class TelegramBotClient:
    def __init__(self, *, bot_token: str, request_timeout_seconds: float = 30.0) -> None:
        self._base_url = f"https://api.telegram.org/bot{bot_token}"
        self._request_timeout_seconds = request_timeout_seconds

    async def send(self, message: OutboundMessage) -> DeliveryResult:
        async with httpx.AsyncClient(timeout=self._request_timeout_seconds) as client:
            try:
                response = await client.post(
                    f"{self._base_url}/sendMessage",
                    json={
                        "chat_id": message.external_chat_id,
                        "text": message.text,
                        "disable_web_page_preview": True,
                    },
                )
            except httpx.HTTPError as exc:
                return DeliveryResult(success=False, error=exc.__class__.__name__, retryable=True)

        retryable = response.status_code >= 500 or response.status_code == 429
        try:
            body = response.json()
        except ValueError:
            return DeliveryResult(
                success=False,
                error=f"Telegram HTTP {response.status_code}",
                retryable=retryable,
            )

        if response.status_code == 200 and body.get("ok") is True:
            result = body.get("result") or {}
            message_id = result.get("message_id")
            return DeliveryResult(
                success=True,
                external_message_id=str(message_id) if message_id is not None else None,
            )

        error = body.get("description") or f"Telegram HTTP {response.status_code}"
        parameters = body.get("parameters") or {}
        if "retry_after" in parameters:
            retryable = True
        return DeliveryResult(success=False, error=str(error)[:500], retryable=retryable)


class TelegramLongPollingAdapter:
    def __init__(
        self,
        *,
        bot_token: str,
        allowed_user_ids: frozenset[int],
        poll_timeout_seconds: int,
        max_concurrency: int = 8,
    ) -> None:
        if not allowed_user_ids:
            raise ValueError("Telegram allowed user ID list cannot be empty")
        if max_concurrency <= 0:
            raise ValueError("Telegram max concurrency must be positive")
        self._base_url = f"https://api.telegram.org/bot{bot_token}"
        self._allowed_user_ids = allowed_user_ids
        self._poll_timeout_seconds = poll_timeout_seconds
        self._max_concurrency = max_concurrency

    async def run(self, service: InboundHandler, stop_event: asyncio.Event) -> None:
        logger.info("telegram long polling started")
        offset: int | None = None
        async with httpx.AsyncClient(timeout=self._poll_timeout_seconds + 10) as client:
            while not stop_event.is_set():
                try:
                    updates = await self._get_updates(client, offset)
                except httpx.HTTPError as exc:
                    logger.warning("telegram getUpdates failed: %s", exc.__class__.__name__)
                    await asyncio.sleep(2)
                    continue

                if not updates:
                    continue
                if not await self._handle_updates(updates, service):
                    await asyncio.sleep(1)
                    continue
                update_ids = [
                    update_id
                    for update in updates
                    if isinstance((update_id := update.get("update_id")), int)
                ]
                if update_ids:
                    offset = max(update_ids) + 1
        logger.info("telegram long polling stopped")

    async def _handle_updates(
        self,
        updates: list[dict[str, Any]],
        service: InboundHandler,
    ) -> bool:
        semaphore = asyncio.Semaphore(self._max_concurrency)

        async def handle(update: dict[str, Any]) -> bool:
            inbound = self._to_inbound(update)
            if inbound is None:
                return True
            try:
                async with semaphore:
                    await service.handle(inbound)
                return True
            except Exception as exc:
                logger.error(
                    "failed to handle telegram update_id=%s error=%s",
                    update.get("update_id"),
                    exc.__class__.__name__,
                )
                return False

        return all(await asyncio.gather(*(handle(update) for update in updates)))

    async def _get_updates(
        self,
        client: httpx.AsyncClient,
        offset: int | None,
    ) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {
            "timeout": self._poll_timeout_seconds,
            "allowed_updates": ["message"],
        }
        if offset is not None:
            payload["offset"] = offset
        response = await client.post(f"{self._base_url}/getUpdates", json=payload)
        response.raise_for_status()
        body = response.json()
        if body.get("ok") is not True:
            raise httpx.HTTPStatusError(
                "Telegram getUpdates returned ok=false",
                request=response.request,
                response=response,
            )
        result = body.get("result") or []
        if not isinstance(result, list):
            return []
        return result

    def _to_inbound(self, update: dict[str, Any]) -> InboundMessage | None:
        update_id = update.get("update_id")
        message = update.get("message")
        if not isinstance(update_id, int) or not isinstance(message, dict):
            return None

        chat = message.get("chat") or {}
        if chat.get("type") != "private":
            return None

        text = message.get("text")
        if not isinstance(text, str) or not text.strip():
            return None

        user = message.get("from") or {}
        user_id = user.get("id")
        if not isinstance(user_id, int):
            return None
        if user_id not in self._allowed_user_ids:
            logger.warning("telegram user_id=%s is not allowed", user_id)
            return None
        if user.get("is_bot") is True:
            return None

        chat_id = chat.get("id")
        if not isinstance(chat_id, int):
            return None

        return InboundMessage(
            channel="telegram",
            external_update_id=str(update_id),
            external_chat_id=str(chat_id),
            external_user_id=str(user_id),
            text=text,
            metadata={
                "telegram_message_id": message.get("message_id"),
                "telegram_date": message.get("date"),
            },
        )
