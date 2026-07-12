from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from jasi.application.effect import EffectDispatcher
from jasi.domain.effect import EffectDraft, EffectRecord, EffectResult


def effect() -> EffectRecord:
    return EffectRecord(
        id=1,
        adapter="feed",
        operation="ack",
        dedupe_key="ack:item-1",
        payload={"id": "item-1"},
        status="executing",
        attempts=0,
        next_attempt_at=datetime.now(UTC),
    )


def test_effect_draft_requires_explicit_idempotency_key() -> None:
    with pytest.raises(ValueError, match="dedupe key"):
        EffectDraft(adapter="feed", operation="ack", dedupe_key=" ")


@pytest.mark.asyncio
async def test_effect_dispatcher_records_success_and_safe_failure() -> None:
    class Repository:
        def __init__(self) -> None:
            self.succeeded: dict | None = None
            self.failed: tuple[str, bool] | None = None

        async def mark_effect_succeeded(self, _id, result):
            self.succeeded = result

        async def mark_effect_failed_attempt(self, _id, error, retryable):
            self.failed = (error, retryable)

    class Adapter:
        async def execute(self, _effect):
            return EffectResult(success=True, result={"acked": True})

    repository = Repository()
    dispatcher = EffectDispatcher(
        repository=repository,
        adapters={"feed": Adapter()},
    )
    await dispatcher.execute(effect())
    assert repository.succeeded == {"acked": True}

    await EffectDispatcher(repository=repository, adapters={}).execute(
        replace(effect(), adapter="missing")
    )
    assert repository.failed == ("unsupported effect adapter: missing", False)


@pytest.mark.asyncio
async def test_effect_adapter_exception_does_not_persist_details() -> None:
    class Repository:
        def __init__(self) -> None:
            self.error: str | None = None

        async def mark_effect_succeeded(self, _id, _result):
            raise AssertionError("failure must not succeed")

        async def mark_effect_failed_attempt(self, _id, error, retryable):
            assert retryable is True
            self.error = error

    class Adapter:
        async def execute(self, _effect):
            raise RuntimeError("secret token")

    repository = Repository()
    await EffectDispatcher(
        repository=repository,
        adapters={"feed": Adapter()},
    ).execute(effect())

    assert repository.error == "RuntimeError"
