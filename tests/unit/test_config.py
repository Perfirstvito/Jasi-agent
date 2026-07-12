from __future__ import annotations

import os

import pytest

from jasi.config import SettingsError, load_settings


def _base_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in tuple(os.environ):
        if name.startswith("JASI_"):
            monkeypatch.delenv(name)
    values = {
        "JASI_DATABASE_URL": "postgresql+asyncpg://jasi:jasi@localhost/jasi",
        "JASI_OPENAI_BASE_URL": "https://chat.example/v1",
        "JASI_OPENAI_API_KEY": "chat-secret",
        "JASI_OPENAI_MODEL": "chat-model",
        "JASI_TELEGRAM_BOT_TOKEN": "telegram-token",
        "JASI_TELEGRAM_ALLOWED_USER_IDS": "123",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def test_embedding_is_explicitly_optional(monkeypatch: pytest.MonkeyPatch) -> None:
    _base_environment(monkeypatch)

    settings = load_settings()

    assert settings.memory_embedding_base_url is None
    assert settings.memory_embedding_api_key is None


def test_embedding_endpoint_can_reuse_chat_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    _base_environment(monkeypatch)
    monkeypatch.setenv("JASI_MEMORY_EMBEDDING_BASE_URL", "https://embedding.example/v1/")

    settings = load_settings()

    assert settings.memory_embedding_base_url == "https://embedding.example/v1"
    assert settings.memory_embedding_api_key == "chat-secret"


def test_embedding_api_key_without_endpoint_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    _base_environment(monkeypatch)
    monkeypatch.setenv("JASI_MEMORY_EMBEDDING_API_KEY", "embedding-secret")

    with pytest.raises(SettingsError, match="EMBEDDING_BASE_URL"):
        load_settings()


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("JASI_MEMORY_CONSOLIDATION_BATCH_MESSAGES", "3", "between 4 and 8"),
        ("JASI_MEMORY_JOB_LEASE_SECONDS", "0", "must be positive"),
        ("JASI_MEMORY_INJECT_LIMIT", "13", "cannot exceed"),
        ("JASI_MEMORY_SCORE_THRESHOLD", "1.1", "between 0 and 1"),
    ],
)
def test_invalid_memory_settings_fail_at_startup(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
    message: str,
) -> None:
    _base_environment(monkeypatch)
    monkeypatch.setenv(name, value)

    with pytest.raises(SettingsError, match=message):
        load_settings()
