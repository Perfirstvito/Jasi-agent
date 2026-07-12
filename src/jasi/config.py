from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType


class SettingsError(ValueError):
    pass


@dataclass(frozen=True)
class Settings:
    database_url: str
    openai_base_url: str
    openai_api_key: str
    openai_model: str
    telegram_bot_token: str
    telegram_allowed_user_ids: frozenset[int]
    prompt_dir: str = "prompts"
    memory_scope_map: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    timezone: str = "Asia/Shanghai"
    model_timeout_seconds: float = 60.0
    telegram_poll_timeout_seconds: int = 30
    telegram_max_concurrency: int = 8
    work_batch_size: int = 8
    work_background_concurrency: int = 2
    work_lease_seconds: float = 600.0
    work_heartbeat_seconds: float = 200.0
    schedule_batch_size: int = 20
    schedule_poll_seconds: float = 1.0
    source_batch_size: int = 10
    initiative_batch_size: int = 10
    drift_batch_size: int = 5
    effect_batch_size: int = 20
    outbox_batch_size: int = 20
    log_level: str = "INFO"


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SettingsError(f"Missing required environment variable: {name}")
    return value


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise SettingsError(f"{name} must be an integer") from exc


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise SettingsError(f"{name} must be a number") from exc


def _allowed_user_ids() -> frozenset[int]:
    raw = _required("JASI_TELEGRAM_ALLOWED_USER_IDS")
    ids: set[int] = set()
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        if not item.isdigit():
            raise SettingsError("JASI_TELEGRAM_ALLOWED_USER_IDS must contain numeric IDs only")
        ids.add(int(item))
    if not ids:
        raise SettingsError("JASI_TELEGRAM_ALLOWED_USER_IDS cannot be empty")
    return frozenset(ids)


def _memory_scope_map() -> Mapping[str, str]:
    raw = os.environ.get("JASI_MEMORY_SCOPE_MAP", "").strip()
    if not raw:
        return MappingProxyType({})
    try:
        payload = json.loads(raw)
    except ValueError as exc:
        raise SettingsError("JASI_MEMORY_SCOPE_MAP must be a JSON object") from exc
    if not isinstance(payload, dict):
        raise SettingsError("JASI_MEMORY_SCOPE_MAP must be a JSON object")
    mapping: dict[str, str] = {}
    for identity, scope_key in payload.items():
        identity = str(identity).strip()
        scope_key = str(scope_key).strip()
        if not identity or not scope_key:
            raise SettingsError("JASI_MEMORY_SCOPE_MAP keys and values cannot be empty")
        mapping[identity] = scope_key
    return MappingProxyType(mapping)


def load_settings() -> Settings:
    return Settings(
        database_url=_required("JASI_DATABASE_URL"),
        openai_base_url=_required("JASI_OPENAI_BASE_URL").rstrip("/"),
        openai_api_key=_required("JASI_OPENAI_API_KEY"),
        openai_model=_required("JASI_OPENAI_MODEL"),
        telegram_bot_token=_required("JASI_TELEGRAM_BOT_TOKEN"),
        telegram_allowed_user_ids=_allowed_user_ids(),
        prompt_dir=os.environ.get("JASI_PROMPT_DIR", "prompts").strip() or "prompts",
        memory_scope_map=_memory_scope_map(),
        timezone=os.environ.get("JASI_TIMEZONE", "Asia/Shanghai").strip() or "Asia/Shanghai",
        model_timeout_seconds=_float("JASI_MODEL_TIMEOUT_SECONDS", 60.0),
        telegram_poll_timeout_seconds=_int("JASI_TELEGRAM_POLL_TIMEOUT_SECONDS", 30),
        telegram_max_concurrency=_int("JASI_TELEGRAM_MAX_CONCURRENCY", 8),
        work_batch_size=_int("JASI_WORK_BATCH_SIZE", 8),
        work_background_concurrency=_int("JASI_WORK_BACKGROUND_CONCURRENCY", 2),
        work_lease_seconds=_float("JASI_WORK_LEASE_SECONDS", 600.0),
        work_heartbeat_seconds=_float("JASI_WORK_HEARTBEAT_SECONDS", 200.0),
        schedule_batch_size=_int("JASI_SCHEDULE_BATCH_SIZE", 20),
        schedule_poll_seconds=_float("JASI_SCHEDULE_POLL_SECONDS", 1.0),
        source_batch_size=_int("JASI_SOURCE_BATCH_SIZE", 10),
        initiative_batch_size=_int("JASI_INITIATIVE_BATCH_SIZE", 10),
        drift_batch_size=_int("JASI_DRIFT_BATCH_SIZE", 5),
        effect_batch_size=_int("JASI_EFFECT_BATCH_SIZE", 20),
        outbox_batch_size=_int("JASI_OUTBOX_BATCH_SIZE", 20),
        log_level=os.environ.get("JASI_LOG_LEVEL", "INFO").strip() or "INFO",
    )
