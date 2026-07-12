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
    light_model_base_url: str
    light_model_api_key: str
    light_model: str
    telegram_bot_token: str
    telegram_allowed_user_ids: frozenset[int]
    prompt_dir: str = "prompts"
    tool_workspace: str = "workspace/tools"
    web_allow_fake_ip_dns: bool = False
    memory_scope_map: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    memory_root: str = "workspace/memory"
    light_model_timeout_seconds: float = 30.0
    memory_embedding_base_url: str | None = None
    memory_embedding_api_key: str | None = None
    memory_embedding_model: str = "text-embedding-v3"
    memory_embedding_timeout_seconds: float = 30.0
    memory_consolidation_batch_messages: int = 6
    memory_job_batch_size: int = 4
    memory_job_lease_seconds: float = 300.0
    memory_reconcile_seconds: float = 30.0
    memory_search_limit: int = 12
    memory_inject_limit: int = 6
    memory_score_threshold: float = 0.35
    memory_max_context_chars: int = 6000
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


def _bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "").strip().casefold()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise SettingsError(f"{name} must be a boolean")


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
    openai_base_url = _required("JASI_OPENAI_BASE_URL").rstrip("/")
    openai_api_key = _required("JASI_OPENAI_API_KEY")
    openai_model = _required("JASI_OPENAI_MODEL")
    light_model_base_url = os.environ.get("JASI_LIGHT_MODEL_BASE_URL", "").strip()
    light_model_api_key = os.environ.get("JASI_LIGHT_MODEL_API_KEY", "").strip()
    light_model = os.environ.get("JASI_LIGHT_MODEL", "").strip()
    configured_light_values = (light_model_base_url, light_model_api_key, light_model)
    if any(configured_light_values) and not all(configured_light_values):
        raise SettingsError(
            "JASI_LIGHT_MODEL_BASE_URL, JASI_LIGHT_MODEL_API_KEY, and "
            "JASI_LIGHT_MODEL must be configured together"
        )
    if not any(configured_light_values):
        light_model_base_url = openai_base_url
        light_model_api_key = openai_api_key
        light_model = openai_model
    light_model_base_url = light_model_base_url.rstrip("/")
    light_model_timeout = _float("JASI_LIGHT_MODEL_TIMEOUT_SECONDS", 30.0)
    consolidation_batch = _int("JASI_MEMORY_CONSOLIDATION_BATCH_MESSAGES", 6)
    if not 4 <= consolidation_batch <= 8:
        raise SettingsError("JASI_MEMORY_CONSOLIDATION_BATCH_MESSAGES must be between 4 and 8")
    embedding_timeout = _float("JASI_MEMORY_EMBEDDING_TIMEOUT_SECONDS", 30.0)
    memory_job_batch_size = _int("JASI_MEMORY_JOB_BATCH_SIZE", 4)
    memory_job_lease_seconds = _float("JASI_MEMORY_JOB_LEASE_SECONDS", 300.0)
    memory_reconcile_seconds = _float("JASI_MEMORY_RECONCILE_SECONDS", 30.0)
    memory_search_limit = _int("JASI_MEMORY_SEARCH_LIMIT", 12)
    memory_inject_limit = _int("JASI_MEMORY_INJECT_LIMIT", 6)
    memory_score_threshold = _float("JASI_MEMORY_SCORE_THRESHOLD", 0.35)
    memory_max_context_chars = _int("JASI_MEMORY_MAX_CONTEXT_CHARS", 6000)
    positive_memory_values = {
        "JASI_MEMORY_EMBEDDING_TIMEOUT_SECONDS": embedding_timeout,
        "JASI_LIGHT_MODEL_TIMEOUT_SECONDS": light_model_timeout,
        "JASI_MEMORY_JOB_BATCH_SIZE": memory_job_batch_size,
        "JASI_MEMORY_JOB_LEASE_SECONDS": memory_job_lease_seconds,
        "JASI_MEMORY_RECONCILE_SECONDS": memory_reconcile_seconds,
        "JASI_MEMORY_SEARCH_LIMIT": memory_search_limit,
        "JASI_MEMORY_INJECT_LIMIT": memory_inject_limit,
        "JASI_MEMORY_MAX_CONTEXT_CHARS": memory_max_context_chars,
    }
    invalid_positive = next(
        (name for name, value in positive_memory_values.items() if value <= 0),
        None,
    )
    if invalid_positive is not None:
        raise SettingsError(f"{invalid_positive} must be positive")
    if memory_inject_limit > memory_search_limit:
        raise SettingsError("JASI_MEMORY_INJECT_LIMIT cannot exceed JASI_MEMORY_SEARCH_LIMIT")
    if not 0 <= memory_score_threshold <= 1:
        raise SettingsError("JASI_MEMORY_SCORE_THRESHOLD must be between 0 and 1")
    embedding_base_url = os.environ.get("JASI_MEMORY_EMBEDDING_BASE_URL", "").strip()
    embedding_api_key = os.environ.get("JASI_MEMORY_EMBEDDING_API_KEY", "").strip()
    if bool(embedding_api_key) != bool(embedding_base_url):
        raise SettingsError(
            "JASI_MEMORY_EMBEDDING_BASE_URL and JASI_MEMORY_EMBEDDING_API_KEY "
            "must be configured together"
        )
    if embedding_base_url:
        embedding_base_url = embedding_base_url.rstrip("/")
    return Settings(
        database_url=_required("JASI_DATABASE_URL"),
        openai_base_url=openai_base_url,
        openai_api_key=openai_api_key,
        openai_model=openai_model,
        light_model_base_url=light_model_base_url,
        light_model_api_key=light_model_api_key,
        light_model=light_model,
        telegram_bot_token=_required("JASI_TELEGRAM_BOT_TOKEN"),
        telegram_allowed_user_ids=_allowed_user_ids(),
        prompt_dir=os.environ.get("JASI_PROMPT_DIR", "prompts").strip() or "prompts",
        tool_workspace=os.environ.get("JASI_TOOL_WORKSPACE", "workspace/tools").strip()
        or "workspace/tools",
        web_allow_fake_ip_dns=_bool("JASI_WEB_ALLOW_FAKE_IP_DNS", False),
        memory_scope_map=_memory_scope_map(),
        memory_root=os.environ.get("JASI_MEMORY_ROOT", "workspace/memory").strip()
        or "workspace/memory",
        light_model_timeout_seconds=light_model_timeout,
        memory_embedding_base_url=embedding_base_url or None,
        memory_embedding_api_key=embedding_api_key or None,
        memory_embedding_model=os.environ.get(
            "JASI_MEMORY_EMBEDDING_MODEL",
            "text-embedding-v3",
        ).strip()
        or "text-embedding-v3",
        memory_embedding_timeout_seconds=embedding_timeout,
        memory_consolidation_batch_messages=consolidation_batch,
        memory_job_batch_size=memory_job_batch_size,
        memory_job_lease_seconds=memory_job_lease_seconds,
        memory_reconcile_seconds=memory_reconcile_seconds,
        memory_search_limit=memory_search_limit,
        memory_inject_limit=memory_inject_limit,
        memory_score_threshold=memory_score_threshold,
        memory_max_context_chars=memory_max_context_chars,
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
