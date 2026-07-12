from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path

from jasi.adapters.channels.telegram import (
    TelegramBotClient,
    TelegramLongPollingAdapter,
    TelegramOutboundPolicy,
)
from jasi.adapters.llm.openai_compatible import OpenAICompatibleModel
from jasi.adapters.llm.openai_compatible_embedding import OpenAICompatibleEmbedding
from jasi.adapters.persistence.markdown.memory_store import MarkdownMemoryStore
from jasi.adapters.persistence.postgres.db import (
    check_database_ready,
    create_engine,
    create_session_factory,
)
from jasi.adapters.persistence.postgres.effect_repository import SQLAlchemyEffectRepository
from jasi.adapters.persistence.postgres.initiative_repository import (
    SQLAlchemyInitiativeRepository,
)
from jasi.adapters.persistence.postgres.memory_repository import (
    SQLAlchemyMemoryRepository,
)
from jasi.adapters.persistence.postgres.memory_tables import EMBEDDING_DIMENSIONS
from jasi.adapters.persistence.postgres.repository import SQLAlchemyRepository
from jasi.adapters.persistence.postgres.schedule_repository import SQLAlchemyScheduleRepository
from jasi.adapters.persistence.postgres.source_repository import SQLAlchemySourceRepository
from jasi.adapters.persistence.postgres.work_repository import SQLAlchemyWorkRepository
from jasi.adapters.system.processes import LocalProcessInspector
from jasi.application.agent_work import AgentWorkHandler
from jasi.application.context import TurnContextProvider
from jasi.application.direct_work import DirectWorkHandler
from jasi.application.effect import EffectDispatcher, EffectWorker
from jasi.application.memory.indexing import MarkdownMemoryIndexer
from jasi.application.memory.maintenance import MemoryConsolidator, MemoryWorker
from jasi.application.memory.reasoning import ModelMemoryReasoner
from jasi.application.memory.retrieval import MemoryContextService
from jasi.application.outbox import OutboxDispatcher, OutboxWorker
from jasi.application.passive_service import PassiveIngressService
from jasi.application.schedule import ScheduleWorker
from jasi.application.source import InitiativePlanner, SourceDispatcher, SourceWorker
from jasi.application.work import WorkDispatcher, WorkFinalizer, WorkWorker
from jasi.config import SettingsError, load_settings
from jasi.logging import configure_logging
from jasi.runtime.profile import (
    DRIFT_PROFILE,
    PASSIVE_PROFILE,
    PROACTIVE_PROFILE,
    SCHEDULED_PROFILE,
)
from jasi.runtime.prompting import PromptAssembler, PromptCatalog
from jasi.runtime.runtime import AgentRuntime
from jasi.tools.builtin import build_builtin_tool_registry
from jasi.tools.filesystem import FileWorkspace
from jasi.tools.sandbox import BubblewrapSandbox
from jasi.tools.shell import CommandTaskManager

logger = logging.getLogger(__name__)


async def run() -> None:
    try:
        settings = load_settings()
    except SettingsError as exc:
        raise SystemExit(str(exc)) from exc

    configure_logging(settings.log_level)
    engine = create_engine(settings.database_url)
    memory_embedding: OpenAICompatibleEmbedding | None = None
    command_tasks: CommandTaskManager | None = None
    try:
        await check_database_ready(engine)

        session_factory = create_session_factory(engine)
        repository = SQLAlchemyRepository(session_factory)
        work_repository = SQLAlchemyWorkRepository(session_factory)
        schedule_repository = SQLAlchemyScheduleRepository(session_factory)
        source_repository = SQLAlchemySourceRepository(session_factory)
        initiative_repository = SQLAlchemyInitiativeRepository(session_factory)
        effect_repository = SQLAlchemyEffectRepository(session_factory)
        memory_repository = SQLAlchemyMemoryRepository(session_factory)
        channel = TelegramBotClient(
            bot_token=settings.telegram_bot_token,
            request_timeout_seconds=30,
        )
        outbox_wakeup = asyncio.Event()
        memory_wakeup = asyncio.Event()
        outbox_dispatcher = OutboxDispatcher(
            repository=repository,
            channels={"telegram": channel},
            delivery_wakeup=memory_wakeup,
        )
        outbox_worker = OutboxWorker(
            repository=repository,
            dispatcher=outbox_dispatcher,
            batch_size=settings.outbox_batch_size,
            wakeup=outbox_wakeup,
        )
        model = OpenAICompatibleModel(
            base_url=settings.openai_base_url,
            api_key=settings.openai_api_key,
            timeout_seconds=settings.model_timeout_seconds,
        )
        light_model = OpenAICompatibleModel(
            base_url=settings.light_model_base_url,
            api_key=settings.light_model_api_key,
            timeout_seconds=settings.light_model_timeout_seconds,
        )
        memory_store = MarkdownMemoryStore(Path(settings.memory_root))
        if settings.memory_embedding_base_url is not None:
            if settings.memory_embedding_api_key is None:
                raise SettingsError("memory embedding API key is missing")
            memory_embedding = OpenAICompatibleEmbedding(
                base_url=settings.memory_embedding_base_url,
                api_key=settings.memory_embedding_api_key,
                model=settings.memory_embedding_model,
                dimensions=EMBEDDING_DIMENSIONS,
                timeout_seconds=settings.memory_embedding_timeout_seconds,
            )
        memory_reasoner = ModelMemoryReasoner(
            model=light_model,
            model_name=settings.light_model,
            timeout_seconds=settings.light_model_timeout_seconds,
        )
        logger.info(
            "model routing configured runtime=%s light=%s embedding=%s",
            settings.openai_model,
            settings.light_model,
            settings.memory_embedding_model if memory_embedding is not None else "disabled",
        )
        memory_indexer = MarkdownMemoryIndexer(
            store=memory_store,
            repository=memory_repository,
            embedding=memory_embedding,
        )
        memory_context = MemoryContextService(
            repository=memory_repository,
            store=memory_store,
            embedding=memory_embedding,
            reasoner=memory_reasoner,
            search_limit=settings.memory_search_limit,
            inject_limit=settings.memory_inject_limit,
            score_threshold=settings.memory_score_threshold,
            max_context_chars=settings.memory_max_context_chars,
        )
        memory_worker = MemoryWorker(
            repository=memory_repository,
            index_repository=memory_repository,
            store=memory_store,
            indexer=memory_indexer,
            consolidator=MemoryConsolidator(
                store=memory_store,
                repository=memory_repository,
                indexer=memory_indexer,
                reasoner=memory_reasoner,
                history_keep_count=PASSIVE_PROFILE.history_limit,
            ),
            wakeup=memory_wakeup,
            batch_size=settings.memory_job_batch_size,
            consolidation_batch_messages=settings.memory_consolidation_batch_messages,
            lease_seconds=settings.memory_job_lease_seconds,
            reconcile_seconds=settings.memory_reconcile_seconds,
        )
        file_workspace = FileWorkspace(Path(settings.tool_workspace))
        command_sandbox = BubblewrapSandbox(
            file_workspace,
            allow_network=settings.shell_network_enabled,
        )
        command_tasks = CommandTaskManager(file_workspace, sandbox=command_sandbox)
        if not command_tasks.sandbox_available:
            logger.warning(
                "sandboxed shell is unavailable: %s",
                command_tasks.sandbox_unavailable_reason,
            )
        if settings.web_allow_fake_ip_dns:
            logger.warning("web fake-IP DNS compatibility is enabled for 198.18.0.0/15")
        tools = build_builtin_tool_registry(
            workspace=file_workspace,
            messages=repository,
            processes=LocalProcessInspector(),
            command_tasks=command_tasks,
            allow_fake_ip_dns=settings.web_allow_fake_ip_dns,
        )
        prompt_catalog = PromptCatalog.load(
            Path(settings.prompt_dir),
            {"passive", "proactive", "scheduled", "drift"},
        )
        runtime = AgentRuntime(
            profiles={
                PASSIVE_PROFILE.name: PASSIVE_PROFILE,
                PROACTIVE_PROFILE.name: PROACTIVE_PROFILE,
                SCHEDULED_PROFILE.name: SCHEDULED_PROFILE,
                DRIFT_PROFILE.name: DRIFT_PROFILE,
            },
            model=model,
            repository=repository,
            context_provider=TurnContextProvider(
                repository=repository,
                memory=memory_context,
            ),
            prompt_assembler=PromptAssembler(prompt_catalog),
            tools=tools,
            model_name=settings.openai_model,
            model_timeout_seconds=settings.model_timeout_seconds,
            timezone=settings.timezone,
        )
        work_wakeup = asyncio.Event()
        work_dispatcher = WorkDispatcher(
            {
                "agent": AgentWorkHandler(
                    runtime=runtime,
                    conversations=repository,
                ),
                "direct": DirectWorkHandler(repository),
            }
        )
        work_worker = WorkWorker(
            repository=work_repository,
            dispatcher=work_dispatcher,
            finalizer=WorkFinalizer(
                repository=work_repository,
                outbound_policies={"telegram": TelegramOutboundPolicy()},
                outbox_wakeup=outbox_wakeup,
            ),
            batch_size=settings.work_batch_size,
            wakeup=work_wakeup,
            lease_seconds=settings.work_lease_seconds,
            heartbeat_seconds=settings.work_heartbeat_seconds,
            background_limit=settings.work_background_concurrency,
        )
        service = PassiveIngressService(
            repository=work_repository,
            work_wakeup=work_wakeup,
            memory_scope_map=settings.memory_scope_map,
        )
        schedule_wakeup = asyncio.Event()
        schedule_worker = ScheduleWorker(
            repository=schedule_repository,
            batch_size=settings.schedule_batch_size,
            schedule_wakeup=schedule_wakeup,
            work_wakeup=work_wakeup,
            poll_seconds=settings.schedule_poll_seconds,
        )
        source_wakeup = asyncio.Event()
        proactive_wakeup = asyncio.Event()
        drift_wakeup = asyncio.Event()
        effect_wakeup = asyncio.Event()
        effect_worker = EffectWorker(
            repository=effect_repository,
            dispatcher=EffectDispatcher(repository=effect_repository, adapters={}),
            batch_size=settings.effect_batch_size,
            wakeup=effect_wakeup,
        )
        source_worker = SourceWorker(
            repository=source_repository,
            dispatcher=SourceDispatcher({}),
            batch_size=settings.source_batch_size,
            source_wakeup=source_wakeup,
            initiative_wakeup=proactive_wakeup,
            effect_wakeup=effect_wakeup,
        )
        proactive_planner = InitiativePlanner(
            kind="proactive",
            repository=initiative_repository,
            batch_size=settings.initiative_batch_size,
            initiative_wakeup=proactive_wakeup,
            work_wakeup=work_wakeup,
        )
        drift_planner = InitiativePlanner(
            kind="drift",
            repository=initiative_repository,
            batch_size=settings.drift_batch_size,
            initiative_wakeup=drift_wakeup,
            work_wakeup=work_wakeup,
        )
        telegram = TelegramLongPollingAdapter(
            bot_token=settings.telegram_bot_token,
            allowed_user_ids=settings.telegram_allowed_user_ids,
            poll_timeout_seconds=settings.telegram_poll_timeout_seconds,
            max_concurrency=settings.telegram_max_concurrency,
        )

        stop_event = asyncio.Event()
        _install_signal_handlers(stop_event)
        worker_tasks = [
            asyncio.create_task(memory_worker.run(stop_event), name="jasi-memory-worker"),
            asyncio.create_task(effect_worker.run(stop_event), name="jasi-effect-worker"),
            asyncio.create_task(source_worker.run(stop_event), name="jasi-source-worker"),
            asyncio.create_task(
                proactive_planner.run(stop_event),
                name="jasi-proactive-planner",
            ),
            asyncio.create_task(drift_planner.run(stop_event), name="jasi-drift-planner"),
            asyncio.create_task(schedule_worker.run(stop_event), name="jasi-schedule-worker"),
            asyncio.create_task(work_worker.run(stop_event), name="jasi-work-worker"),
            asyncio.create_task(outbox_worker.run(stop_event), name="jasi-outbox-worker"),
        ]
        try:
            await telegram.run(service, stop_event)
        finally:
            stop_event.set()
            source_wakeup.set()
            proactive_wakeup.set()
            drift_wakeup.set()
            effect_wakeup.set()
            schedule_wakeup.set()
            work_wakeup.set()
            outbox_wakeup.set()
            memory_wakeup.set()
            await asyncio.gather(*worker_tasks)
    except Exception as exc:
        logger.exception("jasi failed to start or run")
        raise SystemExit(str(exc)) from exc
    finally:
        if command_tasks is not None:
            await command_tasks.aclose()
        if memory_embedding is not None:
            await memory_embedding.aclose()
        await engine.dispose()


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop_event.set)
        except NotImplementedError:
            signal.signal(signum, lambda *_: stop_event.set())
