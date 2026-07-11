from __future__ import annotations

import asyncio
import logging
import signal

from jasi.adapters.channels.telegram import (
    TelegramBotClient,
    TelegramLongPollingAdapter,
    TelegramOutboundPolicy,
)
from jasi.adapters.llm.openai_compatible import OpenAICompatibleModel
from jasi.adapters.persistence.postgres.db import (
    check_database_ready,
    create_engine,
    create_session_factory,
)
from jasi.adapters.persistence.postgres.initiative_repository import (
    SQLAlchemyInitiativeRepository,
)
from jasi.adapters.persistence.postgres.repository import SQLAlchemyRepository
from jasi.adapters.persistence.postgres.schedule_repository import SQLAlchemyScheduleRepository
from jasi.adapters.persistence.postgres.source_repository import SQLAlchemySourceRepository
from jasi.adapters.persistence.postgres.work_repository import SQLAlchemyWorkRepository
from jasi.application.agent_work import AgentWorkHandler
from jasi.application.direct_work import DirectWorkHandler
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
from jasi.runtime.runtime import AgentRuntime
from jasi.tools.registry import ToolRegistry
from jasi.tools.time import get_current_time_tool

logger = logging.getLogger(__name__)


async def run() -> None:
    try:
        settings = load_settings()
    except SettingsError as exc:
        raise SystemExit(str(exc)) from exc

    configure_logging(settings.log_level)
    engine = create_engine(settings.database_url)
    try:
        await check_database_ready(engine)

        session_factory = create_session_factory(engine)
        repository = SQLAlchemyRepository(session_factory)
        work_repository = SQLAlchemyWorkRepository(session_factory)
        schedule_repository = SQLAlchemyScheduleRepository(session_factory)
        source_repository = SQLAlchemySourceRepository(session_factory)
        initiative_repository = SQLAlchemyInitiativeRepository(session_factory)
        channel = TelegramBotClient(
            bot_token=settings.telegram_bot_token,
            request_timeout_seconds=30,
        )
        outbox_wakeup = asyncio.Event()
        outbox_dispatcher = OutboxDispatcher(
            repository=repository,
            channels={"telegram": channel},
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
        tools = ToolRegistry([get_current_time_tool])
        runtime = AgentRuntime(
            profiles={
                PASSIVE_PROFILE.name: PASSIVE_PROFILE,
                PROACTIVE_PROFILE.name: PROACTIVE_PROFILE,
                SCHEDULED_PROFILE.name: SCHEDULED_PROFILE,
                DRIFT_PROFILE.name: DRIFT_PROFILE,
            },
            model=model,
            repository=repository,
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
        )
        service = PassiveIngressService(
            repository=work_repository,
            work_wakeup=work_wakeup,
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
        source_worker = SourceWorker(
            repository=source_repository,
            dispatcher=SourceDispatcher({}),
            batch_size=settings.source_batch_size,
            source_wakeup=source_wakeup,
            initiative_wakeup=proactive_wakeup,
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
            schedule_wakeup.set()
            work_wakeup.set()
            outbox_wakeup.set()
            await asyncio.gather(*worker_tasks)
    except Exception as exc:
        logger.exception("jasi failed to start or run")
        raise SystemExit(str(exc)) from exc
    finally:
        await engine.dispose()


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop_event.set)
        except NotImplementedError:
            signal.signal(signum, lambda *_: stop_event.set())
