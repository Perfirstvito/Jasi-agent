from __future__ import annotations

import asyncio
import os
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("JASI_TEST_DATABASE_URL"),
        reason="set JASI_TEST_DATABASE_URL to run PostgreSQL integration tests",
    ),
]


@pytest.mark.asyncio
async def test_schedule_materialization_is_unique_recoverable_and_uses_outbox() -> None:
    from alembic import command
    from alembic.config import Config
    from sqlalchemy import func, select
    from sqlalchemy.exc import StatementError

    from jasi.adapters.channels.telegram import TelegramOutboundPolicy
    from jasi.adapters.persistence.postgres.db import (
        Conversation,
        ScheduledJob,
        ScheduleOccurrence,
        WorkItem,
        create_engine,
        create_session_factory,
    )
    from jasi.adapters.persistence.postgres.repository import SQLAlchemyRepository
    from jasi.adapters.persistence.postgres.schedule_repository import (
        SQLAlchemyScheduleRepository,
    )
    from jasi.adapters.persistence.postgres.work_repository import SQLAlchemyWorkRepository
    from jasi.application.direct_work import DirectWorkHandler
    from jasi.application.outbox import OutboxDispatcher, OutboxWorker
    from jasi.application.work import WorkFinalizer
    from jasi.domain.schedule import ScheduleSpec
    from jasi.domain.work import WorkExecutionResult
    from tests.unit.fakes import FakeChannel

    database_url = os.environ["JASI_TEST_DATABASE_URL"]
    os.environ["JASI_DATABASE_URL"] = database_url
    await asyncio.to_thread(command.upgrade, Config("alembic.ini"), "head")

    engine = create_engine(database_url)
    session_factory = create_session_factory(engine)
    schedules = SQLAlchemyScheduleRepository(session_factory)
    work_repo = SQLAlchemyWorkRepository(session_factory)
    repository = SQLAlchemyRepository(session_factory)
    try:
        suffix = uuid.uuid4().hex
        async with session_factory.begin() as session:
            conversation = Conversation(
                channel="telegram",
                external_chat_id=f"schedule-chat-{suffix}",
            )
            session.add(conversation)
            await session.flush()
            conversation_id = conversation.id

        now = datetime.now(UTC).replace(microsecond=0)

        def spec(
            name: str,
            *,
            action: str = "direct",
            schedule_kind: str = "at",
            next_run_at: datetime | None = None,
            interval_seconds: int | None = None,
            cron_expression: str | None = None,
            profile: str | None = None,
        ) -> ScheduleSpec:
            return ScheduleSpec(
                dedupe_key=f"schedule-test:{suffix}:{name}",
                session_id=f"telegram:schedule-chat-{suffix}",
                conversation_id=conversation_id,
                action=action,
                schedule_kind=schedule_kind,
                timezone="Asia/Shanghai",
                next_run_at=next_run_at or now - timedelta(minutes=1),
                input_text=f"message-{name}",
                profile=profile,
                interval_seconds=interval_seconds,
                cron_expression=cron_expression,
            )

        one_time = await schedules.create_schedule(spec("once"))
        duplicate = await schedules.create_schedule(spec("once"))
        interval = await schedules.create_schedule(
            spec(
                "interval",
                schedule_kind="interval",
                next_run_at=now - timedelta(minutes=10),
                interval_seconds=60,
            )
        )
        cron = await schedules.create_schedule(
            spec(
                "cron",
                schedule_kind="cron",
                next_run_at=now - timedelta(minutes=5),
                cron_expression="*/5 * * * *",
            )
        )
        agent = await schedules.create_schedule(
            spec("agent", action="agent", profile="scheduled")
        )
        future = await schedules.create_schedule(
            spec("future", next_run_at=now + timedelta(days=1))
        )

        assert one_time.created is True
        assert duplicate.created is False
        assert duplicate.job.id == one_time.job.id
        assert await schedules.cancel_schedule(future.job.id) is True
        assert await schedules.cancel_schedule(future.job.id) is False

        first, second = await asyncio.gather(
            schedules.materialize_due(now, limit=1000),
            schedules.materialize_due(now, limit=1000),
        )
        expected_job_ids = {
            one_time.job.id,
            interval.job.id,
            cron.job.id,
            agent.job.id,
        }
        all_materialized = first + second
        materialized = [
            row
            for row in all_materialized
            if row.payload["schedule_job_id"] in expected_job_ids
        ]

        assert len(materialized) == 4
        assert len({row.id for row in materialized}) == 4
        assert {row.payload["schedule_job_id"] for row in materialized} == expected_job_ids
        assert {row.action for row in materialized} == {"direct", "agent"}
        assert next(row for row in materialized if row.action == "agent").profile == "scheduled"
        assert await schedules.materialize_due(now, limit=1000) == []

        stored_once = await schedules.get_schedule(one_time.job.id)
        stored_interval = await schedules.get_schedule(interval.job.id)
        stored_cron = await schedules.get_schedule(cron.job.id)
        assert stored_once is not None and stored_once.enabled is False
        assert stored_interval is not None and stored_interval.next_run_at > now
        assert stored_cron is not None and stored_cron.next_run_at > now

        async with session_factory() as session:
            occurrence_count = await session.scalar(
                select(func.count())
                .select_from(ScheduleOccurrence)
                .where(ScheduleOccurrence.job_id.in_(expected_job_ids))
            )
            work_count = await session.scalar(
                select(func.count())
                .select_from(WorkItem)
                .where(WorkItem.id.in_([row.id for row in materialized]))
            )
        assert occurrence_count == 4
        assert work_count == 4

        next_interval_time = stored_interval.next_run_at
        next_occurrences = await schedules.materialize_due(
            next_interval_time,
            limit=1000,
        )
        interval_occurrences = [
            row
            for row in next_occurrences
            if row.payload["schedule_job_id"] == interval.job.id
        ]
        assert len(interval_occurrences) == 1

        channel = FakeChannel()
        outbox_wakeup = asyncio.Event()
        finalizer = WorkFinalizer(
            repository=work_repo,
            outbound_policies={"telegram": TelegramOutboundPolicy()},
            outbox_wakeup=outbox_wakeup,
        )
        direct_handler = DirectWorkHandler(repository)
        while True:
            claimed = await work_repo.claim_work_batch(20, lease_seconds=60)
            if not claimed:
                break
            for work in claimed:
                if work.action == "direct":
                    await finalizer.complete(work, await direct_handler.execute(work))
                else:
                    await work_repo.complete_work(
                        work.id,
                        work.lease_token or "",
                        WorkExecutionResult(),
                        (),
                    )

        outbox_worker = OutboxWorker(
            repository=repository,
            dispatcher=OutboxDispatcher(
                repository=repository,
                channels={"telegram": channel},
            ),
            batch_size=20,
            wakeup=outbox_wakeup,
        )
        while await outbox_worker.drain_once():
            pass

        assert {message.text for message in channel.sent} >= {
            "message-once",
            "message-interval",
            "message-cron",
        }
        history = await repository.load_history(
            conversation_id,
            before_sequence=None,
            limit=30,
        )
        assert all(row.origin == "scheduled" for row in history)

        invalid = replace(spec("invalid-payload"), payload={"not_json": object()})
        with pytest.raises(StatementError):
            await schedules.create_schedule(invalid)
        async with session_factory() as session:
            invalid_count = await session.scalar(
                select(func.count())
                .select_from(ScheduledJob)
                .where(ScheduledJob.dedupe_key == invalid.dedupe_key)
            )
        assert invalid_count == 0

        restarted = SQLAlchemyScheduleRepository(session_factory)
        assert await restarted.materialize_due(now, limit=1000) == []
    finally:
        await engine.dispose()
