from __future__ import annotations

import asyncio
import os
import uuid

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("JASI_TEST_DATABASE_URL"),
        reason="set JASI_TEST_DATABASE_URL to run PostgreSQL integration tests",
    ),
]


@pytest.mark.asyncio
async def test_operations_snapshot_reports_expired_lease_without_payloads() -> None:
    from alembic import command
    from alembic.config import Config

    from jasi.adapters.persistence.postgres.db import create_engine, create_session_factory
    from jasi.adapters.persistence.postgres.operations_repository import (
        SQLAlchemyOperationsRepository,
    )
    from jasi.adapters.persistence.postgres.work_repository import SQLAlchemyWorkRepository
    from jasi.domain.work import WorkExecutionResult, WorkSpec

    database_url = os.environ["JASI_TEST_DATABASE_URL"]
    os.environ["JASI_DATABASE_URL"] = database_url
    await asyncio.to_thread(command.upgrade, Config("alembic.ini"), "head")

    engine = create_engine(database_url)
    session_factory = create_session_factory(engine)
    work_repo = SQLAlchemyWorkRepository(session_factory)
    operations = SQLAlchemyOperationsRepository(session_factory)
    try:
        suffix = uuid.uuid4().hex
        queued = await work_repo.enqueue_work(
            WorkSpec(
                kind="passive",
                action="agent",
                dedupe_key=f"operations:{suffix}",
                session_id=f"operations:{suffix}",
                profile="passive",
                input_text="must not appear in snapshot",
                priority=1_000_000,
            )
        )
        claimed = await work_repo.claim_work_batch(100, lease_seconds=0.01)
        own = next(row for row in claimed if row.id == queued.work.id)
        for row in claimed:
            if row.id != own.id:
                await work_repo.complete_work(
                    row.id,
                    row.lease_token or "",
                    WorkExecutionResult(),
                    (),
                )
        await asyncio.sleep(0.02)

        snapshot = await operations.snapshot(own.lease_until)

        assert snapshot.work.get("running", 0) >= 1
        assert snapshot.expired_work_leases >= 1
        assert snapshot.generated_at == own.lease_until
        assert "must not appear in snapshot" not in repr(snapshot)

        reclaimed = await work_repo.claim_work_batch(100, lease_seconds=60)
        own_reclaimed = next(row for row in reclaimed if row.id == own.id)
        for row in reclaimed:
            await work_repo.complete_work(
                row.id,
                row.lease_token or "",
                WorkExecutionResult(),
                (),
            )
        assert own_reclaimed.lease_token != own.lease_token
    finally:
        await engine.dispose()
