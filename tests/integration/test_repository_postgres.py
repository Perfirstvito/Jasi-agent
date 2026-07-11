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
async def test_repository_crud_idempotency_and_outbox_claim() -> None:
    from alembic import command
    from alembic.config import Config
    from sqlalchemy.exc import StatementError

    from jasi.adapters.persistence.postgres.db import (
        check_database_ready,
        create_engine,
        create_session_factory,
    )
    from jasi.adapters.persistence.postgres.repository import SQLAlchemyRepository
    from jasi.domain.models import InboundMessage, OutboundPart
    from jasi.runtime.models import ToolExecutionRecord, Usage

    database_url = os.environ["JASI_TEST_DATABASE_URL"]
    os.environ["JASI_DATABASE_URL"] = database_url

    alembic_config = Config("alembic.ini")
    await asyncio.to_thread(command.upgrade, alembic_config, "head")

    engine = create_engine(database_url)
    try:
        await check_database_ready(engine)
        repo = SQLAlchemyRepository(create_session_factory(engine))

        suffix = uuid.uuid4().hex
        inbound = InboundMessage(
            channel="feishu",
            external_update_id=f"update-{suffix}",
            external_chat_id=f"chat-{suffix}",
            external_user_id="42",
            text="hello",
        )
        claim = await repo.claim_inbound_message(inbound)
        resumed = await repo.claim_inbound_message(inbound)

        assert claim is not None
        assert resumed is not None
        assert resumed.event_id == claim.event_id
        assert resumed.message.id == claim.message.id
        assert claim.message.origin == inbound.channel

        turn_start = await repo.start_turn(
            conversation_id=claim.conversation.id,
            inbound_message_id=claim.message.id,
            profile="passive",
            model="test-model",
            metadata={},
        )
        turn_id = turn_start.turn_id
        await repo.record_tool_execution(
            turn_id,
            ToolExecutionRecord(
                name="get_current_time",
                arguments={},
                result={"iso8601": "2026-07-11T00:00:00+08:00"},
                risk="low",
                status="succeeded",
                duration_ms=1,
            ),
        )
        await repo.finish_turn(
            turn_id,
            "succeeded",
            "reply",
            2,
            Usage(total_tokens=3),
        )
        cached_turn = await repo.start_turn(
            conversation_id=claim.conversation.id,
            inbound_message_id=claim.message.id,
            profile="passive",
            model="test-model",
            metadata={},
        )

        assert cached_turn.cached_result is not None
        assert cached_turn.cached_result.final_text == "reply"
        assert len(cached_turn.cached_result.tool_records) == 1

        _, outbox = await repo.complete_inbound_response(
            inbound_event_id=claim.event_id,
            conversation_id=claim.conversation.id,
            channel=inbound.channel,
            external_chat_id=inbound.external_chat_id,
            turn_id=turn_id,
            text="reply",
            parts=(OutboundPart(text="reply"),),
            origin="model",
            metadata={},
        )
        duplicate = await repo.claim_inbound_message(inbound)

        assert duplicate is None

        first_claim, second_claim = await asyncio.gather(
            repo.claim_outbox_batch(10),
            repo.claim_outbox_batch(10),
        )
        claimed_once = first_claim + second_claim
        claimed_twice = await repo.claim_outbox_batch(10)

        assert [row.id for row in claimed_once] == [row.id for row in outbox]
        assert claimed_twice == []

        await repo.mark_outbox_sent(outbox[0].id, "tg-message-id")
        sent = await repo.get_outbox(outbox[0].id)
        assert sent is not None
        assert sent.status == "sent"

        rollback_inbound = InboundMessage(
            channel="telegram",
            external_update_id=f"rollback-{suffix}",
            external_chat_id=f"rollback-chat-{suffix}",
            external_user_id="42",
            text="rollback",
        )
        rollback_claim = await repo.claim_inbound_message(rollback_inbound)
        assert rollback_claim is not None
        with pytest.raises(StatementError):
            await repo.complete_inbound_response(
                inbound_event_id=rollback_claim.event_id,
                conversation_id=rollback_claim.conversation.id,
                channel=rollback_inbound.channel,
                external_chat_id=rollback_inbound.external_chat_id,
                turn_id=turn_id,
                text="must roll back",
                parts=(OutboundPart(text="must roll back"),),
                origin="model",
                metadata={"not_json": object()},
            )

        reclaimed = await repo.claim_inbound_message(rollback_inbound)
        assert reclaimed is not None
        assert reclaimed.message.id == rollback_claim.message.id
    finally:
        await engine.dispose()
