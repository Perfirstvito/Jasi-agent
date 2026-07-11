from __future__ import annotations

from jasi.adapters.persistence.postgres.db import WorkItem
from jasi.domain.work import WorkRecord


def work_record(row: WorkItem) -> WorkRecord:
    return WorkRecord(
        id=row.id,
        kind=row.kind,
        action=row.action,
        dedupe_key=row.dedupe_key,
        session_id=row.session_id,
        conversation_id=row.conversation_id,
        inbound_event_id=row.inbound_event_id,
        profile=row.profile,
        input_text=row.input_text,
        payload=dict(row.payload or {}),
        priority=row.priority,
        status=row.status,
        attempts=row.attempts,
        max_attempts=row.max_attempts,
        available_at=row.available_at,
        lease_token=row.lease_token,
        lease_until=row.lease_until,
        output_message_id=row.output_message_id,
        last_error=row.last_error,
        created_at=row.created_at,
        started_at=row.started_at,
        completed_at=row.completed_at,
        updated_at=row.updated_at,
    )
