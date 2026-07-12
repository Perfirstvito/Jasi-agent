"""add durable cross-chain work queue

Revision ID: 0003_durable_work
Revises: 0002_reliable_inbound
Create Date: 2026-07-11 20:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0003_durable_work"
down_revision = "0002_reliable_inbound"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "work_items",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("dedupe_key", sa.Text(), nullable=False),
        sa.Column("session_id", sa.Text(), nullable=False),
        sa.Column("conversation_id", sa.BigInteger(), nullable=True),
        sa.Column("inbound_event_id", sa.BigInteger(), nullable=True),
        sa.Column("profile", sa.Text(), nullable=True),
        sa.Column("input_text", sa.Text(), nullable=False, server_default=""),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_token", sa.Text(), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="5"),
        sa.Column("output_message_id", sa.BigInteger(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "kind IN ('passive', 'scheduled', 'proactive', 'drift')",
            name="ck_work_items_kind",
        ),
        sa.CheckConstraint(
            "action IN ('agent', 'direct')",
            name="ck_work_items_action",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'succeeded', 'failed', 'cancelled')",
            name="ck_work_items_status",
        ),
        sa.CheckConstraint("max_attempts > 0", name="ck_work_items_max_attempts"),
        sa.CheckConstraint("attempts >= 0", name="ck_work_items_attempts"),
        sa.CheckConstraint(
            "action <> 'agent' OR (profile IS NOT NULL AND btrim(profile) <> '')",
            name="ck_work_items_agent_profile",
        ),
        sa.ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name="fk_work_items_conversation_id",
        ),
        sa.ForeignKeyConstraint(
            ["inbound_event_id"],
            ["inbound_events.id"],
            name="fk_work_items_inbound_event_id",
        ),
        sa.ForeignKeyConstraint(
            ["output_message_id"],
            ["messages.id"],
            name="fk_work_items_output_message_id",
        ),
        sa.UniqueConstraint("dedupe_key", name="uq_work_items_dedupe_key"),
        sa.UniqueConstraint("inbound_event_id", name="uq_work_items_inbound_event_id"),
    )
    op.create_index(
        "ix_work_items_ready",
        "work_items",
        ["status", "available_at", "priority", "created_at"],
    )
    op.create_index(
        "ix_work_items_session_status",
        "work_items",
        ["session_id", "status"],
    )
    op.create_index(
        "uq_work_items_running_session",
        "work_items",
        ["session_id"],
        unique=True,
        postgresql_where=sa.text("status = 'running'"),
    )


def downgrade() -> None:
    op.drop_index("uq_work_items_running_session", table_name="work_items")
    op.drop_index("ix_work_items_session_status", table_name="work_items")
    op.drop_index("ix_work_items_ready", table_name="work_items")
    op.drop_table("work_items")
