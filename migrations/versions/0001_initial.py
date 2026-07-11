"""initial passive chat schema

Revision ID: 0001_initial
Revises:
Create Date: 2026-07-11 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    op.create_table(
        "conversations",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("channel", sa.Text(), nullable=False),
        sa.Column("external_chat_id", sa.Text(), nullable=False),
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "channel", "external_chat_id", name="uq_conversations_channel_external_chat_id"
        ),
    )

    op.create_table(
        "messages",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("conversation_id", sa.BigInteger(), nullable=False),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column("origin", sa.Text(), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("delivery_status", sa.Text(), nullable=False, server_default="sent"),
        sa.Column("turn_id", sa.BigInteger(), nullable=True),
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("role IN ('user', 'assistant', 'system')", name="ck_messages_role"),
        sa.CheckConstraint(
            "delivery_status IN ('pending', 'sent', 'failed')", name="ck_messages_delivery_status"
        ),
        sa.UniqueConstraint(
            "conversation_id", "sequence", name="uq_messages_conversation_sequence"
        ),
    )
    op.create_index(
        "ix_messages_conversation_sequence", "messages", ["conversation_id", "sequence"]
    )

    op.create_table(
        "inbound_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("channel", sa.Text(), nullable=False),
        sa.Column("external_update_id", sa.Text(), nullable=False),
        sa.Column("conversation_id", sa.BigInteger(), nullable=True),
        sa.Column("message_id", sa.BigInteger(), nullable=True),
        sa.Column("external_user_id", sa.Text(), nullable=True),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "channel", "external_update_id", name="uq_inbound_events_channel_external_update_id"
        ),
    )

    op.create_table(
        "turns",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("conversation_id", sa.BigInteger(), nullable=False),
        sa.Column("inbound_message_id", sa.BigInteger(), nullable=False),
        sa.Column("profile", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("step_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "usage",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("error_code", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("status IN ('running', 'succeeded', 'failed')", name="ck_turns_status"),
    )
    op.create_index("ix_turns_conversation_started", "turns", ["conversation_id", "started_at"])

    op.create_table(
        "tool_executions",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("turn_id", sa.BigInteger(), nullable=False),
        sa.Column("tool_name", sa.Text(), nullable=False),
        sa.Column(
            "arguments",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "result",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("risk", sa.Text(), nullable=False, server_default="low"),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("duration_ms", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "status IN ('succeeded', 'failed', 'rejected')", name="ck_tool_executions_status"
        ),
    )
    op.create_index("ix_tool_executions_turn_id", "tool_executions", ["turn_id"])

    op.create_table(
        "outbox_messages",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("conversation_id", sa.BigInteger(), nullable=False),
        sa.Column("message_id", sa.BigInteger(), nullable=False),
        sa.Column("channel", sa.Text(), nullable=False),
        sa.Column("external_chat_id", sa.Text(), nullable=False),
        sa.Column("segment_index", sa.Integer(), nullable=False),
        sa.Column("segment_count", sa.Integer(), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("external_message_id", sa.Text(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'delivering', 'sent', 'failed')",
            name="ck_outbox_messages_status",
        ),
        sa.UniqueConstraint("message_id", "segment_index", name="uq_outbox_message_segment"),
    )
    op.create_index(
        "ix_outbox_ready", "outbox_messages", ["status", "next_attempt_at", "created_at"]
    )
    op.create_index("ix_outbox_message_id", "outbox_messages", ["message_id"])


def downgrade() -> None:
    op.drop_index("ix_outbox_message_id", table_name="outbox_messages")
    op.drop_index("ix_outbox_ready", table_name="outbox_messages")
    op.drop_table("outbox_messages")
    op.drop_index("ix_tool_executions_turn_id", table_name="tool_executions")
    op.drop_table("tool_executions")
    op.drop_index("ix_turns_conversation_started", table_name="turns")
    op.drop_table("turns")
    op.drop_table("inbound_events")
    op.drop_index("ix_messages_conversation_sequence", table_name="messages")
    op.drop_table("messages")
    op.drop_table("conversations")
