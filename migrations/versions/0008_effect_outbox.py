"""add durable external effect outbox

Revision ID: 0008_effect_outbox
Revises: 0007_drift_opportunities
Create Date: 2026-07-12 01:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0008_effect_outbox"
down_revision = "0007_drift_opportunities"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "effect_outbox",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("adapter", sa.Text(), nullable=False),
        sa.Column("operation", sa.Text(), nullable=False),
        sa.Column("dedupe_key", sa.Text(), nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "result",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
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
        sa.CheckConstraint("btrim(adapter) <> ''", name="ck_effect_outbox_adapter"),
        sa.CheckConstraint("btrim(operation) <> ''", name="ck_effect_outbox_operation"),
        sa.CheckConstraint("attempts >= 0", name="ck_effect_outbox_attempts"),
        sa.CheckConstraint(
            "status IN ('pending', 'executing', 'succeeded', 'failed')",
            name="ck_effect_outbox_status",
        ),
        sa.UniqueConstraint("dedupe_key", name="uq_effect_outbox_dedupe_key"),
    )
    op.create_index(
        "ix_effect_outbox_ready",
        "effect_outbox",
        ["status", "next_attempt_at", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_effect_outbox_ready", table_name="effect_outbox")
    op.drop_table("effect_outbox")
