"""add durable drift opportunities

Revision ID: 0007_drift_opportunities
Revises: 0006_proactive_sources
Create Date: 2026-07-12 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0007_drift_opportunities"
down_revision = "0006_proactive_sources"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "drift_opportunities",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("dedupe_key", sa.Text(), nullable=False),
        sa.Column("session_id", sa.Text(), nullable=False),
        sa.Column("conversation_id", sa.BigInteger(), nullable=False),
        sa.Column("input_text", sa.Text(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("profile", sa.Text(), nullable=False),
        sa.Column("min_idle_seconds", sa.Integer(), nullable=False),
        sa.Column("cooldown_seconds", sa.Integer(), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="20"),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("status", sa.Text(), nullable=False, server_default="new"),
        sa.Column("work_item_id", sa.BigInteger(), nullable=True),
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
        sa.CheckConstraint("btrim(input_text) <> ''", name="ck_drift_opportunities_input"),
        sa.CheckConstraint("btrim(profile) <> ''", name="ck_drift_opportunities_profile"),
        sa.CheckConstraint(
            "expires_at > available_at",
            name="ck_drift_opportunities_expiry",
        ),
        sa.CheckConstraint(
            "min_idle_seconds >= 0",
            name="ck_drift_opportunities_idle",
        ),
        sa.CheckConstraint(
            "cooldown_seconds >= 0",
            name="ck_drift_opportunities_cooldown",
        ),
        sa.CheckConstraint(
            "status IN ('new', 'enqueued', 'dismissed', 'expired')",
            name="ck_drift_opportunities_status",
        ),
        sa.ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name="fk_drift_opportunities_conversation_id",
        ),
        sa.ForeignKeyConstraint(
            ["work_item_id"],
            ["work_items.id"],
            name="fk_drift_opportunities_work_item_id",
        ),
        sa.UniqueConstraint("dedupe_key", name="uq_drift_opportunities_dedupe_key"),
        sa.UniqueConstraint("work_item_id", name="uq_drift_opportunities_work_item_id"),
    )
    op.create_index(
        "ix_drift_opportunities_candidates",
        "drift_opportunities",
        ["status", "available_at", "expires_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_drift_opportunities_candidates", table_name="drift_opportunities")
    op.drop_table("drift_opportunities")
