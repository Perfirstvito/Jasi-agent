"""add proactive sources and initiative state

Revision ID: 0006_proactive_sources
Revises: 0005_schedules
Create Date: 2026-07-11 23:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0006_proactive_sources"
down_revision = "0005_schedules"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "source_subscriptions",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("dedupe_key", sa.Text(), nullable=False),
        sa.Column("session_id", sa.Text(), nullable=False),
        sa.Column("conversation_id", sa.BigInteger(), nullable=False),
        sa.Column("profile", sa.Text(), nullable=False),
        sa.Column(
            "config",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "cursor",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("poll_interval_seconds", sa.Integer(), nullable=False),
        sa.Column("item_ttl_seconds", sa.Integer(), nullable=False),
        sa.Column("cooldown_seconds", sa.Integer(), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="40"),
        sa.Column("next_poll_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("lease_token", sa.Text(), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("poll_attempts", sa.Integer(), nullable=False, server_default="0"),
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
        sa.CheckConstraint("btrim(source) <> ''", name="ck_source_subscriptions_source"),
        sa.CheckConstraint("btrim(profile) <> ''", name="ck_source_subscriptions_profile"),
        sa.CheckConstraint(
            "poll_interval_seconds > 0",
            name="ck_source_subscriptions_poll_interval",
        ),
        sa.CheckConstraint("item_ttl_seconds > 0", name="ck_source_subscriptions_item_ttl"),
        sa.CheckConstraint("cooldown_seconds >= 0", name="ck_source_subscriptions_cooldown"),
        sa.CheckConstraint("poll_attempts >= 0", name="ck_source_subscriptions_attempts"),
        sa.ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name="fk_source_subscriptions_conversation_id",
        ),
        sa.UniqueConstraint("dedupe_key", name="uq_source_subscriptions_dedupe_key"),
    )
    op.create_index(
        "ix_source_subscriptions_due",
        "source_subscriptions",
        ["enabled", "next_poll_at", "lease_until"],
    )

    op.create_table(
        "source_items",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("subscription_id", sa.BigInteger(), nullable=False),
        sa.Column("external_id", sa.Text(), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
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
        sa.CheckConstraint("btrim(external_id) <> ''", name="ck_source_items_external_id"),
        sa.CheckConstraint("btrim(text) <> ''", name="ck_source_items_text"),
        sa.CheckConstraint(
            "status IN ('new', 'enqueued', 'dismissed', 'expired')",
            name="ck_source_items_status",
        ),
        sa.ForeignKeyConstraint(
            ["subscription_id"],
            ["source_subscriptions.id"],
            name="fk_source_items_subscription_id",
        ),
        sa.ForeignKeyConstraint(
            ["work_item_id"],
            ["work_items.id"],
            name="fk_source_items_work_item_id",
        ),
        sa.UniqueConstraint(
            "subscription_id",
            "external_id",
            name="uq_source_items_subscription_external_id",
        ),
        sa.UniqueConstraint("work_item_id", name="uq_source_items_work_item_id"),
    )
    op.create_index(
        "ix_source_items_candidates",
        "source_items",
        ["status", "expires_at", "occurred_at"],
    )

    op.create_table(
        "initiative_states",
        sa.Column("session_id", sa.Text(), primary_key=True),
        sa.Column("conversation_id", sa.BigInteger(), nullable=False),
        sa.Column("last_user_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_delivery_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_proactive_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_drift_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name="fk_initiative_states_conversation_id",
        ),
    )


def downgrade() -> None:
    op.drop_table("initiative_states")
    op.drop_index("ix_source_items_candidates", table_name="source_items")
    op.drop_table("source_items")
    op.drop_index("ix_source_subscriptions_due", table_name="source_subscriptions")
    op.drop_table("source_subscriptions")
