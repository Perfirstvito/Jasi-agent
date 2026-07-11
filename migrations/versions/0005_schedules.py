"""add durable schedules and occurrences

Revision ID: 0005_schedules
Revises: 0004_passive_work
Create Date: 2026-07-11 22:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0005_schedules"
down_revision = "0004_passive_work"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "scheduled_jobs",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("dedupe_key", sa.Text(), nullable=False),
        sa.Column("session_id", sa.Text(), nullable=False),
        sa.Column("conversation_id", sa.BigInteger(), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("schedule_kind", sa.Text(), nullable=False),
        sa.Column("timezone", sa.Text(), nullable=False),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("input_text", sa.Text(), nullable=False),
        sa.Column("profile", sa.Text(), nullable=True),
        sa.Column("interval_seconds", sa.Integer(), nullable=True),
        sa.Column("cron_expression", sa.Text(), nullable=True),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="5"),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
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
        sa.CheckConstraint("action IN ('agent', 'direct')", name="ck_scheduled_jobs_action"),
        sa.CheckConstraint(
            "schedule_kind IN ('at', 'interval', 'cron')",
            name="ck_scheduled_jobs_kind",
        ),
        sa.CheckConstraint("btrim(input_text) <> ''", name="ck_scheduled_jobs_input"),
        sa.CheckConstraint("max_attempts > 0", name="ck_scheduled_jobs_max_attempts"),
        sa.CheckConstraint("version > 0", name="ck_scheduled_jobs_version"),
        sa.CheckConstraint(
            "action <> 'agent' OR (profile IS NOT NULL AND btrim(profile) <> '')",
            name="ck_scheduled_jobs_agent_profile",
        ),
        sa.CheckConstraint(
            "(schedule_kind = 'at' AND interval_seconds IS NULL AND cron_expression IS NULL) OR "
            "(schedule_kind = 'interval' AND interval_seconds IS NOT NULL "
            "AND interval_seconds > 0 AND cron_expression IS NULL) OR "
            "(schedule_kind = 'cron' AND interval_seconds IS NULL "
            "AND cron_expression IS NOT NULL AND btrim(cron_expression) <> '')",
            name="ck_scheduled_jobs_recurrence",
        ),
        sa.ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name="fk_scheduled_jobs_conversation_id",
        ),
        sa.UniqueConstraint("dedupe_key", name="uq_scheduled_jobs_dedupe_key"),
    )
    op.create_index(
        "ix_scheduled_jobs_due",
        "scheduled_jobs",
        ["enabled", "next_run_at"],
    )

    op.create_table(
        "schedule_occurrences",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("job_id", sa.BigInteger(), nullable=False),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column("work_item_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["scheduled_jobs.id"],
            name="fk_schedule_occurrences_job_id",
        ),
        sa.ForeignKeyConstraint(
            ["work_item_id"],
            ["work_items.id"],
            name="fk_schedule_occurrences_work_item_id",
        ),
        sa.UniqueConstraint(
            "job_id",
            "scheduled_for",
            name="uq_schedule_occurrences_job_time",
        ),
        sa.UniqueConstraint("work_item_id", name="uq_schedule_occurrences_work_item_id"),
    )
    op.create_index(
        "ix_schedule_occurrences_job_id",
        "schedule_occurrences",
        ["job_id", "scheduled_for"],
    )


def downgrade() -> None:
    op.drop_index("ix_schedule_occurrences_job_id", table_name="schedule_occurrences")
    op.drop_table("schedule_occurrences")
    op.drop_index("ix_scheduled_jobs_due", table_name="scheduled_jobs")
    op.drop_table("scheduled_jobs")
