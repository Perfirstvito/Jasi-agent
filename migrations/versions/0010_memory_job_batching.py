"""allow one leased batch to contain multiple memory jobs

Revision ID: 0010_memory_job_batching
Revises: 0009_passive_memory
Create Date: 2026-07-12 10:00:00.000000
"""

from __future__ import annotations

from alembic import op

revision = "0010_memory_job_batching"
down_revision = "0009_passive_memory"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("uq_memory_jobs_running_scope", table_name="memory_jobs")


def downgrade() -> None:
    # Existing duplicate running rows must be resolved before downgrading.
    op.create_index(
        "uq_memory_jobs_running_scope",
        "memory_jobs",
        ["scope_id"],
        unique=True,
        postgresql_where="status = 'running'",
    )
