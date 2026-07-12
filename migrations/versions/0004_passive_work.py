"""route agent turns through durable work

Revision ID: 0004_passive_work
Revises: 0003_durable_work
Create Date: 2026-07-11 21:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0004_passive_work"
down_revision = "0003_durable_work"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("turns", sa.Column("work_item_id", sa.BigInteger(), nullable=True))
    op.create_foreign_key(
        "fk_turns_work_item_id",
        "turns",
        "work_items",
        ["work_item_id"],
        ["id"],
    )
    op.create_unique_constraint("uq_turns_work_item_id", "turns", ["work_item_id"])
    op.alter_column("turns", "inbound_message_id", nullable=True)

    op.drop_constraint("ck_inbound_events_status", "inbound_events", type_="check")
    op.create_check_constraint(
        "ck_inbound_events_status",
        "inbound_events",
        "status IN ('pending', 'processing', 'completed', 'failed')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_inbound_events_status", "inbound_events", type_="check")
    op.execute("UPDATE inbound_events SET status = 'pending' WHERE status = 'failed'")
    op.create_check_constraint(
        "ck_inbound_events_status",
        "inbound_events",
        "status IN ('pending', 'processing', 'completed')",
    )

    op.execute("DELETE FROM turns WHERE inbound_message_id IS NULL")
    op.alter_column("turns", "inbound_message_id", nullable=False)
    op.drop_constraint("uq_turns_work_item_id", "turns", type_="unique")
    op.drop_constraint("fk_turns_work_item_id", "turns", type_="foreignkey")
    op.drop_column("turns", "work_item_id")
