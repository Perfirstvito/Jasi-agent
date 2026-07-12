from __future__ import annotations

from jasi.application.operations import OperationsService
from jasi.domain.operations import OperationsSnapshot


async def test_operations_service_exposes_only_aggregate_state() -> None:
    class Repository:
        async def snapshot(self, now):
            return OperationsSnapshot(
                work={"pending": 2},
                outbox={"failed": 1},
                effects={},
                source_items={"new": 3},
                drift_opportunities={},
                due_schedules=1,
                due_sources=0,
                expired_work_leases=0,
                generated_at=now,
            )

    snapshot = await OperationsService(Repository()).snapshot()

    assert snapshot.work == {"pending": 2}
    assert snapshot.outbox == {"failed": 1}
    assert snapshot.generated_at.tzinfo is not None
