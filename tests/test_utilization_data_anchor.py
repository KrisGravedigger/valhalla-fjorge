from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

import valhalla.utilization as utilization
from valhalla.utilization import compute_hourly_utilization


def test_compute_hourly_utilization_defaults_to_latest_position_datetime(monkeypatch):
    positions = [
        SimpleNamespace(
            datetime_open="2026-02-10T10:15:00",
            datetime_close="2026-02-10T12:45:00",
            close_reason="normal",
            sol_deployed=Decimal("1.5"),
        ),
        SimpleNamespace(
            datetime_open="2026-02-10T13:00:00",
            datetime_close="2026-02-11T09:30:00",
            close_reason="normal",
            sol_deployed=Decimal("2.0"),
        ),
        SimpleNamespace(
            datetime_open="2026-02-11T08:00:00",
            datetime_close="",
            close_reason="still_open",
            sol_deployed=Decimal("0.75"),
        ),
    ]
    latest_close = datetime(2026, 2, 11, 9, 30, 0)

    expected = compute_hourly_utilization(positions, reference_time=latest_close)
    actual = compute_hourly_utilization(positions)

    assert actual == expected

    class FarFutureDateTime(datetime):
        @classmethod
        def now(cls):
            return cls(2099, 1, 1, 0, 0, 0)

    monkeypatch.setattr(utilization, "datetime", FarFutureDateTime)

    assert compute_hourly_utilization(positions) == expected
