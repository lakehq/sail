import platform
from datetime import datetime, timezone

import pytest
from pyspark.sql.functions import lit
from pyspark.sql.types import Row

from pysail.testing.spark.utils.sql import strict


@pytest.mark.parametrize(
    ("session_timezone", "local_timezone", "data"),
    [
        (
            "America/Los_Angeles",
            "Asia/Shanghai",
            [
                (datetime(1970, 1, 1), "1969-12-31 08:00:00", datetime(1970, 1, 1)),  # noqa: DTZ001
                (datetime(1970, 1, 1, tzinfo=timezone.utc), "1969-12-31 16:00:00", datetime(1970, 1, 1, 8)),  # noqa: DTZ001
                (datetime(2024, 11, 3, 16, 30), "2024-11-03 01:30:00", datetime(2024, 11, 3, 16, 30)),  # noqa: DTZ001
                (datetime(2024, 11, 3, 17, 30), "2024-11-03 01:30:00", datetime(2024, 11, 3, 17, 30)),  # noqa: DTZ001
                (datetime(2025, 3, 9, 17, 30), "2025-03-09 01:30:00", datetime(2025, 3, 9, 17, 30)),  # noqa: DTZ001
                (datetime(2025, 3, 9, 18, 30), "2025-03-09 03:30:00", datetime(2025, 3, 9, 18, 30)),  # noqa: DTZ001
            ],
        ),
        (
            "America/Los_Angeles",
            "America/Los_Angeles",
            [
                (datetime(1970, 1, 1), "1970-01-01 00:00:00", datetime(1970, 1, 1)),  # noqa: DTZ001
                (datetime(1970, 1, 1, tzinfo=timezone.utc), "1969-12-31 16:00:00", datetime(1969, 12, 31, 16)),  # noqa: DTZ001
                # The PySpark client does not seem to respect the `fold` parameter for local `datetime` objects.
                # This is a bug on the client side, and no special handling is needed for the server.
                (datetime(2024, 11, 3, 1, 30), "2024-11-03 01:30:00", datetime(2024, 11, 3, 1, 30, fold=1)),  # noqa: DTZ001
                (datetime(2024, 11, 3, 1, 30, fold=0), "2024-11-03 01:30:00", datetime(2024, 11, 3, 1, 30, fold=1)),  # noqa: DTZ001
                (datetime(2024, 11, 3, 1, 30, fold=1), "2024-11-03 01:30:00", datetime(2024, 11, 3, 1, 30, fold=1)),  # noqa: DTZ001
                (datetime(2025, 3, 9, 2, 30), "2025-03-09 03:30:00", datetime(2025, 3, 9, 3, 30)),  # noqa: DTZ001
                (datetime(2025, 3, 9, 3, 30), "2025-03-09 03:30:00", datetime(2025, 3, 9, 3, 30)),  # noqa: DTZ001
            ],
        ),
    ],
    indirect=["session_timezone", "local_timezone"],
)
@pytest.mark.skipif(platform.system() == "Windows", reason="`time.tzset()` is not available on Windows")
def test_datetime_literal(spark, session_timezone, local_timezone, data):  # noqa: ARG001
    for dt, k, v in data:
        assert spark.range(1).select(lit(dt)).collect() == [strict(Row(**{f"TIMESTAMP '{k}'": v}))]


@pytest.mark.parametrize(
    ("session_timezone", "local_timezone", "sail_user_timezone", "data"),
    [
        (
            "UTC",
            "Asia/Shanghai",
            "UTC",
            [
                # Naive datetime: with sail.user.timeZone=UTC, treated as UTC regardless of OS timezone.
                # The stored value is 2026-06-01 12:00:00 UTC, and collect returns it as UTC naive.
                (datetime(2026, 6, 1, 12, 0, 0), "2026-06-01 12:00:00", datetime(2026, 6, 1, 12, 0, 0)),  # noqa: DTZ001
                # Aware UTC datetime: stored as 12:00 UTC, collected as UTC naive (not Shanghai naive).
                (
                    datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
                    "2026-06-01 12:00:00",
                    datetime(2026, 6, 1, 12, 0, 0),  # noqa: DTZ001
                ),
            ],
        ),
    ],
    indirect=["session_timezone", "local_timezone", "sail_user_timezone"],
)
@pytest.mark.skipif(platform.system() == "Windows", reason="`time.tzset()` is not available on Windows")
def test_user_timezone(spark, session_timezone, local_timezone, sail_user_timezone, data):  # noqa: ARG001
    """Test that ``sail.user.timeZone`` overrides the OS local timezone for datetime handling.

    When ``sail.user.timeZone=UTC`` is set alongside ``spark.sql.session.timeZone=UTC``,
    naive ``datetime`` objects should be ingested as UTC (not OS local time) and collected
    back as UTC-based naive ``datetime`` objects, regardless of the OS timezone.
    """
    for dt, show_value, collect_value in data:
        # Verify the stored value via lit() round-trip.
        assert spark.range(1).select(lit(dt)).collect() == [strict(Row(**{f"TIMESTAMP '{show_value}'": collect_value}))]

    # Verify createDataFrame round-trip with naive and aware datetimes.
    naive_dt = datetime(2026, 6, 1, 12, 0, 0)  # noqa: DTZ001
    aware_dt = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    df = spark.createDataFrame(
        [(1, naive_dt), (2, aware_dt)],
        "id INTEGER, ts TIMESTAMP",
    )
    rows = {row.id: row.ts for row in df.collect()}
    expected = datetime(2026, 6, 1, 12, 0, 0)  # noqa: DTZ001
    assert rows[1] == expected, f"Naive datetime round-trip failed: {rows[1]} != {expected}"
    assert rows[2] == expected, f"Aware UTC datetime collect failed: {rows[2]} != {expected}"
