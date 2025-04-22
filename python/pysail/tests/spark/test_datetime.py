import platform
from datetime import datetime, timezone

import pytest
from pyspark.sql.functions import lit
from pyspark.sql.types import Row

from pysail.tests.spark.utils import strict


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
def test_datetime_literal(sail, session_timezone, local_timezone, data):  # noqa: ARG001
    for dt, k, v in data:
        assert sail.range(1).select(lit(dt)).collect() == [strict(Row(**{f"TIMESTAMP '{k}'": v}))]
