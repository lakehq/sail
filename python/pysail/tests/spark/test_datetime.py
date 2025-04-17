import os
import time
from datetime import datetime, timezone

import pytest
from pyspark.sql.functions import lit
from pyspark.sql.types import Row


@pytest.fixture
def session_timezone(sail, request):
    tz = sail.conf.get("spark.sql.session.timeZone")
    sail.conf.set("spark.sql.session.timeZone", request.param)
    yield
    sail.conf.set("spark.sql.session.timeZone", tz)


@pytest.fixture
def local_timezone(request):
    tz = os.environ.get("TZ")
    os.environ["TZ"] = request.param
    time.tzset()
    yield
    if tz is None:
        os.environ.pop("TZ")
    else:
        os.environ["TZ"] = tz
    time.tzset()


@pytest.mark.parametrize(
    ("session_timezone", "local_timezone", "data"),
    [
        (
            "America/Los_Angeles",
            "Asia/Shanghai",
            [
                (datetime(1970, 1, 1), "1969-12-31 16:00:00", datetime(1970, 1, 1)),  # noqa: DTZ001
                (datetime(1970, 1, 1, tzinfo=timezone.utc), "1970-01-01 00:00:00", datetime(1970, 1, 1, 8)),  # noqa: DTZ001
            ],
        ),
        (
            "America/Los_Angeles",
            "America/Los_Angeles",
            [
                (datetime(1970, 1, 1), "1970-01-01 08:00:00", datetime(1970, 1, 1)),  # noqa: DTZ001
                (datetime(1970, 1, 1, tzinfo=timezone.utc), "1970-01-01 00:00:00", datetime(1969, 12, 31, 16)),  # noqa: DTZ001
            ],
        ),
    ],
    indirect=["session_timezone", "local_timezone"],
)
def test_datetime_literal(sail, session_timezone, local_timezone, data):  # noqa: ARG001
    for dt, k, v in data:
        assert sail.range(1).select(lit(dt)).collect() == [Row(**{f"TIMESTAMP '{k}'": v})]
