"""Saved scalar-subquery regressions, using explicit Spark/relational expectations.

The corpus includes unsuccessful historical queries: known Sail bugs execute as
xfails, while unresolved expectations have an explicit skip reason. See the data
file's README for provenance and the deliberately different Spark expectations.
"""

import json
import re
from collections import Counter
from datetime import timezone
from pathlib import Path

import pytest
from pyspark.sql.types import TimestampType, YearMonthIntervalType

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

CASES = [
    json.loads(line)
    for line in Path(__file__).with_name("data").joinpath("scalar_subquery.jsonl").read_text().splitlines()
]

pytestmark = [
    pytest.mark.timeout(30),
    pytest.mark.skipif(
        pyspark_version() < (4, 2),
        reason="This historical corpus records Spark 4.2 analysis and optimizer behavior",
    ),
]


@pytest.fixture(scope="module", params=["spark"] if is_jvm_spark() else ["local", "local-cluster"])
def execution_mode(request):
    return request.param


@pytest.fixture(scope="module")
def remote(execution_mode):
    # One attempt preserves the query's original error instead of a retry conflict.
    envs = (
        None
        if execution_mode == "spark"
        else {
            "SAIL_MODE": execution_mode,
            "SAIL_CLUSTER__TASK_MAX_ATTEMPTS": "1",
        }
    )
    with spark_connect_server(envs=envs) as server:
        yield server.remote


def _rows(frame):
    if any(isinstance(field.dataType, YearMonthIntervalType) for field in frame.schema):
        # PySpark cannot collect month intervals. Format only the output value;
        # the saved query still performs its original scalar-subquery operations.
        frame = frame.select(
            *[
                frame[field.name].cast("string")
                if isinstance(field.dataType, YearMonthIntervalType)
                else frame[field.name]
                for field in frame.schema
            ]
        )
    result = []
    for row in frame.collect():
        values = []
        for value, field in zip(row, frame.schema, strict=True):
            # collect() renders LTZ timestamps in the process timezone; the corpus
            # uses UTC instants. NTZ values already represent local calendar time.
            if value is not None and isinstance(field.dataType, TimestampType):
                value = value.astimezone(timezone.utc).replace(tzinfo=None)
            values.append(None if value is None else str(value))
        result.append(values)
    return result


def _assert_rows(rows, expected, ordered):
    if "allowed_rows" in expected:
        if ordered:
            assert rows in expected["allowed_rows"]
        else:
            assert any(
                Counter(map(tuple, rows)) == Counter(map(tuple, allowed)) for allowed in expected["allowed_rows"]
            )
        return
    if "check" not in expected:
        if ordered:
            assert rows == expected["rows"]
        else:
            assert Counter(map(tuple, rows)) == Counter(map(tuple, expected["rows"]))
        return

    check = expected["check"]
    if check == "nullable_true_rows":
        assert len(rows) == expected["count"]
        assert all(row in ([None], ["True"]) for row in rows)
    elif check == "random_pair":
        assert len(rows) == 1 and len(rows[0]) == expected["width"]
        first, second = map(float, rows[0][:2])
        assert 0 <= first < 1 and 0 <= second < 1 and first != second
        if expected["width"] == 4:
            assert rows[0][2:] == ["1", "1"]
    elif check == "numeric_bounds":
        assert len(rows) == expected["row_count"]
        assert all(len(row) == expected["columns"] for row in rows)
        assert all(expected["minimum"] <= float(value) < expected["maximum_exclusive"] for row in rows for value in row)
    elif check == "shared_random_rows":
        assert [row[0] for row in rows] == ["0", "1", "2"]
        values = [float(row[1]) for row in rows]
        assert all(0 <= value < 1 for value in values) and len(set(values)) == 1
    elif check == "shared_uuid_rows":
        assert [row[0] for row in rows] == ["0", "1", "2"]
        assert all(re.fullmatch(r"[0-9a-fA-F-]{36}:" + row[0], row[1]) for row in rows)
        assert len({row[1].rsplit(":", 1)[0] for row in rows}) == 1
    elif check == "shared_random_struct_rows":
        assert [row[0] for row in rows] == ["0", "1", "2"]
        values = []
        for row in rows:
            match = re.fullmatch(r"Row\(value=([0-9.e+-]+), id=([012])\)", row[1])
            assert match is not None and match[2] == row[0]
            values.append(float(match[1]))
        assert all(0 <= value < 1 for value in values) and len(set(values)) == 1
    else:
        pytest.fail(f"Unknown corpus invariant: {check}")


@pytest.mark.parametrize("case", CASES, ids=[case["id"] for case in CASES])
def test_scalar_subquery_corpus(spark, execution_mode, case, request):
    expected = case.get("spark", case["expect"]) if is_jvm_spark() else case["expect"]
    if "skip" in expected:
        pytest.skip(expected["skip"])
    if reason := case.get("sail_bug", {}).get(execution_mode):
        # Independent fixes can report XPASS without making this corpus patch incompatible.
        request.applymarker(pytest.mark.xfail(reason=reason, strict=False))

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", case.get("ansi", "true"))
    if "error" in expected:
        with pytest.raises(Exception, match=expected["error"]):
            spark.sql(case["sql"]).collect()
        return

    frame = spark.sql(case["sql"])
    _assert_rows(_rows(frame), expected, case.get("ordered", False))
