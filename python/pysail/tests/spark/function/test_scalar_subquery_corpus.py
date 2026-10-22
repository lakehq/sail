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
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, YearMonthIntervalType
from pyspark.sql.pandas.types import from_arrow_schema

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


def _query_frame(spark, case):
    if "api" not in case:
        return spark.sql(case["sql"])
    api = case["api"]
    if api["kind"] == "grouping_count_metadata":
        outer = spark.range(2).alias("o")
        inner = spark.range(2).alias("i").filter(F.col("i.id") == F.col("o.id").outer())
        metadata = {"review": "retained"} if api["metadata"] else {}
        scalar = inner.groupingSets([[]]).agg(F.count("*").alias("v", metadata=metadata)).select("v").scalar()
        return outer.select("id", scalar.alias("v")).orderBy("id")
    assert api["kind"] == "scalar_alias_metadata", api
    outer = spark.range(2, 4).alias("o")
    inner = spark.range(2).alias("i").filter(F.col("i.id") == F.col("o.id").outer())
    value = F.lit(1.0) + F.coalesce(F.rand(), F.lit(0.0)) + F.count("*") * F.lit(0)
    named = value.alias("v", metadata={"review": "retained"}) if api["metadata"] else value.alias("v")
    scalar = inner.select(named).scalar()
    if api["wrapper"] == "coalesce":
        result = F.coalesce(scalar, F.lit(9.0))
    else:
        assert api["wrapper"] == "case", api
        # Bind the scalar once. Reusing its API plan directly in both CASE arms
        # would exercise a separate duplicate-plan-ID limitation.
        outer = outer.select("id", scalar.alias("v"))
        result = F.when(F.col("v").isNull(), F.lit(9.0)).otherwise(F.col("v"))
    return outer.select("id", (result > F.lit(0.0)).alias("positive")).orderBy("id")


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
    elif check == "projection_identity":
        assert len(rows) == expected["row_count"]
        assert [row[0] for row in rows] == expected["ordered_ids"]
        for left, right in expected["equal_columns"]:
            assert all(row[left] == row[right] for row in rows)
        for index in expected["constant_columns"]:
            assert len({row[index] for row in rows}) == 1
    elif check == "guarded_volatile_defaults":
        assert [row[:3] for row in rows] == [[None, "10", "10"], ["0", "11", "11"], ["1", "11", "11"]]
        assert rows[2][3] == "1"
        # Matched rows share the aggregate; missing keys evaluate its default.
        assert all(1 < int(row[3]) <= int(row[1]) for row in rows[:2])
    elif check == "partitioned_volatile_defaults":
        assert [row[:2] for row in rows] == [[str(index), str(index % 4)] for index in range(32)]
        assert all(len(row) == 4 for row in rows)
        assert all(row[2] is not None and 0 <= float(row[2]) < 1 for row in rows)
        assert all(
            row[3] is not None and re.fullmatch(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}0", row[3])
            for row in rows
        )
        for key in range(4):
            group = [row for row in rows if row[1] == str(key)]
            # Matched keys share a result; each missing row evaluates its default.
            assert all(len({row[column] for row in group}) == (1 if key < 2 else 8) for column in (2, 3))
    elif check == "volatile_aggregate_values":
        assert [row[0] for row in rows] == ["0", "1", "2", "3"]
        values = [row[1] for row in rows]
        assert all(value is not None for value in values)
        if expected["kind"] == "uuid":
            assert all(
                re.fullmatch(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}0", value) for value in values
            )
        elif expected["kind"] == "choice":
            assert all(value in ("1", "2") for value in values)
        else:
            assert expected["kind"] == "rand"
            assert all(0 <= float(value) < 1 for value in values)
    elif check == "filtered_volatile_alias":
        assert 0 < len(rows) <= 256
        ids = [int(row[0]) for row in rows]
        assert ids == sorted(set(ids)) and all(0 <= value < 256 for value in ids)
        assert all(len(row) == (3 if expected.get("repeated") else 2) for row in rows)
        values = [row[1] for row in rows]
        if expected["kind"] == "null":
            assert all(value is None for value in values)
        elif expected["kind"] == "count":
            assert all(value == "1" for value in values)
        elif expected["kind"] == "rand":
            assert all(value is not None and 0 <= float(value) < 0.5 for value in values)
        else:
            assert expected["kind"] == "uuid"
            pattern = r"[0-7][0-9a-fA-F]{7}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"
            assert all(
                value is not None and re.fullmatch(pattern + expected.get("suffix", ""), value) for value in values
            )
            if expected.get("repeated"):
                assert all(row[1] == row[2] for row in rows)
    else:
        pytest.fail(f"Unknown corpus invariant: {check}")


def _nullability_violations(value, data_type, nullable=True, path="result"):
    """Locate actual NULLs that contradict a schema, including nested fields."""
    if value is None:
        return [] if nullable else [path]
    if not isinstance(data_type, dict):
        return []
    errors = []
    if data_type["type"] == "struct":
        fields = data_type["fields"]
        values = [value[field["name"]] for field in fields] if isinstance(value, dict) else value
        for field, child in zip(fields, values, strict=True):
            errors.extend(_nullability_violations(child, field["type"], field["nullable"], f"{path}.{field['name']}"))
    elif data_type["type"] == "array":
        for child in value:
            errors.extend(
                _nullability_violations(child, data_type["elementType"], data_type["containsNull"], f"{path}[]")
            )
    elif data_type["type"] == "map":
        for key, child in value.items() if isinstance(value, dict) else value:
            errors.extend(_nullability_violations(key, data_type["keyType"], False, f"{path}.key"))
            errors.extend(
                _nullability_violations(child, data_type["valueType"], data_type["valueContainsNull"], f"{path}.value")
            )
    return errors


def _assert_nullability(value, data_type):
    errors = _nullability_violations(value, data_type)
    assert not errors, f"NULL in non-nullable fields: {errors}"


@pytest.mark.parametrize("case", CASES, ids=[case["id"] for case in CASES])
def test_scalar_subquery_corpus(spark, execution_mode, case, request):
    expected = case.get("spark", case["expect"]) if is_jvm_spark() else case["expect"]
    if "skip" in expected:
        pytest.skip(expected["skip"])
    reason = case.get("sail_bug", {}).get(execution_mode)
    known_error = case.get("sail_execution_error", {}).get(execution_mode) if reason else None
    if reason and not known_error:
        # Independent fixes can report XPASS without making this corpus patch incompatible.
        request.applymarker(pytest.mark.xfail(reason=reason, strict=False))

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", case.get("ansi", "true"))
    for sql in case.get("setup", []):
        spark.sql(sql).collect()

    def execute():
        try:
            frame = _query_frame(spark, case)
            return frame.collect() if "error" in expected else _rows(frame)
        except Exception as error:
            # A formerly unsupported query must fail normally if it starts
            # returning wrong rows or raises a different, newly introduced error.
            if known_error and re.search(known_error, str(error)):
                pytest.xfail(reason)
            raise

    if "error" in expected:
        with pytest.raises(Exception, match=expected["error"]):
            execute()
        return
    _assert_rows(execute(), expected, case.get("ordered", False))


SCHEMA_CASES = [case for case in CASES if "schema" in case]


@pytest.mark.parametrize("case", SCHEMA_CASES, ids=[case["id"] for case in SCHEMA_CASES])
def test_scalar_subquery_schema(spark, execution_mode, case):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", case.get("ansi", "true"))
    for sql in case.get("setup", []):
        spark.sql(sql).collect()
    try:
        frame = _query_frame(spark, case)
        logical = frame.schema.jsonValue()
        if is_jvm_spark():
            expected = case.get("spark", case["expect"])
            if "error" in expected:
                assert logical == case["schema"]
                with pytest.raises(Exception, match=expected["error"]):
                    frame.collect()
                return
            frame.collect()
            table = None
        else:
            # Connect collect() does not cache the execution schema.
            table, returned_schema = frame._to_table()  # noqa: SLF001
            arrow = from_arrow_schema(table.schema, prefer_timestamp_ntz=True).jsonValue()
            execution = returned_schema.jsonValue() if returned_schema else arrow
    except Exception as error:
        # A known unsupported query cannot reach schema validation. Match that
        # exact failure; a generic row xfail must never hide a new schema defect.
        pattern = case.get("sail_execution_error", {}).get(execution_mode)
        if pattern and re.search(pattern, str(error)):
            pytest.xfail("Known Sail execution failure prevents the schema check")
        raise

    known_failures = []
    if table is not None:
        # Preserve Sail's existing alias metadata through native default compensation.
        # Spark drops this metadata at the scalar boundary, so its schema stays separate.
        for name, metadata in case.get("sail_field_metadata", {}).items():
            for layer, schema in (("logical", logical), ("execution", execution), ("arrow", arrow)):
                field = next(field for field in schema["fields"] if field["name"] == name)
                assert field["metadata"] == metadata, f"{layer} lost {name} metadata"
        # Execution may truthfully narrow the public schema. Every declaration
        # must still permit the actual NULLs, including nested container values.
        rows = table.to_pylist()
        violations = {
            f"{layer}:{path}"
            for layer, schema in (("logical", logical), ("execution", execution), ("arrow", arrow))
            for row in rows
            for path in _nullability_violations(row, schema)
        }
        inherited = set(case.get("sail_nullability_bug", {}).get(execution_mode, []))
        assert not violations - inherited, f"New unsafe NULL declarations: {sorted(violations - inherited)}"
        if violations:
            known_failures.append(f"Inherited unsafe NULL declarations: {sorted(violations)}")
    # Apply public-schema exceptions only after checking emitted NULLs: an
    # inherited, wider schema must not hide a newly unsafe execution schema.
    inherited = case.get("sail_schema_bug", {}).get(execution_mode)
    for layer, schema in (("logical", logical), ("public after execution", frame.schema.jsonValue())):
        if schema != case["schema"] and inherited and schema == inherited["schema"]:
            known_failures.append(inherited["reason"])
        else:
            assert schema == case["schema"], f"{layer} scalar-subquery schema"
    if known_failures:
        pytest.xfail("; ".join(sorted(set(known_failures))))
