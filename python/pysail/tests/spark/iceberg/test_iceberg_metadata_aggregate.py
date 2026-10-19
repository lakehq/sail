import contextlib
import math
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.conversions import to_bytes
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat, ManifestContent
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import BinaryType, DecimalType, DoubleType, IntegerType, LongType, NestedField, StringType

from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.tests.spark.iceberg.test_iceberg_data_skipping import unavailable_iceberg_files
from pysail.tests.spark.iceberg.test_iceberg_equality_delete import _append_equality_delete_snapshot
from pysail.tests.spark.iceberg.test_iceberg_merge import _current_manifest_entries, _local_file_path
from pysail.tests.spark.iceberg.utils import create_sql_catalog


@pytest.fixture
def aggregate_table(tmp_path, request):
    field_type = getattr(request, "param", LongType())
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.metadata_aggregate"
    table = catalog.create_table(identifier, schema=Schema(NestedField(1, "v", field_type)))
    try:
        yield table
    finally:
        catalog.drop_table(identifier)


def _append_metric_file(table, tmp_path, name, values, arrow_type, *, omitted=(), partition=None, bounds=None):
    path = tmp_path / f"{name}.parquet"
    schema = pa.schema([pa.field("v", arrow_type, metadata={b"PARQUET:field_id": b"1"})])
    pq.write_table(pa.Table.from_arrays([pa.array(values, arrow_type)], schema=schema), path)
    ordered_values = [
        value for value in values if value is not None and not (isinstance(value, float) and math.isnan(value))
    ]
    field_type = table.schema().find_field("v").field_type
    metrics = {
        "value_counts": {1: len(values)},
        "null_value_counts": {1: sum(value is None for value in values)},
    }
    if isinstance(field_type, DoubleType):
        metrics["nan_value_counts"] = {1: sum(isinstance(value, float) and math.isnan(value) for value in values)}
    if ordered_values:
        lower, upper = bounds if bounds is not None else (min(ordered_values), max(ordered_values))
        metrics["lower_bounds"] = {1: to_bytes(field_type, lower)}
        metrics["upper_bounds"] = {1: to_bytes(field_type, upper)}
    for metric in omitted:
        metrics.pop(metric, None)
    file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=path.as_uri(),
        file_format=FileFormat.PARQUET,
        partition=partition if partition is not None else Record(),
        record_count=len(values),
        file_size_in_bytes=path.stat().st_size,
        spec_id=table.spec().spec_id,
        **metrics,
    )
    with table.transaction() as transaction, transaction.update_snapshot().fast_append() as append:
        append.append_data_file(file)
    return path.as_uri()


def _assert_aggregate_result(snapshot, frame, expressions, expected, *, predicate=None):
    query = (frame.filter(predicate) if predicate else frame).selectExpr(*expressions)
    assert [tuple(row) for row in query.collect()] == [expected]
    assert normalize_plan_text(query._explain_string()) == snapshot  # noqa: SLF001


AGGREGATES = ("COUNT(*) AS rows", "COUNT(v) AS present", "MIN(v) AS minimum", "MAX(v) AS maximum")
METRICS = ("value_counts", "null_value_counts", "nan_value_counts", "lower_bounds", "upper_bounds")


@pytest.mark.parametrize(
    "omitted",
    [
        (),
        ("value_counts",),
        ("null_value_counts",),
        ("lower_bounds",),
        ("upper_bounds",),
        ("lower_bounds", "upper_bounds"),
        METRICS,
    ],
    ids=["complete", "missing-values", "missing-nulls", "missing-min", "missing-max", "missing-bounds", "no-metrics"],
)
def test_aggregate_results_with_incomplete_metrics(spark, tmp_path, aggregate_table, omitted, snapshot):
    table = aggregate_table
    paths = [
        _append_metric_file(table, tmp_path, "first", [2, None], pa.int64()),
        _append_metric_file(table, tmp_path, "second", [3, 10], pa.int64(), omitted=omitted),
    ]
    frame = spark.read.format("iceberg").load(table.location())
    unresolved = set(omitted) & {"null_value_counts", "lower_bounds", "upper_bounds"}
    guard = contextlib.nullcontext() if unresolved else unavailable_iceberg_files(paths)
    with guard:
        _assert_aggregate_result(snapshot, frame, AGGREGATES, (4, 3, 2, 10))
    if unresolved:
        with unavailable_iceberg_files(paths), pytest.raises(Exception, match=r"(?i)not found|no such file"):
            frame.selectExpr(*AGGREGATES).collect()


@pytest.mark.parametrize("all_null", [False, True])
def test_null_files_do_not_require_bounds_or_data_reads(spark, tmp_path, aggregate_table, all_null, snapshot):
    table = aggregate_table
    paths = [
        _append_metric_file(table, tmp_path, "nulls", [None, None], pa.int64()),
        _append_metric_file(table, tmp_path, "values", [None, None] if all_null else [2, 10], pa.int64()),
    ]
    expected = (4, 0, None, None) if all_null else (4, 2, 2, 10)
    with unavailable_iceberg_files(paths):
        frame = spark.read.format("iceberg").load(table.location())
        _assert_aggregate_result(snapshot, frame, AGGREGATES, expected)


@pytest.mark.parametrize("aggregate_table", [StringType(), BinaryType()], indirect=True, ids=["string", "binary"])
def test_truncated_bounds_cannot_replace_extrema(spark, tmp_path, aggregate_table, snapshot):
    table = aggregate_table
    binary = isinstance(table.schema().find_field("v").field_type, BinaryType)
    values, bounds = ([b"aaaa", b"azzz"], (b"aa", b"b")) if binary else (["aaaa", "azzz"], ("aa", "b"))
    _append_metric_file(table, tmp_path, "truncated", values, pa.binary() if binary else pa.string(), bounds=bounds)
    frame = spark.read.format("iceberg").load(table.location())
    _assert_aggregate_result(snapshot, frame, AGGREGATES, (2, 2, values[0], values[1]))


@pytest.mark.parametrize("aggregate_table", [DoubleType()], indirect=True)
@pytest.mark.parametrize("nan_count", ["zero", "missing", "positive"])
def test_float_extrema_require_complete_nan_metrics(spark, tmp_path, aggregate_table, nan_count, snapshot):
    table = aggregate_table
    values = [1.0, float("nan")] if nan_count == "positive" else [1.0, 3.0]
    path = _append_metric_file(
        table,
        tmp_path,
        "floating",
        values,
        pa.float64(),
        omitted=("nan_value_counts",) if nan_count == "missing" else (),
    )
    expressions = ("COUNT(*) AS rows", "MIN(v) AS minimum", "isnan(MAX(v)) AS nan_max")
    expected = (2, 1.0, nan_count == "positive")
    guard = unavailable_iceberg_files([path]) if nan_count == "zero" else contextlib.nullcontext()
    with guard:
        frame = spark.read.format("iceberg").load(table.location())
        _assert_aggregate_result(snapshot, frame, expressions, expected)


@pytest.mark.parametrize("aggregate_table", [DecimalType(18, 0)], indirect=True)
def test_decimal_metadata_preserves_integer_precision(spark, tmp_path, aggregate_table, snapshot):
    values = [Decimal(9007199254740993), Decimal(9007199254740995)]
    path = _append_metric_file(aggregate_table, tmp_path, "decimal", values, pa.decimal128(18, 0))
    with unavailable_iceberg_files([path]):
        frame = spark.read.format("iceberg").load(aggregate_table.location())
        _assert_aggregate_result(snapshot, frame, AGGREGATES, (2, 2, values[0], values[1]))


def test_aggregates_apply_equality_deletes(spark, tmp_path, aggregate_table, snapshot):
    _append_metric_file(aggregate_table, tmp_path, "data", [1, 2, 3], pa.int64())
    _append_equality_delete_snapshot(aggregate_table, pa.table({"v": pa.array([1, 1, 99], pa.int64())}), [1])
    frame = spark.read.format("iceberg").load(aggregate_table.location())
    _assert_aggregate_result(snapshot, frame, AGGREGATES, (2, 2, 2, 3))


def test_partition_evolution_uses_each_files_spec(spark, tmp_path, aggregate_table, snapshot):
    table = aggregate_table
    previous = _append_metric_file(table, tmp_path, "unpartitioned", [2, 3], pa.int64())
    old_spec_id = table.spec().spec_id
    with table.update_spec() as update:
        update.add_identity("v")
    assert table.spec().spec_id != old_spec_id
    current = _append_metric_file(
        table, tmp_path, "partitioned", [20, 20], pa.int64(), omitted=METRICS, partition=Record(20)
    )
    with unavailable_iceberg_files([previous, current]):
        frame = spark.read.format("iceberg").load(table.location())
        _assert_aggregate_result(snapshot, frame, AGGREGATES, (2, 2, 20, 20), predicate="v = 20")
    _assert_aggregate_result(snapshot, frame, AGGREGATES, (1, 1, 3, 3), predicate="v = 3")


def test_snapshot_selection_pins_aggregate_statistics(spark, tmp_path, aggregate_table, snapshot):
    table = aggregate_table
    first = _append_metric_file(table, tmp_path, "previous", [2, None], pa.int64())
    snapshot_id = table.current_snapshot().snapshot_id
    second = _append_metric_file(table, tmp_path, "current", [3, 10], pa.int64())
    with unavailable_iceberg_files([first, second]):
        previous = spark.read.format("iceberg").option("snapshot-id", snapshot_id).load(table.location())
        current = spark.read.format("iceberg").load(table.location())
        _assert_aggregate_result(snapshot, previous, AGGREGATES, (2, 1, 2, 2))
        _assert_aggregate_result(snapshot, current, AGGREGATES, (4, 3, 2, 10))


@pytest.mark.parametrize("aggregate_table", [IntegerType()], indirect=True, ids=["int"])
@pytest.mark.parametrize("evolution", ["rename", "promote"])
def test_schema_evolution_resolves_metrics_by_field_id(spark, tmp_path, aggregate_table, evolution, snapshot):
    table = aggregate_table
    path = _append_metric_file(table, tmp_path, "original_schema", [2, None, 10], pa.int32())
    column = "renamed" if evolution == "rename" else "v"
    with table.update_schema() as update:
        if evolution == "rename":
            update.rename_column("v", column)
        else:
            update.update_column("v", field_type=LongType())
    assert table.schema().find_field(column).field_id == 1
    expressions = tuple(expression.replace("(v)", f"({column})") for expression in AGGREGATES)
    with unavailable_iceberg_files([path]):
        frame = spark.read.format("iceberg").load(table.location())
        _assert_aggregate_result(snapshot, frame, expressions, (3, 2, 2, 10))


def test_added_column_does_not_inherit_metrics_from_existing_fields(spark, tmp_path, aggregate_table, snapshot):
    table = aggregate_table
    _append_metric_file(table, tmp_path, "old_schema", [2, 10], pa.int64())
    with table.update_schema() as update:
        update.add_column("added", LongType())
    expressions = tuple(expression.replace("(v)", "(added)") for expression in AGGREGATES)
    frame = spark.read.format("iceberg").load(table.location())
    _assert_aggregate_result(snapshot, frame, expressions, (2, 0, None, None))


@pytest.mark.parametrize("null_file", [False, True], ids=["non-null", "null-file"])
def test_constant_file_casts_preserve_lexical_order(spark, tmp_path, aggregate_table, null_file, snapshot):
    paths = [
        _append_metric_file(aggregate_table, tmp_path, f"constant_{value}", [value, value], pa.int64())
        for value in [2, 3, 10]
    ]
    if null_file:
        paths.append(_append_metric_file(aggregate_table, tmp_path, "nulls", [None, None], pa.int64()))
    expressions = ("COUNT(*) AS rows", "MIN(CAST(v AS STRING)) AS minimum", "MAX(CAST(v AS STRING)) AS maximum")
    with unavailable_iceberg_files(paths):
        frame = spark.read.format("iceberg").load(aggregate_table.location())
        _assert_aggregate_result(snapshot, frame, expressions[1:], ("10", "3"))
        _assert_aggregate_result(snapshot, frame, ("COUNT(CAST(v AS STRING)) AS present",), (6,))
        _assert_aggregate_result(snapshot, frame, expressions, (8 if null_file else 6, "10", "3"))


def test_constant_file_cast_overflow_preserves_null_counts(spark, tmp_path, aggregate_table, snapshot):
    path = _append_metric_file(aggregate_table, tmp_path, "overflow", [128, 128], pa.int64())
    expressions = tuple(expression.replace("(v)", "(TRY_CAST(v AS TINYINT))") for expression in AGGREGATES)
    with unavailable_iceberg_files([path]):
        frame = spark.read.format("iceberg").load(aggregate_table.location())
        _assert_aggregate_result(snapshot, frame, (expressions[1],), (0,))
        _assert_aggregate_result(snapshot, frame, expressions, (2, 0, None, None))


def test_disjoint_delete_metrics_allow_exact_aggregation(spark, tmp_path, aggregate_table, snapshot):
    path = _append_metric_file(aggregate_table, tmp_path, "data", [2, 3], pa.int64())
    delete_value = 99
    location = _append_equality_delete_snapshot(
        aggregate_table,
        pa.table({"v": pa.array([delete_value], pa.int64())}),
        [1],
        metrics={
            "value_counts": {1: 1},
            "null_value_counts": {1: 0},
            "lower_bounds": {1: to_bytes(LongType(), delete_value)},
            "upper_bounds": {1: to_bytes(LongType(), delete_value)},
        },
    )
    deletes = _current_manifest_entries(location, ManifestContent.DELETES)
    assert len(deletes) == 1
    with unavailable_iceberg_files([path, deletes[0].data_file.file_path]):
        frame = spark.read.format("iceberg").load(aggregate_table.location())
        _assert_aggregate_result(snapshot, frame, AGGREGATES, (2, 2, 2, 3))


def test_aggregates_apply_position_deletes(spark, tmp_path, snapshot):
    location = tmp_path / "position_deletes"
    try:
        spark.sql(f"""CREATE TABLE metadata_agg_positions (v BIGINT) USING iceberg
            LOCATION '{location.as_uri()}'
            TBLPROPERTIES ('format-version'='2', 'write.merge.mode'='merge-on-read')""")
        spark.sql("INSERT INTO metadata_agg_positions VALUES (1), (2), (3)")
        spark.sql("""MERGE INTO metadata_agg_positions AS t USING (SELECT 1L AS v) AS s
            ON t.v = s.v WHEN MATCHED THEN DELETE""").collect()
        entries = _current_manifest_entries(location, ManifestContent.DELETES)
        assert len(entries) == 1
        assert entries[0].data_file.content == DataFileContent.POSITION_DELETES
        assert pq.read_table(_local_file_path(entries[0].data_file.file_path)).num_rows == 1
        _assert_aggregate_result(snapshot, spark.table("metadata_agg_positions"), AGGREGATES, (2, 2, 2, 3))
    finally:
        spark.sql("DROP TABLE IF EXISTS metadata_agg_positions")
