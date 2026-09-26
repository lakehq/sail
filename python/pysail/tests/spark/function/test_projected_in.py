import pyarrow as pa
import pytest
from pyspark.sql import Row

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version


@pytest.mark.skipif(pyspark_version() < (4,), reason="Arrow table input requires PySpark 4.0+")
@pytest.mark.parametrize("batch_size", [5000, 15000])
def test_local_projected_in_preserves_expression_state_across_batches(spark, batch_size):
    # SQL BDD cannot control the Arrow IPC batch boundaries of a local relation.
    batches = [
        pa.record_batch([pa.array(range(start, min(start + batch_size, 100000)), type=pa.int64())], names=["id"])
        for start in range(0, 100000, batch_size)
    ]
    spark.createDataFrame(pa.Table.from_batches(batches)).createOrReplaceTempView("local_in_candidates")
    try:
        actual = spark.sql(
            """
            SELECT SUM(CAST(present AS BIGINT)) AS matches,
              SUM(CAST(monotonic_present AS BIGINT)) AS monotonic_matches,
              SUM(CAST(partition_present AS BIGINT)) AS partition_matches
            FROM (
              SELECT id IN (
                SELECT CAST(rand(0) * 100000 AS BIGINT) FROM local_in_candidates
              ) AS present,
              id IN (
                SELECT monotonically_increasing_id() FROM local_in_candidates
              ) AS monotonic_present,
              id IN (
                SELECT spark_partition_id() FROM local_in_candidates
              ) AS partition_present
              FROM range(100000)
            )
            """
        ).collect()
        # Spark initializes local state once at partition zero for all input rows.
        assert actual == [Row(matches=63228, monotonic_matches=100000, partition_matches=1)]
    finally:
        spark.catalog.dropTempView("local_in_candidates")


@pytest.mark.skipif(pyspark_version() < (4,), reason="Arrow table input requires PySpark 4.0+")
@pytest.mark.parametrize("bad_index", [0, 49999])
def test_local_projected_in_evaluates_unused_errors_across_large_inputs(spark, bad_index):
    values = ["1"] * 50000
    values[bad_index] = "invalid"
    spark.createDataFrame(pa.table({"x": range(50000), "value": values})).createOrReplaceTempView("local_in_errors")
    ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        with pytest.raises(Exception, match=r"(?i)(cast_invalid_input|cannot cast string)"):
            spark.sql(
                """
                SELECT id IN (
                  SELECT x FROM (
                    SELECT x, CAST(value AS INT) AS unused FROM local_in_errors
                  )
                ) AS present FROM range(1)
                """
            ).collect()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        spark.catalog.dropTempView("local_in_errors")


@pytest.mark.skipif(pyspark_version() < (4,), reason="Arrow table input requires PySpark 4.0+")
@pytest.mark.parametrize("operator", ["AND", "OR"])
def test_local_projected_in_boolean_masks_preserve_nulls_and_short_circuiting(spark, operator):
    count = 10003
    gates = [None if i % 5 == 0 else i % 3 == 0 for i in range(count)]
    values = [
        "invalid" if (gate is False if operator == "AND" else gate is True) else str(i % 2)
        for i, gate in enumerate(gates)
    ]
    table = pa.table({"id": range(count), "gate": pa.array(gates, type=pa.bool_()), "value": values})
    # SQL BDD cannot supply explicit Arrow batch boundaries.
    table = pa.Table.from_batches(table.to_batches(max_chunksize=97))
    spark.createDataFrame(table).createOrReplaceTempView("local_in_boolean_masks")
    ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        actual = spark.sql(
            f"""
            SELECT SUM(CAST(present AS BIGINT)) AS matches FROM (
              SELECT id IN (
                SELECT id FROM local_in_boolean_masks
                WHERE gate {operator} CAST(value AS INT) > 0
              ) AS present FROM range({count})
            )
            """  # noqa: S608 -- operator and count are fixed test inputs.
        ).collect()
        expected = sum(
            (gate is True and i % 2 == 1) if operator == "AND" else (gate is True or i % 2 == 1)
            for i, gate in enumerate(gates)
        )
        assert actual == [Row(matches=expected)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        spark.catalog.dropTempView("local_in_boolean_masks")


@pytest.mark.parametrize("condition", ["NOT present", "id > 0 AND NOT present"])
@pytest.mark.skipif(is_jvm_spark(), reason="checks Sail physical join operators")
def test_projected_in_alias_filter_keeps_null_aware_anti_join(spark, condition):
    query = f"""
        SELECT COUNT(*) AS matches FROM (
          SELECT id, id IN (SELECT NULLIF(id, -1L) FROM range(1000)) AS present
          FROM range(2000)
        ) WHERE {condition}
        """  # noqa: S608 -- condition is a fixed test parameter.
    result = spark.sql(query)
    # Check the complexity-sensitive operator without snapshotting incidental
    # column IDs or the rest of the plan. Do this before executing the join.
    plan = result._explain_string()  # noqa: SLF001
    assert "HashJoinExec" in plan
    assert "null_aware" in plan
    assert "NestedLoopJoinExec" not in plan
    assert result.collect() == [Row(matches=1000)]


@pytest.mark.skipif(pyspark_version() < (4,), reason="Arrow table input requires PySpark 4.0+")
@pytest.mark.parametrize(
    ("shape", "comparison"),
    [
        pytest.param(
            shape,
            comparison,
            marks=(
                pytest.mark.xfail(
                    not is_jvm_spark(),
                    reason="Sail lacks Catalyst row/container provenance for imported local-plan identity",
                    strict=True,
                )
                if comparison == "identical" or (shape == "scalar" and comparison == "aliased")
                else ()
            ),
        )
        for shape in ["scalar", "array", "struct"]
        for comparison in ["identical", "opposite_bits", "mixed_origins", "mixed_rows", "aliased"]
    ],
)
def test_projected_in_imported_local_identity(spark, shape, comparison):
    # Arrow imports produce UnsafeRow/UnsafeArrayData in Spark. SQL VALUES and
    # interpreted projections produce generic rows, retaining nested imported
    # containers. BDD cannot supply these different local row representations.
    data_type, sql_type, expression = {
        "scalar": (pa.float64(), "DOUBLE", "{}"),
        "array": (pa.list_(pa.float64()), "ARRAY<DOUBLE>", "array({})"),
        "struct": (pa.struct([("x", pa.float64())]), "STRUCT<x:DOUBLE>", "named_struct('x', {})"),
    }[shape]
    views = ["identity_positive", "identity_negative", "identity_operand"]
    try:
        for name, number in zip(views[:2], [0.0, -0.0], strict=True):
            value = {"scalar": number, "array": [number], "struct": {"x": number}}[shape]
            table = pa.table({"v": pa.array([value, None], type=data_type)})
            # An explicit schema avoids Connect's automatic toDF projection,
            # which would otherwise convert the imported outer row to generic.
            spark.createDataFrame(table, schema=f"v {sql_type}").createOrReplaceTempView(name)
        spark.createDataFrame(pa.table({"v": pa.nulls(1, data_type)}), schema=f"v {sql_type}").createOrReplaceTempView(
            views[2]
        )
        positive = "SELECT v FROM identity_positive"
        sql_value = expression.format("CAST(0 AS DOUBLE)")
        left, right = {
            "identical": (positive, positive),
            "opposite_bits": (positive, "SELECT v FROM identity_negative"),
            "mixed_origins": (
                positive,
                f"SELECT v FROM VALUES ({sql_value}), (CAST(NULL AS {sql_type})) t(v)",  # noqa: S608
            ),
            "mixed_rows": (positive, "SELECT v AS w FROM identity_positive"),
            "aliased": ("SELECT v AS w FROM identity_positive", "SELECT v AS w FROM identity_negative"),
        }[comparison]
        actual = spark.sql(
            f"SELECT (v IN ({left})) OR NOT (v IN ({right})) AS p FROM identity_operand"  # noqa: S608
        ).collect()
        # Only generic scalar rows equate opposite signed zeros. Generic rows
        # containing imported arrays/structs retain bitwise container equality.
        expected = comparison == "identical" or (shape == "scalar" and comparison == "aliased")
        assert actual == [Row(p=expected)]
    finally:
        for view in views:
            spark.catalog.dropTempView(view)
