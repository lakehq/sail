# ruff: noqa: S608

from pathlib import Path

import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.testing.spark.utils.sql import escape_sql_string_literal

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Lake-format merge tests for JVM Spark")

MERGE_APIS = [
    "sql",
    pytest.param(
        "dataframe",
        marks=pytest.mark.skipif(
            not hasattr(SparkDataFrame, "mergeInto"), reason="DataFrame.mergeInto requires Spark 4.0+"
        ),
    ),
]
MERGE_SCHEMA = StructType([StructField("id", IntegerType()), StructField("dep", StringType())])
SOURCE_VIEW = "merge_default_resolution_source"


def _table_files(path: Path) -> dict[Path, bytes]:
    return {file.relative_to(path): file.read_bytes() for file in path.rglob("*") if file.is_file()}


def _assert_rows_and_schema(frame, expected):
    assert frame.schema == MERGE_SCHEMA
    assert [tuple(row) for row in frame.orderBy("id").collect()] == expected


# Both lake formats use the shared assignment resolver; actual columns take precedence.
@pytest.fixture(params=[("iceberg", 2), ("delta", None)], ids=["iceberg-v2", "delta"])
def merge_resolution_table(spark, tmp_path, request):
    file_format, version = request.param
    name = "merge_api_default_resolution"
    path = tmp_path / name
    properties = f"TBLPROPERTIES ('format-version' = '{version}')" if version is not None else ""
    spark.sql(f"""CREATE TABLE {name} (id INT, dep STRING) USING {file_format}
        LOCATION '{escape_sql_string_literal(str(path))}' {properties}""")
    try:
        yield name, path, file_format
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {SOURCE_VIEW}")
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize(
    ("api", "reference"),
    [
        ("sql", "default"),
        ("sql", "`default`"),
        ("sql", "s.default"),
        ("sql", "concat(default, '!')"),
        *[
            pytest.param(
                "dataframe",
                reference,
                marks=pytest.mark.skipif(
                    not hasattr(SparkDataFrame, "mergeInto"), reason="DataFrame.mergeInto requires Spark 4.0+"
                ),
            )
            for reference in ["column", "quoted", "qualified", "plan-bound", "expression", "expression-lower", "concat"]
        ],
    ],
)
def test_merge_resolves_source_column_named_default(spark, merge_resolution_table, api, reference):
    name, path, file_format = merge_resolution_table
    spark.sql(f"INSERT INTO {name} VALUES (1, 'old')")
    source = spark.createDataFrame([(1, "new"), (2, "added")], "src_id INT, default STRING").alias("s")
    if api == "sql":
        source.createOrReplaceTempView(SOURCE_VIEW)
        spark.sql(f"""MERGE INTO {name} AS t USING {SOURCE_VIEW} AS s ON t.id = s.src_id
            WHEN MATCHED THEN UPDATE SET dep = {reference}
            WHEN NOT MATCHED THEN INSERT (id, dep) VALUES (s.src_id, {reference})""").collect()
    else:
        value = {
            "column": F.col("default"),
            "quoted": F.col("`default`"),
            "qualified": F.col("s.default"),
            "plan-bound": source["default"],
            "expression": F.expr("DEFAULT"),
            "expression-lower": F.expr("default"),
            "concat": F.concat(F.col("default"), F.lit("!")),
        }[reference]
        (
            source.mergeInto(name, F.col("id") == F.col("src_id"))
            .whenMatched()
            .update({"dep": value})
            .whenNotMatched()
            .insert({"id": F.col("src_id"), "dep": value})
            .merge()
        )
    suffix = "!" if reference.startswith("concat") else ""
    expected = [(1, f"new{suffix}"), (2, f"added{suffix}")]
    _assert_rows_and_schema(spark.table(name), expected)
    _assert_rows_and_schema(spark.read.format(file_format).load(str(path)), expected)


@pytest.mark.parametrize("api", MERGE_APIS)
def test_merge_default_column_resolution_uses_clause_scope(spark, merge_resolution_table, api):
    name, path, file_format = merge_resolution_table
    spark.sql(f"ALTER TABLE {name} ADD COLUMN `default` STRING")
    spark.sql(f"INSERT INTO {name} VALUES (1, 'old', 'target-one'), (3, 'old', 'target-three')")
    source = spark.createDataFrame([(1,), (2,)], "src_id INT").alias("s")
    if api == "sql":
        source.createOrReplaceTempView(SOURCE_VIEW)
        spark.sql(f"""MERGE INTO {name} AS t USING {SOURCE_VIEW} AS s ON t.id = s.src_id
            WHEN MATCHED THEN UPDATE SET dep = DEFAULT
            WHEN NOT MATCHED THEN INSERT (id, dep, `default`) VALUES (s.src_id, DEFAULT, 'inserted')
            WHEN NOT MATCHED BY SOURCE THEN UPDATE SET dep = concat(default, '!')""").collect()
    else:
        (
            source.mergeInto(name, F.col("id") == F.col("src_id"))
            .whenMatched()
            .update({"dep": F.expr("DEFAULT")})
            .whenNotMatched()
            .insert({"id": F.col("src_id"), "dep": F.expr("DEFAULT"), "default": F.lit("inserted")})
            .whenNotMatchedBySource()
            .update({"dep": F.concat(F.col("default"), F.lit("!"))})
            .merge()
        )
    expected = [(1, "target-one", "target-one"), (2, None, "inserted"), (3, "target-three!", "target-three")]
    for frame in [spark.table(name), spark.read.format(file_format).load(str(path))]:
        assert [tuple(row) for row in frame.orderBy("id").collect()] == expected


@pytest.mark.skipif(not hasattr(SparkDataFrame, "mergeInto"), reason="DataFrame.mergeInto requires Spark 4.0+")
@pytest.mark.parametrize("reference", ["qualified", "plan-bound"])
def test_merge_target_only_does_not_resolve_source_default_column(spark, merge_resolution_table, reference):
    name, path, _ = merge_resolution_table
    spark.sql(f"INSERT INTO {name} VALUES (1, 'original')")
    source = spark.createDataFrame([(2, "source")], "src_id INT, default STRING").alias("s")
    value = source["default"] if reference == "plan-bound" else F.col("s.default")
    before = _table_files(path)
    merge = source.mergeInto(name, F.col("id") == F.col("src_id")).whenNotMatchedBySource().update({"dep": value})
    with pytest.raises(Exception, match=r"(?i)(cannot resolve|unresolved|missing)"):
        merge.merge()
    _assert_rows_and_schema(spark.table(name), [(1, "original")])
    assert _table_files(path) == before


def _merge_update_expression(spark, name, source, api, expression):
    if api == "sql":
        source.createOrReplaceTempView(SOURCE_VIEW)
        spark.sql(f"""MERGE INTO {name} AS t USING {SOURCE_VIEW} AS s ON t.id = s.src_id
            WHEN MATCHED THEN UPDATE SET dep = {expression}""").collect()
    else:
        source.mergeInto(name, F.col("id") == F.col("src_id")).whenMatched().update({"dep": F.expr(expression)}).merge()


@pytest.mark.parametrize("api", MERGE_APIS)
def test_merge_ambiguous_default_column_is_not_a_default_value(spark, merge_resolution_table, api):
    name, path, _ = merge_resolution_table
    spark.sql(f"INSERT INTO {name} VALUES (1, 'original')")
    source = spark.createDataFrame([(1, "first", "second")], "src_id INT, default STRING, default STRING")
    before = _table_files(path)
    with pytest.raises(Exception, match=r"(?i)(ambiguous|duplicate qualified field)"):
        _merge_update_expression(spark, name, source, api, "DEFAULT")
    _assert_rows_and_schema(spark.table(name), [(1, "original")])
    assert _table_files(path) == before


@pytest.mark.parametrize("api", MERGE_APIS)
@pytest.mark.parametrize("assignment", ["DEFAULT", "default", "NULL"])
@pytest.mark.parametrize("merge_resolution_table", [("iceberg", 3), ("delta", None)], indirect=True)
def test_merge_unresolved_default_and_explicit_null(spark, merge_resolution_table, api, assignment):
    name, path, file_format = merge_resolution_table
    spark.sql(f"ALTER TABLE {name} ALTER COLUMN dep SET DEFAULT 'fallback'")
    spark.sql(f"INSERT INTO {name} VALUES (1, 'old'), (3, 'old')")
    source = spark.createDataFrame([(1,), (2,)], "src_id INT")
    if api == "sql":
        source.createOrReplaceTempView(SOURCE_VIEW)
        spark.sql(f"""MERGE INTO {name} AS t USING {SOURCE_VIEW} AS s ON t.id = s.src_id
            WHEN MATCHED THEN UPDATE SET dep = {assignment}
            WHEN NOT MATCHED THEN INSERT (id, dep) VALUES (s.src_id, {assignment})
            WHEN NOT MATCHED BY SOURCE THEN UPDATE SET dep = {assignment}""").collect()
    else:
        value = F.expr(assignment)
        (
            source.mergeInto(name, F.col("id") == F.col("src_id"))
            .whenMatched()
            .update({"dep": value})
            .whenNotMatched()
            .insert({"id": F.col("src_id"), "dep": value})
            .whenNotMatchedBySource()
            .update({"dep": value})
            .merge()
        )
    expected = [(key, None if assignment == "NULL" else "fallback") for key in (1, 2, 3)]
    _assert_rows_and_schema(spark.table(name), expected)
    _assert_rows_and_schema(spark.read.format(file_format).load(str(path)), expected)


@pytest.mark.parametrize("api", MERGE_APIS)
def test_merge_rejects_unresolved_default_in_complex_expression(spark, merge_resolution_table, api):
    name, path, _ = merge_resolution_table
    spark.sql(f"INSERT INTO {name} VALUES (1, 'original')")
    source = spark.createDataFrame([(1,)], "src_id INT")
    before = _table_files(path)
    with pytest.raises(Exception, match=r"DEFAULT.*(standalone|complex expression)"):
        _merge_update_expression(spark, name, source, api, "CAST(DEFAULT AS STRING)")
    _assert_rows_and_schema(spark.table(name), [(1, "original")])
    assert _table_files(path) == before
