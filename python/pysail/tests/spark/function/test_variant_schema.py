import pytest
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.common import pyspark_version

pytestmark = pytest.mark.skipif(pyspark_version() < (4,), reason="VARIANT requires Spark 4+")


@pytest.mark.parametrize("parser", ["parse_json", "try_parse_json"])
def test_numeric_variant_schema_from_columns(spark, parser):
    data = spark.range(0, 8, 1, 4).selectExpr(
        "id",
        "concat('[', CAST(id AS STRING), '.01,', CAST(id AS STRING), '.00]') AS j",
        "array(CAST(id + 0.01 AS DECIMAL(3,2)), CAST(id AS DECIMAL(3,2))) AS d",
    )
    result = data.selectExpr(
        "id", f"schema_of_variant({parser}(j)) AS parsed", "schema_of_variant(CAST(d AS VARIANT)) AS casted"
    ).orderBy("id")
    assert [(r.id, r.parsed, r.casted) for r in result.collect()] == [
        (i, "ARRAY<DECIMAL(3,2)>", "ARRAY<DECIMAL(3,2)>") for i in range(8)
    ]


@pytest.mark.parametrize("parser", ["parse_json", "try_parse_json"])
def test_numeric_variant_schema_aggregate_across_partitions(spark, parser):
    # Each source partition contains only one numeric schema (or NULL). The
    # grouped final aggregate must widen schemas from different partial states.
    data = spark.range(0, 16, 1, 4).selectExpr(
        "id % 2 AS g",
        "CASE WHEN id < 4 THEN '1.01' WHEN id < 8 THEN '1.00' "
        "WHEN id < 12 THEN 'null' END AS j",
        "CAST(CASE WHEN id < 4 THEN 1.01 WHEN id < 8 THEN 1.00 END AS DECIMAL(3,2)) AS d",
        "CASE WHEN id < 8 THEN '1.1' ELSE '1e0' END AS mixed",
    )
    result = data.groupBy("g").agg(
        F.expr(f"schema_of_variant_agg({parser}(j))").alias("parsed"),
        F.expr("schema_of_variant_agg(CAST(d AS VARIANT))").alias("casted"),
        F.expr(f"schema_of_variant_agg({parser}(mixed))").alias("mixed"),
        F.expr(f"schema_of_variant_agg({parser}(concat('{{\"a\":[', j, ']}}')))").alias("nested"),
    ).orderBy("g")
    assert [(r.g, r.parsed, r.casted, r.mixed, r.nested) for r in result.collect()] == [
        (g, "DECIMAL(3,2)", "DECIMAL(3,2)", "DOUBLE", "OBJECT<a: ARRAY<DECIMAL(3,2)>>") for g in range(2)
    ]
