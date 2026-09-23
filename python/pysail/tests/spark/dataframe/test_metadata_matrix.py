import pytest
from pyspark.sql import Window
from pyspark.sql import functions as sf
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# The metadata Spark reports for each output field, and for each field inside a struct, after one
# operation over a frame whose every column carries metadata of its own. The rule underneath is
# `Alias.metadata`: a name inherits the metadata of an attribute, of another alias and of a struct
# field, and reports none for any other expression. What varies is how each operation builds its
# output, and that is where a divergence hides: `withColumn` gives the column it adds or replaces
# explicit empty metadata even when it copies another one, and a cast or an aggregate is an
# expression rather than a name. Every expected value was measured on the Spark JVM. The fields
# are listed in order, since a join gives two of the same name.


def _frame(spark):
    schema = StructType(
        [
            StructField("a", IntegerType(), metadata={"m": "a"}),
            StructField("b", IntegerType(), metadata={"m": "b"}),
            StructField("k", StringType(), metadata={"m": "k"}),
            StructField("s", StructType([StructField("x", IntegerType(), metadata={"m": "x"})]), metadata={"m": "s"}),
        ]
    )
    return spark.createDataFrame([(1, 2, "k", (3,))], schema)


def _other(spark):
    schema = StructType(
        [StructField("a", IntegerType(), metadata={"m": "ra"}), StructField("c", IntegerType(), metadata={"m": "c"})]
    )
    return spark.createDataFrame([(1, 5)], schema)


def _view(df, sql):
    df.createOrReplaceTempView("metadata_matrix")
    return df.sparkSession.sql(sql)


_OPERATIONS = {
    "select col": lambda d: d.select("a"),
    "select alias": lambda d: d.select(sf.col("a").alias("z")),
    "select expr": lambda d: d.select((sf.col("a") + 1).alias("z")),
    "select cast": lambda d: d.select(sf.col("a").cast("long")),
    "select s.x": lambda d: d.select(sf.col("s.x")),
    "select s[x]": lambda d: d.select(sf.col("s")["x"]),
    "select s.x alias": lambda d: d.select(sf.col("s.x").alias("z")),
    "selectExpr": lambda d: d.selectExpr("a", "a + 1 AS z", "s.x"),
    "sql view": lambda d: _view(d, "SELECT a, a AS z, a + 1 AS e, s.x FROM metadata_matrix"),
    "withColumn copy": lambda d: d.withColumn("z", sf.col("a")),
    "withColumn same": lambda d: d.withColumn("a", sf.col("a")),
    "withColumn expr": lambda d: d.withColumn("a", sf.col("a") + 1),
    "withColumn lit": lambda d: d.withColumn("z", sf.lit(1)),
    "withColumns": lambda d: d.withColumns({"z": sf.col("a"), "b": sf.col("b")}),
    "withColumnRenamed": lambda d: d.withColumnRenamed("a", "z"),
    "withColumnsRenamed": lambda d: d.withColumnsRenamed({"a": "z", "b": "y"}),
    "withMetadata": lambda d: d.withMetadata("a", {"n": "1"}),
    "drop": lambda d: d.drop("b"),
    "filter": lambda d: d.filter("a > 0"),
    "sort": lambda d: d.sort("a"),
    "limit": lambda d: d.limit(1),
    "distinct": lambda d: d.select("a", "b").distinct(),
    "dropDuplicates": lambda d: d.dropDuplicates(["a"]),
    "sample": lambda d: d.sample(fraction=1.0, seed=1),
    "repartition": lambda d: d.repartition(2),
    "alias": lambda d: d.alias("t").select("t.a"),
    "groupBy key": lambda d: d.groupBy("a").agg(sf.count("b").alias("n")),
    "groupBy first": lambda d: d.groupBy("k").agg(sf.first("a").alias("f")),
    "agg max": lambda d: d.agg(sf.max("a").alias("mx")),
    "window row_number": lambda d: d.withColumn("r", sf.row_number().over(Window.orderBy("a"))),
    "window max": lambda d: d.withColumn("mx", sf.max("a").over(Window.partitionBy("k"))),
    "join on": lambda d: d.alias("l").join(_other(d.sparkSession).alias("r"), sf.col("l.a") == sf.col("r.a")),
    "join using": lambda d: d.join(_other(d.sparkSession), "a"),
    "crossJoin": lambda d: d.select("b").crossJoin(_other(d.sparkSession)),
    "withField": lambda d: d.withColumn("s", sf.col("s").withField("y", sf.lit(1))),
    "dropFields": lambda d: d.withColumn("s", sf.col("s").withField("y", sf.lit(1)).dropFields("y")),
    "struct of cols": lambda d: d.select(sf.struct("a", "b").alias("st")),
    "explode": lambda d: d.select(sf.explode(sf.array("a")).alias("e")),
    "coalesce": lambda d: d.select(sf.coalesce("a").alias("c")),
    "unpivot": lambda d: d.select("k", "a", "b").unpivot("k", ["a", "b"], "var", "val"),
    "pivot": lambda d: d.groupBy("k").pivot("b").agg(sf.first("a")),
    "cube": lambda d: d.cube("a").agg(sf.count("b").alias("n")),
}

_EXPECTED = {
    "select col": [("a", {"m": "a"})],
    "select alias": [("z", {"m": "a"})],
    "select expr": [("z", {})],
    "select cast": [("a", {})],
    "select s.x": [("x", {"m": "x"})],
    "select s[x]": [("s.x", {"m": "x"})],
    "select s.x alias": [("z", {"m": "x"})],
    "selectExpr": [("a", {"m": "a"}), ("z", {}), ("x", {"m": "x"})],
    "sql view": [("a", {"m": "a"}), ("z", {"m": "a"}), ("e", {}), ("x", {"m": "x"})],
    "withColumn copy": [
        ("a", {"m": "a"}),
        ("b", {"m": "b"}),
        ("k", {"m": "k"}),
        ("s", {"m": "s"}),
        ("s.x", {"m": "x"}),
        ("z", {}),
    ],
    "withColumn same": [("a", {}), ("b", {"m": "b"}), ("k", {"m": "k"}), ("s", {"m": "s"}), ("s.x", {"m": "x"})],
    "withColumn expr": [("a", {}), ("b", {"m": "b"}), ("k", {"m": "k"}), ("s", {"m": "s"}), ("s.x", {"m": "x"})],
    "withColumn lit": [
        ("a", {"m": "a"}),
        ("b", {"m": "b"}),
        ("k", {"m": "k"}),
        ("s", {"m": "s"}),
        ("s.x", {"m": "x"}),
        ("z", {}),
    ],
    "withColumns": [("a", {"m": "a"}), ("b", {}), ("k", {"m": "k"}), ("s", {"m": "s"}), ("s.x", {"m": "x"}), ("z", {})],
    "withColumnRenamed": [
        ("z", {"m": "a"}),
        ("b", {"m": "b"}),
        ("k", {"m": "k"}),
        ("s", {"m": "s"}),
        ("s.x", {"m": "x"}),
    ],
    "withColumnsRenamed": [
        ("z", {"m": "a"}),
        ("y", {"m": "b"}),
        ("k", {"m": "k"}),
        ("s", {"m": "s"}),
        ("s.x", {"m": "x"}),
    ],
    "withMetadata": [("a", {"n": "1"}), ("b", {"m": "b"}), ("k", {"m": "k"}), ("s", {"m": "s"}), ("s.x", {"m": "x"})],
    "drop": [("a", {"m": "a"}), ("k", {"m": "k"}), ("s", {"m": "s"}), ("s.x", {"m": "x"})],
    "filter": [("a", {"m": "a"}), ("b", {"m": "b"}), ("k", {"m": "k"}), ("s", {"m": "s"}), ("s.x", {"m": "x"})],
    "sort": [("a", {"m": "a"}), ("b", {"m": "b"}), ("k", {"m": "k"}), ("s", {"m": "s"}), ("s.x", {"m": "x"})],
    "limit": [("a", {"m": "a"}), ("b", {"m": "b"}), ("k", {"m": "k"}), ("s", {"m": "s"}), ("s.x", {"m": "x"})],
    "distinct": [("a", {"m": "a"}), ("b", {"m": "b"})],
    "dropDuplicates": [("a", {"m": "a"}), ("b", {"m": "b"}), ("k", {"m": "k"}), ("s", {"m": "s"}), ("s.x", {"m": "x"})],
    "sample": [("a", {"m": "a"}), ("b", {"m": "b"}), ("k", {"m": "k"}), ("s", {"m": "s"}), ("s.x", {"m": "x"})],
    "repartition": [("a", {"m": "a"}), ("b", {"m": "b"}), ("k", {"m": "k"}), ("s", {"m": "s"}), ("s.x", {"m": "x"})],
    "alias": [("a", {"m": "a"})],
    "groupBy key": [("a", {"m": "a"}), ("n", {})],
    "groupBy first": [("k", {"m": "k"}), ("f", {})],
    "agg max": [("mx", {})],
    "window row_number": [
        ("a", {"m": "a"}),
        ("b", {"m": "b"}),
        ("k", {"m": "k"}),
        ("s", {"m": "s"}),
        ("s.x", {"m": "x"}),
        ("r", {}),
    ],
    "window max": [
        ("a", {"m": "a"}),
        ("b", {"m": "b"}),
        ("k", {"m": "k"}),
        ("s", {"m": "s"}),
        ("s.x", {"m": "x"}),
        ("mx", {}),
    ],
    "join on": [
        ("a", {"m": "a"}),
        ("b", {"m": "b"}),
        ("k", {"m": "k"}),
        ("s", {"m": "s"}),
        ("s.x", {"m": "x"}),
        ("a", {"m": "ra"}),
        ("c", {"m": "c"}),
    ],
    "join using": [
        ("a", {"m": "a"}),
        ("b", {"m": "b"}),
        ("k", {"m": "k"}),
        ("s", {"m": "s"}),
        ("s.x", {"m": "x"}),
        ("c", {"m": "c"}),
    ],
    "crossJoin": [("b", {"m": "b"}), ("a", {"m": "ra"}), ("c", {"m": "c"})],
    "withField": [("a", {"m": "a"}), ("b", {"m": "b"}), ("k", {"m": "k"}), ("s", {}), ("s.x", {"m": "x"}), ("s.y", {})],
    "dropFields": [("a", {"m": "a"}), ("b", {"m": "b"}), ("k", {"m": "k"}), ("s", {}), ("s.x", {"m": "x"})],
    "struct of cols": [("st", {}), ("st.a", {"m": "a"}), ("st.b", {"m": "b"})],
    "explode": [("e", {})],
    "coalesce": [("c", {})],
    "unpivot": [("k", {"m": "k"}), ("var", {}), ("val", {})],
    "pivot": [("k", {"m": "k"}), ("2", {})],
    "cube": [("a", {"m": "a"}), ("n", {})],
}


def _metadata(df):
    df._cached_schema = None  # noqa: SLF001
    fields = []
    for field in df.schema.fields:
        fields.append((field.name, dict(field.metadata)))
        if isinstance(field.dataType, StructType):
            fields.extend((f"{field.name}.{inner.name}", dict(inner.metadata)) for inner in field.dataType.fields)
    return fields


@pytest.mark.parametrize("operation", list(_OPERATIONS))
def test_the_metadata_an_operation_reports(spark, operation):
    try:
        assert _metadata(_OPERATIONS[operation](_frame(spark))) == _EXPECTED[operation]
    finally:
        spark.sql("DROP VIEW IF EXISTS metadata_matrix")
