"""The field metadata the server puts on the Arrow stream.

`toArrow()` cannot see this: it rebuilds the Arrow schema client-side from `df.schema`
and casts the batches to it (`pyspark/sql/connect/dataframe.py`, `toArrow`), so every
metadata key the server sent is replaced by whatever the Connect proto schema implies.
The only way to assert what Sail actually writes is the table behind that cast, which is
why these tests reach for `_to_table()`.
"""

import json

import pyarrow as pa
import pytest
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import ArrayType, IntegerType, StructField, StructType

from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.tests.spark.dataframe.udt import UnnamedPythonUDT

SPARK_METADATA_KEY = b"SPARK::metadata::json"


def walk(field, path=""):
    """Yield `(path, metadata)` for a field and every field nested under it."""
    path = f"{path}.{field.name}" if path else field.name
    yield path, dict(field.metadata or {})
    data_type = field.type
    if pa.types.is_struct(data_type):
        for index in range(data_type.num_fields):
            yield from walk(data_type.field(index), path)
    elif pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        yield from walk(data_type.value_field, path)
    elif pa.types.is_map(data_type):
        yield from walk(data_type.key_field, path)
        yield from walk(data_type.item_field, path)


def wire_metadata(df):
    """The metadata of every field of the Arrow stream the server sent, by path."""
    if not hasattr(df, "_to_table"):
        pytest.skip("this PySpark client cannot expose the server's Arrow schema")
    table, _ = df._to_table()  # noqa: SLF001
    return {path: metadata for field in table.schema for path, metadata in walk(field)}


def spark_metadata(metadata):
    """The `SPARK::metadata::json` blob of one field, parsed -- the spacing is not stable."""
    raw = metadata.get(SPARK_METADATA_KEY)
    return None if raw is None else json.loads(raw)


def field_ending_in(metadata, suffix):
    """The one field whose path ends in `suffix`.

    Addressed by suffix because the engines disagree on what to call the child of a list,
    which is a separate divergence and must not be what makes these tests fail.
    """
    matches = [value for path, value in metadata.items() if path.endswith(suffix)]
    assert len(matches) == 1, f"expected one field ending in {suffix}, got {sorted(metadata)}"
    return matches[0]


def test_a_plain_column_carries_no_metadata(spark):
    assert wire_metadata(spark.sql("SELECT 1 AS c")) == {"c": {}}


def test_metadata_from_a_dataframe_schema_reaches_the_wire(spark):
    # The route that already works in both engines, and the reference for the rest:
    # the metadata travels as one JSON blob on the field that declares it.
    inner = StructType([StructField("a", IntegerType(), metadata={"comment": "note"})])
    schema = StructType([StructField("c", IntegerType(), metadata={"k": "v"}), StructField("s", inner)])
    metadata = wire_metadata(spark.createDataFrame([(1, (1,))], schema))
    assert spark_metadata(metadata["c"]) == {"k": "v"}
    assert spark_metadata(metadata["s.a"]) == {"comment": "note"}


def test_a_field_without_metadata_carries_no_blob(spark):
    # Both engines write the key only when there is something to say. Sail used to send `{}`
    # on the parent of any field that did carry metadata.
    inner = StructType([StructField("a", IntegerType(), metadata={"comment": "note"})])
    metadata = wire_metadata(spark.createDataFrame([((1,),)], StructType([StructField("s", inner)])))
    assert metadata["s"] == {}


@pytest.mark.parametrize(
    "expression",
    [
        pytest.param("CAST(named_struct('a',1) AS STRUCT<a: INT COMMENT 'note'>)", id="struct"),
        pytest.param(
            "CAST(named_struct('x', named_struct('a',1)) AS STRUCT<x: STRUCT<a: INT COMMENT 'note'>>)",
            id="nested-struct",
        ),
        pytest.param("CAST(array(named_struct('a',1)) AS ARRAY<STRUCT<a: INT COMMENT 'note'>>)", id="array-element"),
        pytest.param(
            "CAST(map('k', named_struct('a',1)) AS MAP<STRING, STRUCT<a: INT COMMENT 'note'>>)",
            id="map-value",
        ),
    ],
)
def test_a_comment_in_a_sql_type_reaches_the_wire(spark, expression):
    # Inside the plan the comment is a loose `comment` key, which Delta and the catalogs read
    # directly; it is folded into the blob on the way out, at whatever depth it sits.
    metadata = wire_metadata(spark.sql(f"SELECT {expression} AS c"))
    assert spark_metadata(field_ending_in(metadata, ".a")) == {"comment": "note"}


def test_a_table_column_comment_reaches_the_wire(spark):
    spark.sql("DROP TABLE IF EXISTS wire_metadata_comment")
    spark.sql("CREATE TABLE wire_metadata_comment (c INT COMMENT 'note') USING parquet")
    try:
        metadata = wire_metadata(spark.sql("SELECT c FROM wire_metadata_comment"))
        assert spark_metadata(metadata["c"]) == {"comment": "note"}
    finally:
        spark.sql("DROP TABLE IF EXISTS wire_metadata_comment")


@pytest.mark.xfail(
    not is_jvm_spark(),
    strict=True,
    reason="Sail names the child of a list `item`, Spark names it `element`",
)
def test_the_child_of_a_list_is_named_element(spark):
    # Arrow's own default is `item`, which is what Sail sends; Spark renames it. Invisible
    # through `toArrow()`, because the client's cast rebuilds the schema with Spark's name.
    table, _ = spark.sql("SELECT array(1,2) AS c")._to_table()  # noqa: SLF001
    assert table.schema.field(0).type.value_field.name == "element"


def test_the_internal_udt_marker_does_not_reach_the_wire(spark):
    # Sail marks a UDT with a `SAIL::` key on the Arrow field so the Connect encoder can rebuild
    # the UDT type for `df.schema`. Spark sends the storage type with no metadata at all, and the
    # marker must not ride along: `toArrow()` would hide it, but a consumer reading the Arrow
    # stream itself would see Sail's internals.
    udt = UnnamedPythonUDT()
    schema = StructType().add("a", udt).add("s", StructType().add("u", udt)).add("arr", ArrayType(udt))
    metadata = wire_metadata(spark.createDataFrame(data=[], schema=schema))
    assert metadata, "the probe walked no fields"
    leaked = {path: sorted(k.decode() for k in value if k.startswith(b"SAIL::")) for path, value in metadata.items()}
    assert {path: keys for path, keys in leaked.items() if keys} == {}


@pytest.mark.parametrize(
    "metadata",
    [pytest.param({"comment": "new"}, id="same-key"), pytest.param({"k": "v"}, id="other-key")],
)
def test_explicit_metadata_replaces_a_table_column_comment(spark, metadata):
    # An alias with explicit metadata REPLACES the column's own, the table comment included
    # (`Alias.explicitMetadata`, `namedExpressions.scala:172-178`); the loose `comment` must not be
    # folded back over it.
    spark.sql("DROP TABLE IF EXISTS wire_metadata_replaced")
    spark.sql("CREATE TABLE wire_metadata_replaced (c INT COMMENT 'old') USING parquet")
    try:
        df = spark.table("wire_metadata_replaced")
        assert df.withMetadata("c", metadata).schema["c"].metadata == metadata
        assert df.select(F.col("c").alias("c", metadata=metadata)).schema["c"].metadata == metadata
    finally:
        spark.sql("DROP TABLE IF EXISTS wire_metadata_replaced")


# TODO: a table column's loose `comment` rides through expressions Spark does not inherit metadata
#   from (CAST, `first`, `any_value`, `withColumn`), as blob metadata already did before; the fix is in
#   how the resolver propagates field metadata, not in the wire sanitizer.
@pytest.mark.xfail(not is_jvm_spark(), strict=True, reason="Sail propagates field metadata through CAST and aggregates")
@pytest.mark.parametrize(
    "query",
    [
        pytest.param("SELECT CAST(c AS BIGINT) AS r FROM wire_metadata_inherit", id="cast"),
        pytest.param("SELECT first(c) AS r FROM wire_metadata_inherit GROUP BY g", id="first"),
        pytest.param("SELECT any_value(c) AS r FROM wire_metadata_inherit GROUP BY g", id="any_value"),
    ],
)
def test_a_table_column_comment_is_not_inherited_by_an_expression(spark, query):
    spark.sql("DROP TABLE IF EXISTS wire_metadata_inherit")
    spark.sql("CREATE TABLE wire_metadata_inherit (c INT COMMENT 'old', g INT) USING parquet")
    try:
        assert spark.sql(query).schema["r"].metadata == {}
    finally:
        spark.sql("DROP TABLE IF EXISTS wire_metadata_inherit")


@pytest.mark.xfail(not is_jvm_spark(), strict=True, reason="Sail's withColumn inherits the column's metadata")
def test_a_table_column_comment_is_not_inherited_by_with_column(spark):
    spark.sql("DROP TABLE IF EXISTS wire_metadata_inherit")
    spark.sql("CREATE TABLE wire_metadata_inherit (c INT COMMENT 'old') USING parquet")
    try:
        assert spark.table("wire_metadata_inherit").withColumn("c", F.col("c")).schema["c"].metadata == {}
    finally:
        spark.sql("DROP TABLE IF EXISTS wire_metadata_inherit")
