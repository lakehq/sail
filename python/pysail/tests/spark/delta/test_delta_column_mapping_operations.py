"""Query and DML operations on Delta tables with column mapping.

Each query and DML operation runs against a column-mapped Delta table and
against a Delta table without column mapping holding the same rows, and the
results must match exactly. The reference is a Delta table rather than an
in-memory view so that both sides go through the same Parquet round trip, and
only column mapping differs between them.
"""

from __future__ import annotations

import datetime
import json
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pysail.testing.spark.utils.sql import escape_sql_string_literal

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


pytestmark = pytest.mark.integration


_INNER = StructType([StructField("c", IntegerType()), StructField("d", StringType())])
_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("grp", StringType()),
        StructField("big", LongType()),
        StructField("dbl", DoubleType()),
        StructField("dec", DecimalType(10, 2)),
        StructField("flag", BooleanType()),
        StructField("name", StringType()),
        StructField("dt", DateType()),
        StructField("ts", TimestampType()),
        StructField("bin", BinaryType()),
        StructField("spaced col", IntegerType()),
        StructField("dotted.col", StringType()),
        StructField(
            "s",
            StructType(
                [
                    StructField("a", IntegerType()),
                    StructField("b", StringType()),
                    StructField("inner", _INNER),
                ]
            ),
        ),
        StructField("arr", ArrayType(IntegerType())),
        StructField(
            "arr_s",
            ArrayType(StructType([StructField("x", IntegerType()), StructField("y", StringType())])),
        ),
        StructField("m", MapType(StringType(), IntegerType())),
        StructField(
            "m_s",
            MapType(StringType(), StructType([StructField("p", IntegerType()), StructField("q", StringType())])),
        ),
        StructField("m_arr", MapType(StringType(), ArrayType(IntegerType()))),
        StructField(
            "nested",
            StructType(
                [
                    StructField("inner_map", MapType(StringType(), StringType())),
                    StructField(
                        "inner_arr",
                        ArrayType(StructType([StructField("z", IntegerType())])),
                    ),
                ]
            ),
        ),
    ]
)


def _row(i: int, grp: str | None, *, a: int, b: str, c: int, **overrides) -> tuple:
    values = {
        "id": i,
        "grp": grp,
        "big": i * 100,
        "dbl": i + 0.5,
        "dec": Decimal(f"{i}.25"),
        "flag": i % 2 == 0,
        "name": f"n{i}",
        "dt": datetime.date(2024, 1, i),
        "ts": datetime.datetime(2024, 1, i, 12, 0, i),  # noqa: DTZ001
        "bin": bytes([i]),
        "spaced col": i * 10,
        "dotted.col": f"d{i}",
        "s": Row(a=a, b=b, inner=Row(c=c, d=f"inner{i}")),
        "arr": [i, i + 1],
        "arr_s": [Row(x=i, y=f"y{i}"), Row(x=i + 10, y=f"y{i + 10}")],
        "m": {"k1": i, "k2": i * 2},
        "m_s": {"k1": Row(p=i, q=f"q{i}")},
        "m_arr": {"k1": [i, i]},
        "nested": Row(inner_map={"a": f"A{i}"}, inner_arr=[Row(z=i)]),
    }
    values.update(overrides)
    return tuple(values[field.name] for field in _SCHEMA.fields)


# Rows 1 and 5 share the same struct value `s` so that grouping and joins on structs see duplicates.
_ROWS_FIRST = [
    _row(1, "g1", a=1, b="x", c=10),
    _row(2, "g1", a=2, b="y", c=20),
    _row(3, "g2", a=3, b="z", c=30, arr=[], arr_s=[], m={}, m_s={}),
    _row(4, None, a=4, b="w", c=40),
]
_ROWS_SECOND = [
    _row(5, "g2", a=1, b="x", c=10),
    _row(6, "g1", a=6, b="v", c=60, name=None, flag=None, dbl=None),
    _row(7, "g2", a=7, b="u", c=70, s=None, arr=None, arr_s=None, m=None, m_s=None, m_arr=None, nested=None),
    _row(8, None, a=8, b="t", c=80, arr=[8, None], m={"k1": None}),
]

_TABLE_VARIANTS = [
    pytest.param(("name", False), id="name-unpartitioned"),
    pytest.param(("name", True), id="name-partitioned"),
    pytest.param(("id", False), id="id-unpartitioned"),
    pytest.param(("id", True), id="id-partitioned"),
]

# Each query reads `{t}`, which is either the Delta table or the reference view.
# Every query has a deterministic result order.
_QUERIES = {
    # projections
    "select_all": "SELECT * FROM {t} ORDER BY id",
    "select_scalars": "SELECT id, grp, big, dbl, dec, flag, name, dt, ts, bin FROM {t} ORDER BY id",
    "select_special_names": "SELECT id, `spaced col`, `dotted.col` FROM {t} ORDER BY id",
    "select_struct": "SELECT id, s FROM {t} ORDER BY id",
    "select_struct_fields": "SELECT id, s.a, s.b, s.inner, s.inner.c, s.inner.d FROM {t} ORDER BY id",
    "select_struct_star": "SELECT id, s.* FROM {t} ORDER BY id",
    "select_arrays": "SELECT id, arr, arr_s FROM {t} ORDER BY id",
    "select_array_elements": "SELECT id, get(arr, 0) AS a0, get(arr_s, 0) AS e0, get(arr_s, 0).x AS x0,"
    " arr_s.y AS ys FROM {t} ORDER BY id",
    "select_element_at": "SELECT id, try_element_at(arr, 1) AS a1, try_element_at(arr_s, -1).y AS last_y"
    " FROM {t} ORDER BY id",
    "select_maps": "SELECT id, m, m_s, m_arr FROM {t} ORDER BY id",
    "select_map_values": "SELECT id, m['k1'] AS k1, m_s['k1'] AS s1, m_s['k1'].q AS q1, m_arr['k1'] AS a1"
    " FROM {t} ORDER BY id",
    "select_map_functions": "SELECT id, map_keys(m) AS ks, map_values(m_s) AS vs, map_entries(m_arr) AS es"
    " FROM {t} ORDER BY id",
    "select_nested": "SELECT id, nested, nested.inner_map['a'] AS a, nested.inner_arr[0].z AS z FROM {t} ORDER BY id",
    "select_expressions": "SELECT id, big + id AS x, upper(name) AS u, s.a * 2 AS a2, concat(s.b, s.inner.d) AS bd"
    " FROM {t} ORDER BY id",
    "select_casts": "SELECT id, CAST(s AS STRING) AS s1, CAST(arr AS STRING) AS a1, CAST(m AS STRING) AS m1,"
    " to_json(s) AS j1, to_json(arr_s) AS j2 FROM {t} ORDER BY id",
    "select_struct_construction": "SELECT id, named_struct('k', s.a, 'v', arr_s) AS n, struct(s.b, s.inner) AS st"
    " FROM {t} ORDER BY id",
    # conditional expressions on nested values
    "case_struct": "SELECT id, CASE WHEN s.a > 2 THEN s END AS s2 FROM {t} ORDER BY id",
    "case_struct_literal": "SELECT id, CASE WHEN s.a > 2 THEN s.inner"
    " ELSE named_struct('c', 0, 'd', 'none') END AS i2 FROM {t} ORDER BY id",
    "coalesce_struct": "SELECT id, coalesce(s.inner, named_struct('c', -1, 'd', 'missing')) AS i2 FROM {t} ORDER BY id",
    "coalesce_whole_struct": "SELECT id, coalesce(s, named_struct('a', 0, 'b', '',"
    " 'inner', named_struct('c', 0, 'd', ''))) AS s2 FROM {t} ORDER BY id",
    "if_struct": "SELECT id, if(flag, s.inner, NULL) AS i2 FROM {t} ORDER BY id",
    "nvl_array_struct": "SELECT id, nvl(arr_s, array(named_struct('x', 0, 'y', ''))) AS a2 FROM {t} ORDER BY id",
    "coalesce_map": "SELECT id, coalesce(m_s, map('none', named_struct('p', 0, 'q', ''))) AS m2 FROM {t} ORDER BY id",
    # filters
    "filter_scalar": "SELECT id FROM {t} WHERE big > 200 AND dbl < 6 ORDER BY id",
    "filter_partition_column": "SELECT id FROM {t} WHERE grp = 'g2' ORDER BY id",
    "filter_partition_null": "SELECT id FROM {t} WHERE grp IS NULL ORDER BY id",
    "filter_in": "SELECT id FROM {t} WHERE name IN ('n1', 'n3', 'n8') ORDER BY id",
    "filter_between_date": "SELECT id FROM {t} WHERE dt BETWEEN DATE '2024-01-02' AND DATE '2024-01-05' ORDER BY id",
    "filter_like": "SELECT id FROM {t} WHERE `dotted.col` LIKE 'd%' AND `spaced col` >= 30 ORDER BY id",
    "filter_struct_field": "SELECT id FROM {t} WHERE s.a = 1 ORDER BY id",
    "filter_deep_struct_field": "SELECT id FROM {t} WHERE s.inner.c > 20 AND s.inner.d <> 'inner7' ORDER BY id",
    "filter_struct_equality": "SELECT id FROM {t} WHERE s = named_struct('a', 1, 'b', 'x',"
    " 'inner', named_struct('c', 10, 'd', 'inner1')) ORDER BY id",
    "filter_inner_struct_equality": "SELECT id FROM {t} WHERE s.inner = named_struct('c', 20, 'd', 'inner2')"
    " ORDER BY id",
    "filter_struct_in": "SELECT id FROM {t} WHERE s.inner IN (named_struct('c', 10, 'd', 'inner1'),"
    " named_struct('c', 30, 'd', 'inner3')) ORDER BY id",
    "filter_struct_is_null": "SELECT id FROM {t} WHERE s IS NULL OR s.inner IS NULL ORDER BY id",
    "filter_array_struct_is_null": "SELECT id FROM {t} WHERE arr_s IS NULL ORDER BY id",
    "filter_map_struct_is_not_null": "SELECT id FROM {t} WHERE m_s IS NOT NULL ORDER BY id",
    "filter_array": "SELECT id FROM {t} WHERE array_contains(arr, 2) OR size(arr_s) = 0 ORDER BY id",
    "filter_array_struct": "SELECT id FROM {t} WHERE exists(arr_s, e -> e.x > 12) ORDER BY id",
    "filter_map": "SELECT id FROM {t} WHERE m['k2'] >= 8 OR m_s['k1'].p = 1 ORDER BY id",
    "filter_nested_map": "SELECT id FROM {t} WHERE nested.inner_map['a'] = 'A3' ORDER BY id",
    "filter_or_across_columns": "SELECT id FROM {t} WHERE grp = 'g1' OR s.b = 'u' OR flag ORDER BY id",
    # aggregation
    "count": "SELECT count(*) AS n, count(s) AS ns, count(s.a) AS na, count(arr) AS narr FROM {t}",
    "aggregate_scalars": "SELECT sum(big) AS sb, avg(dbl) AS ad, sum(dec) AS sd, min(dt) AS mn, max(ts) AS mx FROM {t}",
    "group_by_partition": "SELECT grp, count(*) AS n, sum(s.a) AS sa, max(s.inner.c) AS mc FROM {t}"
    " GROUP BY grp ORDER BY grp",
    "group_by_struct_field": "SELECT s.b, count(*) AS n FROM {t} GROUP BY s.b ORDER BY s.b",
    "group_by_struct": "SELECT s, count(*) AS n FROM {t} GROUP BY s ORDER BY s.a, s.inner.d",
    "group_by_having": "SELECT grp, count(*) AS n FROM {t} GROUP BY grp HAVING count(*) > 2 ORDER BY grp",
    "distinct_scalar": "SELECT DISTINCT grp FROM {t} ORDER BY grp",
    "distinct_struct": "SELECT DISTINCT s.a, s.b FROM {t} ORDER BY a, b",
    "distinct_whole_struct": "SELECT DISTINCT s FROM {t} ORDER BY s.a, s.inner.d",
    "count_distinct_struct": "SELECT count(DISTINCT s) AS n, count(DISTINCT s.b) AS nb FROM {t}",
    "min_max_struct": "SELECT min(s) AS mn, max(s) AS mx, min(s.inner) AS mi FROM {t}",
    "collect_list": "SELECT grp, collect_list(s.a) AS l, sort_array(collect_set(s.b)) AS st FROM"
    " (SELECT * FROM {t} ORDER BY id) GROUP BY grp ORDER BY grp",
    "first_struct": "SELECT first(s) AS f, first(arr_s) AS fa FROM (SELECT * FROM {t} ORDER BY id)",
    # sorting and limits
    "order_by_struct_field": "SELECT id FROM {t} ORDER BY s.inner.c DESC NULLS LAST, id",
    "order_by_struct": "SELECT id FROM {t} ORDER BY s, id",
    "order_by_limit": "SELECT id, s, m FROM {t} ORDER BY dbl DESC NULLS LAST, id LIMIT 3",
    "limit_offset": "SELECT id FROM {t} ORDER BY id LIMIT 3 OFFSET 2",
    # windows
    "window_row_number": "SELECT id, row_number() OVER (PARTITION BY grp ORDER BY s.a, id) AS rn FROM {t} ORDER BY id",
    "window_struct": "SELECT id, max(s) OVER (PARTITION BY grp) AS mx, lag(s.inner) OVER (ORDER BY id) AS prev"
    " FROM {t} ORDER BY id",
    "window_running_sum": "SELECT id, sum(s.a) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
    " AS total FROM {t} ORDER BY id",
    # generators and higher-order functions
    "explode_array": "SELECT id, v FROM {t} LATERAL VIEW explode(arr) AS v ORDER BY id, v",
    "explode_array_struct": "SELECT id, e.x, e.y FROM {t} LATERAL VIEW explode(arr_s) AS e ORDER BY id, e.x",
    "posexplode": "SELECT id, pos, e FROM {t} LATERAL VIEW posexplode(arr_s) AS pos, e ORDER BY id, pos",
    "inline": "SELECT id, inline(arr_s) FROM {t} ORDER BY id, x",
    "explode_map": "SELECT id, k, v FROM {t} LATERAL VIEW explode(m_s) AS k, v ORDER BY id, k",
    "explode_outer": "SELECT id, e FROM {t} LATERAL VIEW OUTER explode(arr_s) AS e ORDER BY id, e.x",
    "higher_order": "SELECT id, transform(arr_s, e -> e.x + 1) AS xs, filter(arr_s, e -> e.x < 10) AS f,"
    " aggregate(arr, 0, (acc, v) -> acc + coalesce(v, 0)) AS total FROM {t} ORDER BY id",
    "sort_array_struct": "SELECT id, sort_array(arr_s, false) AS sorted FROM {t} ORDER BY id",
    # joins and set operations
    "self_join_scalar": "SELECT a.id, b.name FROM {t} a JOIN {t} b ON a.id = b.id ORDER BY a.id",
    "self_join_struct": "SELECT a.id AS l, b.id AS r FROM {t} a JOIN {t} b ON a.s = b.s ORDER BY l, r",
    "self_join_struct_field": "SELECT a.id AS l, b.id AS r FROM {t} a JOIN {t} b ON a.s.a = b.s.a AND a.id < b.id"
    " ORDER BY l, r",
    "left_join_nested": "SELECT a.id, b.arr_s FROM {t} a LEFT JOIN {t} b ON a.id = b.id + 1 ORDER BY a.id",
    "semi_join": "SELECT id FROM {t} a WHERE EXISTS (SELECT 1 FROM {t} b WHERE b.s.a = a.s.a AND b.id <> a.id)"
    " ORDER BY id",
    "in_subquery": "SELECT id FROM {t} WHERE s.a IN (SELECT s.a FROM {t} WHERE grp = 'g2') ORDER BY id",
    "scalar_subquery": "SELECT id, (SELECT max(s.inner.c) FROM {t}) AS mx FROM {t} ORDER BY id",
    "union_all": "SELECT id, s, arr_s, m_s FROM {t} UNION ALL SELECT id, s, arr_s, m_s FROM {t} ORDER BY id",
    "union": "SELECT id, s, arr_s FROM {t} UNION SELECT id, s, arr_s FROM {t} ORDER BY id",
    "intersect": "SELECT id, s, arr_s FROM {t} INTERSECT SELECT id, s, arr_s FROM {t} WHERE id > 2 ORDER BY id",
    "except": "SELECT id, s FROM {t} EXCEPT SELECT id, s FROM {t} WHERE grp = 'g1' ORDER BY id",
    "cte": "WITH c AS (SELECT id, s.inner AS i, grp FROM {t} WHERE s IS NOT NULL)"
    " SELECT grp, max(i) AS mi FROM c GROUP BY grp ORDER BY grp",
}

# Queries that fail for reasons unrelated to column mapping: each of them also fails
# on the Delta table without column mapping. The value is the reason, and whether
# the failure only happens on partitioned tables.
_KNOWN_QUERY_FAILURES = {
    "nvl_array_struct": (
        "type coercion fails on nested fields carrying Spark field metadata, also without Delta",
        False,
    ),
    "filter_array_struct": (
        "a higher-order function over an array of structs in a filter cannot resolve the column",
        False,
    ),
    "semi_join": ("a correlated reference to a nested field cannot be resolved", False),
    "intersect": ("INTERSECT on array of struct columns fails to compare list values", False),
}


def _load_rows(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    return spark.createDataFrame(rows, schema=_SCHEMA)


def _write_delta(df: DataFrame, path: Path, *, mode: str, column_mapping_mode: str, partitioned: bool) -> None:
    writer = df.write.format("delta").mode(mode)
    if column_mapping_mode != "none":
        writer = writer.option("delta.columnMapping.mode", column_mapping_mode)
    if partitioned:
        writer = writer.partitionBy("grp")
    writer.save(str(path))


def _sorted_rows(df: DataFrame) -> list[Row]:
    return df.orderBy("id").collect()


@pytest.fixture(scope="module", params=_TABLE_VARIANTS)
def column_mapped_views(request, spark: SparkSession, tmp_path_factory) -> dict:
    """Create a column-mapped Delta table and return the names of the views to query.

    The table is written in two commits so that reads span multiple files. It is
    registered both as a view over the path and as a catalog table. The reference
    view reads a Delta table without column mapping written the same way.
    """
    column_mapping_mode, partitioned = request.param
    suffix = f"{column_mapping_mode}_{'part' if partitioned else 'flat'}"
    path = tmp_path_factory.mktemp(f"delta_cm_{suffix}") / "table"
    _write_delta(
        _load_rows(spark, _ROWS_FIRST),
        path,
        mode="overwrite",
        column_mapping_mode=column_mapping_mode,
        partitioned=partitioned,
    )
    _write_delta(
        _load_rows(spark, _ROWS_SECOND),
        path,
        mode="append",
        column_mapping_mode=column_mapping_mode,
        partitioned=partitioned,
    )

    reference_path = path.parent / "reference"
    for rows, mode in ((_ROWS_FIRST, "overwrite"), (_ROWS_SECOND, "append")):
        _write_delta(
            _load_rows(spark, rows), reference_path, mode=mode, column_mapping_mode="none", partitioned=partitioned
        )

    reference = f"cm_reference_{suffix}"
    path_view = f"cm_path_{suffix}"
    table = f"cm_table_{suffix}"
    spark.read.format("delta").load(str(reference_path)).createOrReplaceTempView(reference)
    spark.read.format("delta").load(str(path)).createOrReplaceTempView(path_view)
    spark.sql(f"CREATE TABLE {table} USING delta LOCATION '{escape_sql_string_literal(str(path))}'")
    yield {
        "reference": reference,
        "path": path_view,
        "table": table,
        "location": str(path),
        "partitioned": partitioned,
    }
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.catalog.dropTempView(path_view)
    spark.catalog.dropTempView(reference)


def test_column_mapped_table_is_column_mapped(column_mapped_views: dict) -> None:
    commit = Path(column_mapped_views["location"]) / "_delta_log" / "00000000000000000000.json"
    actions = [json.loads(line) for line in commit.read_text().splitlines()]
    metadata = next(action["metaData"] for action in actions if "metaData" in action)
    assert metadata["configuration"]["delta.columnMapping.mode"] in {"name", "id"}


@pytest.mark.parametrize("access", ["path", "table"])
def test_column_mapped_schema_matches_reference(spark: SparkSession, column_mapped_views: dict, access: str) -> None:
    schema = spark.table(column_mapped_views[access]).schema
    reference = spark.table(column_mapped_views["reference"]).schema
    assert [(f.name, f.dataType) for f in schema.fields] == [(f.name, f.dataType) for f in reference.fields]


@pytest.mark.parametrize("access", ["path", "table"])
@pytest.mark.parametrize(("name", "query"), list(_QUERIES.items()), ids=list(_QUERIES))
def test_column_mapped_query_matches_reference(
    request, spark: SparkSession, column_mapped_views: dict, access: str, name: str, query: str
) -> None:
    if name in _KNOWN_QUERY_FAILURES:
        reason, partitioned_only = _KNOWN_QUERY_FAILURES[name]
        if column_mapped_views["partitioned"] or not partitioned_only:
            request.applymarker(pytest.mark.xfail(reason=reason, strict=True))
    actual = spark.sql(query.format(t=column_mapped_views[access])).collect()
    expected = spark.sql(query.format(t=column_mapped_views["reference"])).collect()
    assert actual == expected


_DATAFRAME_OPERATIONS = {
    "drop_duplicates_scalar": lambda df: df.dropDuplicates(["grp"]).select("grp").orderBy("grp"),
    "drop_duplicates_struct": lambda df: df.select("s").dropDuplicates().orderBy("s.a", "s.inner.d"),
    "select_nested_columns": lambda df: df.select("id", "s.inner.c", F.get("arr_s", 0)["y"].alias("y")).orderBy("id"),
    "filter_nested_column": lambda df: df.filter(F.col("s.inner.c") >= F.lit(30)).select("id").orderBy("id"),
    "with_field": lambda df: df.select("id", F.col("s").withField("a", F.lit(0)).alias("s")).orderBy("id"),
    "drop_fields": lambda df: df.select("id", F.col("s").dropFields("inner").alias("s")).orderBy("id"),
    "with_column": lambda df: df.withColumn("a2", F.col("s.a") + 1).select("id", "a2", "s").orderBy("id"),
    "group_by_agg": lambda df: (
        df.groupBy("grp")
        .agg(F.count("*").alias("n"), F.max("s.inner").alias("mi"), F.collect_set("s.b").alias("bs"))
        .select("grp", "n", "mi", F.array_sort("bs").alias("bs"))
        .orderBy("grp")
    ),
    "union_by_name": lambda df: (
        df.select("id", "s", "m").unionByName(df.select("m", "s", "id")).orderBy("id").select("id", "s", "m")
    ),
    "fill_na": lambda df: df.select("id", "name", "dbl").na.fill({"name": "none", "dbl": 0.0}).orderBy("id"),
    "drop_na": lambda df: df.select("id", "s", "arr").na.drop().orderBy("id"),
    "explode": lambda df: df.select("id", F.explode("arr_s").alias("e")).select("id", "e.x", "e.y").orderBy("id", "x"),
    "sort_desc": lambda df: df.select("id", "s").orderBy(F.col("s.inner.c").desc_nulls_last(), "id"),
    "join_on_struct_field": lambda df: (
        df.select(F.col("id").alias("l"), F.col("s.a").alias("k"))
        .join(df.select(F.col("id").alias("r"), F.col("s.a").alias("k")), "k")
        .select("l", "r")
        .orderBy("l", "r")
    ),
    "distinct": lambda df: df.select("grp", "flag").distinct().orderBy("grp", "flag"),
}


@pytest.mark.parametrize("access", ["path", "table"])
@pytest.mark.parametrize("operation", list(_DATAFRAME_OPERATIONS.values()), ids=list(_DATAFRAME_OPERATIONS))
def test_column_mapped_dataframe_operation_matches_reference(
    spark: SparkSession, column_mapped_views: dict, access: str, operation
) -> None:
    actual = operation(spark.table(column_mapped_views[access])).collect()
    expected = operation(spark.table(column_mapped_views["reference"])).collect()
    assert actual == expected


class _TablePair:
    """A column-mapped Delta table and a Delta table without column mapping holding the same rows."""

    def __init__(self, spark: SparkSession, base: Path, column_mapping_mode: str, *, partitioned: bool) -> None:
        self.spark = spark
        self.partitioned = partitioned
        self.paths = {"mapped": base / "mapped", "plain": base / "plain"}
        self.modes = {"mapped": column_mapping_mode, "plain": "none"}
        for key, path in self.paths.items():
            _write_delta(
                _load_rows(spark, _ROWS_FIRST + _ROWS_SECOND),
                path,
                mode="overwrite",
                column_mapping_mode=self.modes[key],
                partitioned=partitioned,
            )

    def table(self, key: str) -> str:
        return f"delta.`{self.paths[key]}`"

    def catalog_sql(self, statement: str) -> None:
        """Run a statement against catalog tables registered over both tables."""
        for key, path in self.paths.items():
            name = f"cm_pair_{key}"
            self.spark.sql(f"CREATE TABLE {name} USING delta LOCATION '{escape_sql_string_literal(str(path))}'")
            try:
                self.spark.sql(statement.format(t=name)).collect()
            finally:
                self.spark.sql(f"DROP TABLE IF EXISTS {name}")

    def sql(self, statement: str) -> None:
        for key in self.paths:
            self.spark.sql(statement.format(t=self.table(key))).collect()

    def write(self, df: DataFrame, *, mode: str, **options: str) -> None:
        for path in self.paths.values():
            writer = df.write.format("delta").mode(mode)
            for name, value in options.items():
                writer = writer.option(name, value)
            writer.save(str(path))

    def read(self, key: str, **options: str) -> DataFrame:
        reader = self.spark.read.format("delta")
        for name, value in options.items():
            reader = reader.option(name, value)
        return reader.load(str(self.paths[key]))

    def assert_same(self, **options: str) -> None:
        mapped = self.read("mapped", **options)
        plain = self.read("plain", **options)
        assert mapped.schema == plain.schema
        assert _sorted_rows(mapped) == _sorted_rows(plain)
        for query in ("filter_struct_field", "group_by_struct", "explode_map", "self_join_struct"):
            sql = _QUERIES[query]
            mapped_view = f"cm_dml_mapped_{query}"
            plain_view = f"cm_dml_plain_{query}"
            mapped.createOrReplaceTempView(mapped_view)
            plain.createOrReplaceTempView(plain_view)
            try:
                assert (
                    self.spark.sql(sql.format(t=mapped_view)).collect()
                    == self.spark.sql(sql.format(t=plain_view)).collect()
                ), query
            finally:
                self.spark.catalog.dropTempView(mapped_view)
                self.spark.catalog.dropTempView(plain_view)


@pytest.fixture(params=_TABLE_VARIANTS)
def table_pair(request, spark: SparkSession, tmp_path: Path) -> _TablePair:
    column_mapping_mode, partitioned = request.param
    return _TablePair(spark, tmp_path, column_mapping_mode, partitioned=partitioned)


def test_column_mapped_insert(table_pair: _TablePair) -> None:
    table_pair.catalog_sql("INSERT INTO {t} SELECT * FROM {t} WHERE id <= 3 OR s IS NULL")
    table_pair.assert_same()


def test_column_mapped_update(table_pair: _TablePair) -> None:
    table_pair.sql("UPDATE {t} SET dbl = coalesce(dbl, 0) + 1, name = upper(name) WHERE s.a > 2")
    table_pair.assert_same()
    table_pair.sql(
        "UPDATE {t} SET s = named_struct('a', s.a + 10, 'b', s.b, 'inner', s.inner),"
        " arr_s = array(named_struct('x', -1, 'y', 'updated')) WHERE array_contains(arr, 2)"
    )
    table_pair.assert_same()
    table_pair.sql("UPDATE {t} SET m_s = map('k9', named_struct('p', 9, 'q', 'q9')) WHERE m['k1'] IS NULL")
    table_pair.assert_same()


def test_column_mapped_delete(table_pair: _TablePair) -> None:
    table_pair.sql("DELETE FROM {t} WHERE s.inner.c = 30")
    table_pair.assert_same()
    table_pair.sql("DELETE FROM {t} WHERE grp IS NULL OR exists(arr_s, e -> e.x = 16)")
    table_pair.assert_same()


def test_column_mapped_dml_by_struct_value(table_pair: _TablePair) -> None:
    table_pair.sql(
        "UPDATE {t} SET name = 'matched' WHERE s = named_struct('a', 1, 'b', 'x',"
        " 'inner', named_struct('c', 10, 'd', 'inner1'))"
    )
    table_pair.assert_same()
    assert [row.id for row in table_pair.read("mapped").filter("name = 'matched'").collect()] == [1]
    table_pair.sql("DELETE FROM {t} WHERE s.inner = named_struct('c', 20, 'd', 'inner2')")
    table_pair.assert_same()
    assert sorted(row.id for row in table_pair.read("mapped").collect()) == [1, 3, 4, 5, 6, 7, 8]


def test_column_mapped_merge(spark: SparkSession, table_pair: _TablePair) -> None:
    source = _load_rows(
        spark,
        [
            _row(2, "g1", a=20, b="merged", c=200),
            _row(7, "g2", a=70, b="merged", c=700),
            _row(9, "g2", a=9, b="new", c=90),
        ],
    )
    source.createOrReplaceTempView("cm_merge_source")
    try:
        table_pair.sql(
            "MERGE INTO {t} AS target USING cm_merge_source AS source ON target.id = source.id"
            " WHEN MATCHED AND source.s.a > 50 THEN DELETE"
            " WHEN MATCHED THEN UPDATE SET target.s = source.s, target.m_s = source.m_s"
            " WHEN NOT MATCHED THEN INSERT *"
        )
    finally:
        spark.catalog.dropTempView("cm_merge_source")
    table_pair.assert_same()


def test_column_mapped_append_and_overwrite(spark: SparkSession, table_pair: _TablePair) -> None:
    extra = [_row(10, "g3", a=10, b="appended", c=100)]
    options = {"partitionBy": "grp"} if table_pair.partitioned else {}
    table_pair.write(_load_rows(spark, extra), mode="append", **options)
    table_pair.assert_same()
    table_pair.write(_load_rows(spark, _ROWS_SECOND), mode="overwrite", **options)
    table_pair.assert_same()


@pytest.mark.xfail(
    reason="replaceWhere fails with mismatched nested types in the union, also without column mapping", strict=True
)
def test_column_mapped_replace_where(spark: SparkSession, table_pair: _TablePair) -> None:
    replacement = [_row(11, "g1", a=11, b="replaced", c=110)]
    options = {"partitionBy": "grp"} if table_pair.partitioned else {}
    table_pair.write(_load_rows(spark, replacement), mode="overwrite", replaceWhere="grp = 'g1'", **options)
    table_pair.assert_same()


def test_column_mapped_schema_evolution(spark: SparkSession, table_pair: _TablePair) -> None:
    evolved = (
        _load_rows(spark, [_row(12, "g1", a=12, b="evolved", c=120)])
        .withColumn("extra", F.when(F.col("id").isNotNull(), F.lit("e")))
        .withColumn("s", F.col("s").withField("added", F.when(F.col("id").isNotNull(), F.lit(1))))
    )
    options = {"partitionBy": "grp"} if table_pair.partitioned else {}
    table_pair.write(evolved, mode="append", mergeSchema="true", **options)
    table_pair.assert_same()
    for key in table_pair.paths:
        df = table_pair.read(key)
        assert df.schema["extra"].dataType == StringType()
        assert df.schema["s"].dataType["added"].dataType == IntegerType()


@pytest.mark.xfail(
    reason="reading old files after a mergeSchema append of a non-nullable column fails, also without column mapping",
    strict=True,
)
def test_column_mapped_schema_evolution_non_nullable_column(spark: SparkSession, table_pair: _TablePair) -> None:
    evolved = _load_rows(spark, [_row(12, "g1", a=12, b="evolved", c=120)]).withColumn("extra", F.lit("e"))
    options = {"partitionBy": "grp"} if table_pair.partitioned else {}
    table_pair.write(evolved, mode="append", mergeSchema="true", **options)
    table_pair.assert_same()


@pytest.mark.skip(
    reason="reading old files after a mergeSchema append of a non-nullable nested field panics the server,"
    " also without column mapping"
)
def test_column_mapped_schema_evolution_non_nullable_nested_field(spark: SparkSession, table_pair: _TablePair) -> None:
    evolved = _load_rows(spark, [_row(12, "g1", a=12, b="evolved", c=120)]).withColumn(
        "s", F.col("s").withField("added", F.lit(1))
    )
    options = {"partitionBy": "grp"} if table_pair.partitioned else {}
    table_pair.write(evolved, mode="append", mergeSchema="true", **options)
    table_pair.assert_same()


@pytest.mark.xfail(
    reason="setting a struct column to NULL fails a type check, also without column mapping", strict=True
)
def test_column_mapped_update_struct_to_null(table_pair: _TablePair) -> None:
    table_pair.sql("UPDATE {t} SET s = NULL WHERE id = 1")
    table_pair.assert_same()


def test_column_mapped_time_travel(table_pair: _TablePair) -> None:
    table_pair.sql("DELETE FROM {t} WHERE id > 4")
    table_pair.sql(
        "UPDATE {t} SET name = 'travelled', s = named_struct('a', 0, 'b', '', 'inner', s.inner) WHERE id = 1"
    )
    for version in ("0", "1", "2"):
        table_pair.assert_same(versionAsOf=version)
    assert len(table_pair.read("mapped", versionAsOf="0").collect()) == len(_ROWS_FIRST) + len(_ROWS_SECOND)
