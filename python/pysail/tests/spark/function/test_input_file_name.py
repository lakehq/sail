import json
from collections import Counter
from itertools import pairwise
from pathlib import Path
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import Window
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

from pysail.testing.spark.session import spark_connect_server, spark_session_factory

PARQUET_SPLIT_ROW_COUNT = 16_384
PARQUET_SPLIT_ROW_GROUP_SIZE = 512
PARQUET_SPLIT_PAYLOAD_SIZE = 512
DATAFUSION_REPARTITION_FILE_MIN_SIZE = 1024 * 1024


@pytest.fixture
def parquet_metadata(spark, tmp_path):
    return _parquet_metadata(spark, tmp_path, [("a.parquet", [1, 2]), ("b.parquet", [3, 4])])


def _parquet_metadata(spark, tmp_path, groups):
    files = {}
    for name, ids in groups:
        path = tmp_path / name
        pq.write_table(pa.table({"id": ids, "arr": [[value, value + 1] for value in ids]}), path)
        files.update({value: (path.as_uri(), 0, path.stat().st_size) for value in ids})
    return spark.read.parquet(str(tmp_path)), files


@pytest.mark.parametrize("boundary", ["lambda", "coalesce", "union", "union_coalesce"])
@pytest.mark.parametrize("limited", [False, True])
def test_input_file_metadata_survives_projection_boundaries(spark, parquet_metadata, boundary, limited):
    dataframe, expected = parquet_metadata
    if boundary == "lambda":
        dataframe = dataframe.selectExpr("id", "transform(arr, x -> x + id) AS arr")
    elif boundary == "coalesce":
        dataframe = dataframe.coalesce(1)
    else:
        dataframe = dataframe.select("id").union(spark.range(5, 7))
        expected.update({5: ("", -1, -1), 6: ("", -1, -1)})
        if boundary == "union_coalesce":
            dataframe = dataframe.coalesce(1)

    dataframe = dataframe.select("*", *_metadata_columns())
    rows = _assert_metadata_rows(
        dataframe.limit(3) if limited else dataframe, expected, 3 if limited else len(expected)
    )
    if boundary == "lambda":
        assert all(row.arr == [2 * row.id, 2 * row.id + 1] for row in rows)


# Limit/offset and expression contracts verified with Spark 4.0.4 (ANSI, UTC).
# File URIs and lengths come from independently written artifacts, not query results.
METADATA_SCHEMA = StructType(
    [
        StructField("file_name", StringType(), False),
        StructField("block_start", LongType(), False),
        StructField("block_length", LongType(), False),
    ]
)


def _metadata_columns():
    return [
        F.input_file_name().alias("file_name"),
        F.input_file_block_start().alias("block_start"),
        F.input_file_block_length().alias("block_length"),
    ]


def _assert_metadata_rows(dataframe, expected, count, *, nullable=False):
    schema = StructType([StructField(field.name, field.dataType, nullable) for field in METADATA_SCHEMA])
    assert StructType([dataframe.schema[field.name] for field in schema]) == schema
    rows = dataframe.collect()
    assert len(rows) == count
    assert len({row.id for row in rows}) == count
    for row in rows:
        assert (row.file_name, row.block_start, row.block_length) == expected[row.id]
    return rows


@pytest.mark.parametrize("api", ["sql", "dataframe"])
@pytest.mark.parametrize("limit", [0, 1, 3, 9])
@pytest.mark.parametrize("select_after", [False, True])
def test_input_file_metadata_limit(spark, parquet_metadata, api, limit, select_after):
    dataframe, files = parquet_metadata
    if api == "sql":
        dataframe.createOrReplaceTempView("input_file_limit")
        try:
            source = (
                f"(SELECT * FROM input_file_limit LIMIT {limit})"  # noqa: S608
                if select_after
                else "input_file_limit"
            )
            suffix = "" if select_after else f" LIMIT {limit}"
            dataframe = spark.sql(
                "SELECT id, input_file_name() AS file_name, input_file_block_start() AS block_start, "  # noqa: S608
                f"input_file_block_length() AS block_length FROM {source}{suffix}"
            )
            schema = dataframe.schema
            _assert_metadata_rows(
                dataframe, dict.fromkeys(files, ("", -1, -1)) if select_after else files, min(limit, len(files))
            )
        finally:
            spark.catalog.dropTempView("input_file_limit")
    else:
        dataframe = (
            dataframe.limit(limit).select("id", *_metadata_columns())
            if select_after
            else dataframe.select("id", *_metadata_columns()).limit(limit)
        )
        schema = dataframe.schema
        _assert_metadata_rows(
            dataframe, dict.fromkeys(files, ("", -1, -1)) if select_after else files, min(limit, len(files))
        )
    assert schema == StructType([StructField("id", LongType(), True), *METADATA_SCHEMA])


@pytest.mark.parametrize(
    ("boundary", "placement", "count"),
    [
        ("limit", "before", 1),
        ("limit", "between", 1),
        ("limit", "after", 1),
        ("offset", "before", 1),
        ("offset", "between", 1),
        ("offset", "after", 1),
        ("exhausted", "before", 0),
        ("exhausted", "after", 0),
    ],
)
def test_input_file_metadata_nested_limits_offsets(parquet_metadata, boundary, placement, count):
    dataframe, files = parquet_metadata
    if placement == "before":
        dataframe = dataframe.select("id", *_metadata_columns())
    dataframe = dataframe.limit(3) if boundary == "limit" else dataframe.offset(9 if boundary == "exhausted" else 1)
    if placement == "between":
        dataframe = dataframe.select("id", *_metadata_columns())
    dataframe = dataframe.limit(1) if boundary == "limit" else dataframe.offset(1).limit(1)
    if placement == "after":
        dataframe = dataframe.select("id", *_metadata_columns())
    _assert_metadata_rows(dataframe, files if placement == "before" else dict.fromkeys(files, ("", -1, -1)), count)


@pytest.mark.parametrize(("distinct", "limit"), [(False, 3), (True, 9), (True, 1)])
def test_input_file_metadata_only_limit(parquet_metadata, distinct, limit):
    dataframe, files = parquet_metadata
    dataframe = dataframe.select(*_metadata_columns())
    if distinct:
        dataframe = dataframe.distinct()
    dataframe = dataframe.limit(limit)
    assert dataframe.schema == METADATA_SCHEMA
    actual = Counter(tuple(row) for row in dataframe.collect())
    expected = Counter(set(files.values())) if distinct else Counter(files.values())
    assert sum(actual.values()) == min(limit, sum(expected.values()))
    assert actual <= expected


def test_input_file_metadata_nested_expressions_limit(parquet_metadata):
    dataframe, files = parquet_metadata
    dataframe = dataframe.select(
        "id",
        F.struct(*_metadata_columns()).alias("bundle"),
        F.array(F.input_file_name(), F.input_file_name()).alias("duplicate"),
        F.concat(F.input_file_name(), F.lit(":"), F.input_file_block_length().cast("string")).alias("label"),
    ).limit(2)
    assert dataframe.schema == StructType(
        [
            StructField("id", LongType(), True),
            StructField("bundle", METADATA_SCHEMA, False),
            StructField("duplicate", ArrayType(StringType(), False), False),
            StructField("label", StringType(), False),
        ]
    )
    rows = dataframe.collect()
    assert len(rows) == len({row.id for row in rows}) == 2  # noqa: PLR2004
    for row in rows:
        name, start, length = files[row.id]
        assert tuple(row.bundle) == (name, start, length)
        assert row.duplicate == [name, name]
        assert row.label == f"{name}:{length}"


def test_input_file_metadata_filename_filter_limit(parquet_metadata):
    dataframe, files = parquet_metadata
    dataframe = dataframe.where(F.input_file_name() == files[1][0]).select("id", *_metadata_columns()).limit(1)
    _assert_metadata_rows(dataframe, {key: files[key] for key in [1, 2]}, 1)


# Semantic ports of Spark test_functions.test_input_file_name_udf and
# test_udf.test_udf_with_input_file_name; extended to all three metadata functions.
@pytest.mark.parametrize("receiving", [False, True])
@pytest.mark.parametrize("limited", [False, True])
def test_input_file_metadata_python_udf(parquet_metadata, receiving, limited):
    dataframe, files = parquet_metadata
    if receiving:
        string_identity = F.udf(lambda value: value, "string")
        long_identity = F.udf(lambda value: value, "long")
        dataframe = dataframe.select(
            "id",
            string_identity(F.input_file_name()).alias("file_name"),
            long_identity(F.input_file_block_start()).alias("block_start"),
            long_identity(F.input_file_block_length()).alias("block_length"),
        )
    else:
        sibling = F.udf(lambda value: value + 10, "long")
        dataframe = dataframe.select("id", sibling("id").alias("sibling"), *_metadata_columns())
        assert dataframe.schema["sibling"] == StructField("sibling", LongType(), True)
    rows = _assert_metadata_rows(
        dataframe.limit(1) if limited else dataframe, files, 1 if limited else 4, nullable=receiving
    )
    if not receiving:
        assert all(row.sibling == row.id + 10 for row in rows)


@pytest.mark.parametrize("empty", ["parquet", "csv", "filter"])
def test_input_file_metadata_empty_and_state_reset(spark, tmp_path, parquet_metadata, empty):
    dataframe, files = parquet_metadata
    # Spark test_input_file_name_reset_for_rdd, adapted to a Connect-compatible scan.
    _assert_metadata_rows(dataframe.select("id", *_metadata_columns()), files, 4)
    defaults = dict.fromkeys(range(3), ("", -1, -1))
    _assert_metadata_rows(spark.range(3).select("id", *_metadata_columns()), defaults, 3)
    if empty == "parquet":
        path = tmp_path / "empty.parquet"
        pq.write_table(pa.table({"id": pa.array([], type=pa.int64())}), path)
        dataframe = spark.read.parquet(str(path))
    elif empty == "csv":
        path = tmp_path / "empty-csv"
        path.mkdir()
        dataframe = spark.read.schema("id long").csv(str(path))
    else:
        dataframe = dataframe.where("id < 0")
    _assert_metadata_rows(dataframe.select("id", *_metadata_columns()).limit(1), {}, 0)
    _assert_metadata_rows(spark.range(3).select("id", *_metadata_columns()).limit(1), defaults, 1)


@pytest.mark.parametrize("file_format", ["csv", "json", "text", "binaryFile", "avro", "arrow"])
def test_input_file_metadata_reader_limit(spark, tmp_path, file_format):
    path = tmp_path / f"rows.{file_format}"
    reader = spark.read.format(file_format)
    if file_format == "csv":
        path.write_text("1\n2\n", encoding="utf-8")
        reader = reader.schema("id long")
    elif file_format == "json":
        path.write_text('{"id": 1}\n{"id": 2}\n', encoding="utf-8")
        reader = reader.schema("id long")
    elif file_format in ["text", "binaryFile"]:
        path.write_bytes(b"first\nsecond\n")
    elif file_format == "arrow":
        table = pa.table({"id": [1, 2]})
        with pa.OSFile(str(path), "wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
    else:
        # Minimal Avro OCF, null codec, two long records; no optional writer dependency.
        def encode_long(value):
            value <<= 1
            result = bytearray()
            while value > 127:  # noqa: PLR2004
                result.append((value & 127) | 128)
                value >>= 7
            return bytes([*result, value])

        def encode_bytes(value):
            return encode_long(len(value)) + value

        schema = json.dumps({"type": "record", "name": "row", "fields": [{"name": "id", "type": "long"}]}).encode()
        metadata = encode_long(1) + encode_bytes(b"avro.schema") + encode_bytes(schema) + encode_long(0)
        sync = b"input-file-smoke"
        assert len(sync) == 16  # noqa: PLR2004
        records = encode_long(1) + encode_long(2)
        path.write_bytes(b"Obj\x01" + metadata + sync + encode_long(2) + encode_long(len(records)) + records + sync)
    # Spark 4.0.4 verified CSV/JSON/text/binaryFile. Avro's module was unavailable,
    # and Arrow is unsupported there; those expectations use only the file artifact.
    dataframe = reader.load(str(path)).select(*_metadata_columns()).limit(1)
    assert dataframe.schema == METADATA_SCHEMA
    assert [tuple(row) for row in dataframe.collect()] == [(path.as_uri(), 0, path.stat().st_size)]


@pytest.mark.parametrize("select_after", [False, True])
def test_input_file_metadata_topk(parquet_metadata, select_after):
    dataframe, files = parquet_metadata
    if not select_after:
        dataframe = dataframe.select("id", *_metadata_columns())
    dataframe = dataframe.orderBy("id").limit(3)
    if select_after:
        dataframe = dataframe.select("id", *_metadata_columns())
    rows = _assert_metadata_rows(dataframe, dict.fromkeys(files, ("", -1, -1)) if select_after else files, 3)
    assert [row.id for row in rows] == [1, 2, 3]


@pytest.mark.parametrize(
    ("expression", "string_result"),
    [
        ("concat(input_file_name(), ':', cast(x + y AS STRING))", True),
        ("input_file_block_start() + x + y", False),
        ("input_file_block_length() + x + y", False),
    ],
)
def test_input_file_metadata_after_sort_narrow_projection(spark, tmp_path, expression, string_result):
    # Three scan columns and two projected columns exercise narrowing across Sort.
    path = tmp_path / "three-columns.parquet"
    pq.write_table(pa.table({"id": [1, 2], "x": [10, 20], "y": [100, 200]}), path)
    dataframe = spark.read.parquet(str(path)).orderBy("id").selectExpr("id", f"{expression} AS value")
    assert dataframe.schema == StructType(
        [
            StructField("id", LongType(), True),
            StructField("value", StringType() if string_result else LongType(), True),
        ]
    )
    assert [tuple(row) for row in dataframe.collect()] == [
        (key, f":{110 * key}" if string_result else 110 * key - 1) for key in [1, 2]
    ]


@pytest.mark.parametrize("select_after", [False, True])
def test_input_file_metadata_sql_window_siblings(spark, parquet_metadata, select_after):
    dataframe, files = parquet_metadata
    dataframe.createOrReplaceTempView("input_file_window")
    metadata = (
        "input_file_name() AS file_name, input_file_block_start() AS block_start, "
        "input_file_block_length() AS block_length"
    )
    try:
        query = (
            f"SELECT id, rn, {metadata} FROM "  # noqa: S608
            "(SELECT id, row_number() OVER (ORDER BY id) AS rn FROM input_file_window) q"
            if select_after
            else f"SELECT id, row_number() OVER (ORDER BY id) AS rn, {metadata} FROM input_file_window"  # noqa: S608
        )
        dataframe = spark.sql(query)
        assert dataframe.schema["rn"].dataType.simpleString() == "int"
        assert not dataframe.schema["rn"].nullable
        rows = _assert_metadata_rows(dataframe, dict.fromkeys(files, ("", -1, -1)) if select_after else files, 4)
        assert {row.id: row.rn for row in rows} == {key: key for key in files}
    finally:
        spark.catalog.dropTempView("input_file_window")


# These projection boundaries were verified with Spark 4.0.4 (ANSI, UTC).
@pytest.mark.parametrize("projection", ["withColumns", "successive", "keys", "multiple", "collision", "aggregate"])
def test_input_file_metadata_dataframe_window_siblings(parquet_metadata, projection):
    dataframe, files = parquet_metadata
    window = Window.orderBy("id")
    if projection == "aggregate":
        dataframe = dataframe.groupBy("id").count()
    if projection == "collision":
        dataframe = dataframe.withColumn("__sail_input_file_metadata_0", F.col("id") * 100)
    if projection == "keys":
        window = Window.partitionBy(
            F.input_file_name(), F.input_file_block_start(), F.input_file_block_length()
        ).orderBy("id")
    expressions = {"rn": F.row_number().over(window)}
    if projection == "multiple":
        expressions["reverse_rn"] = F.row_number().over(Window.orderBy(F.desc("id")))
    if projection == "successive":
        dataframe = dataframe.withColumn("rn", expressions["rn"])
        for name, expression in zip(METADATA_SCHEMA.fieldNames(), _metadata_columns(), strict=True):
            dataframe = dataframe.withColumn(name, expression)
    else:
        expressions.update(zip(METADATA_SCHEMA.fieldNames(), _metadata_columns(), strict=True))
        dataframe = dataframe.withColumns(expressions)
    expected = dict.fromkeys(files, ("", -1, -1)) if projection in ["successive", "aggregate"] else files
    rows = _assert_metadata_rows(dataframe, expected, len(files))
    for row in rows:
        assert row.rn == ((row.id - 1) % 2 + 1 if projection == "keys" else row.id)
        if projection == "multiple":
            assert row.reverse_rn == len(files) + 1 - row.id
        if projection == "collision":
            assert row["__sail_input_file_metadata_0"] == row.id * 100


def test_input_file_metadata_nested_window_free_siblings(parquet_metadata):
    dataframe, files = parquet_metadata
    rows = dataframe.select(
        "id",
        F.row_number().over(Window.orderBy("id")).alias("rn"),
        F.struct(*_metadata_columns()).alias("bundle"),
        F.transform(
            "arr",
            lambda value: F.struct(
                F.concat(F.input_file_name(), F.lit(":"), value.cast("string")).alias("name"),
                (F.input_file_block_start() + value).alias("start"),
                (F.input_file_block_length() + value).alias("length"),
            ),
        ).alias("mapped"),
    ).collect()
    assert {row.id: row.rn for row in rows} == {key: key for key in files}
    for row in rows:
        name, start, length = files[row.id]
        assert tuple(row.bundle) == files[row.id]
        assert [tuple(value) for value in row.mapped] == [
            (f"{name}:{value}", start + value, length + value) for value in [row.id, row.id + 1]
        ]


def test_input_file_metadata_combined_with_window_uses_defaults(parquet_metadata):
    dataframe, files = parquet_metadata
    rows = dataframe.selectExpr(
        "id",
        "concat(input_file_name(), ':', cast(row_number() OVER (ORDER BY id) AS STRING)) AS name",
        "input_file_block_start() + row_number() OVER (ORDER BY id) AS start",
        "input_file_block_length() + row_number() OVER (ORDER BY id) AS length",
    ).collect()
    assert sorted(tuple(row) for row in rows) == [(key, f":{key}", key - 1, key - 1) for key in files]


def test_input_file_metadata_join_predicate_error_precedes_multiple_sources(spark, parquet_metadata):
    dataframe, _ = parquet_metadata
    dataframe.createOrReplaceTempView("input_file_join")
    try:
        with pytest.raises(Exception, match="INVALID_NON_DETERMINISTIC_EXPRESSIONS"):
            spark.sql(
                "SELECT l.id, input_file_name(), input_file_block_start(), input_file_block_length() "
                "FROM input_file_join l JOIN input_file_join r ON l.id = r.id "
                "AND input_file_name() != 'never' "
                "AND input_file_block_start() < 0 AND input_file_block_length() < 0"
            ).collect()
    finally:
        spark.catalog.dropTempView("input_file_join")


@pytest.mark.parametrize(
    "predicate", ["input_file_name() != 'never'", "input_file_block_start() < 0", "input_file_block_length() < 0"]
)
@pytest.mark.parametrize("comparison", ["l.id = r.id", "l.id < r.id"])
def test_input_file_metadata_rejected_in_join_predicate(spark, predicate, comparison):
    with pytest.raises(Exception, match="INVALID_NON_DETERMINISTIC_EXPRESSIONS"):
        spark.sql(
            "SELECT l.id AS lid, r.id AS rid FROM range(1, 5) l JOIN range(1, 5) r "  # noqa: S608
            f"ON {comparison} AND {predicate}"
        ).collect()


@pytest.fixture
def streamed_join_input(tmp_path):
    # Keep the file probe in the same execution context; an optimizer-inserted
    # exchange at normal parallelism is deliberately not a streamed-file contract.
    with (
        spark_connect_server({"SAIL_EXECUTION__DEFAULT_PARALLELISM": "1"}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        spark = sessions.create()
        dataframe, files = _parquet_metadata(spark, tmp_path, [("a.parquet", [101, 103]), ("b.parquet", [105, 107])])
        dataframe.createOrReplaceTempView("input_file_streamed_join")
        yield spark, dataframe, files


# Spark 4.0.4, AQE off, one partition, non-file broadcast build. Spark's
# right semi/anti oracle reverses the SQL operands to left semi/anti. Sail
# uses no hints. Expected triples are independent Parquet URI/stat values.
@pytest.mark.parametrize(
    "context", ["inner", "right", "right_semi", "right_anti", "duplicates", "fetch", "cross", "full"]
)
def test_input_file_metadata_streamed_join(streamed_join_input, context):
    spark, _, files = streamed_join_input
    source = "range(101,104) r"
    operator = {"right": "RIGHT", "right_semi": "RIGHT SEMI", "right_anti": "RIGHT ANTI", "full": "FULL"}.get(
        context, "INNER"
    )
    predicate = "r.id=t.id"
    rid = "r.id"
    pairs = [(101, 101), (103, 103)]
    if context == "right":
        pairs += [(None, 105), (None, 107)]
    elif context in ["right_semi", "right_anti"]:
        rid = "CAST(NULL AS BIGINT)"
        pairs = [(None, key) for key in ([101, 103] if context == "right_semi" else [105, 107])]
    elif context == "duplicates":
        source = (
            "(SELECT CAST(101 AS BIGINT) AS id UNION ALL SELECT CAST(101 AS BIGINT) AS id "
            "UNION ALL SELECT CAST(103 AS BIGINT) AS id) r"
        )
        pairs.append((101, 101))
    elif context == "cross":
        source = "range(101,102) r"
        pairs = [(101, key) for key in files]
    elif context == "full":
        predicate = "r.id=t.id OR (r.id=101 AND t.id=105)"
        pairs += [(101, 105), (None, 107), (102, None)]
    join = (
        f"{source} CROSS JOIN input_file_streamed_join t"
        if context == "cross"
        else f"{source} {operator} JOIN input_file_streamed_join t ON {predicate}"
    )
    suffix = " LIMIT 1" if context == "fetch" else ""
    dataframe = spark.sql(
        f"SELECT {rid} AS rid,t.id AS fileid,input_file_name() AS file_name,"  # noqa: S608
        "input_file_block_start() AS block_start,input_file_block_length() AS block_length "
        f"FROM {join}{suffix}"
    )
    assert dataframe.schema == StructType(
        [
            StructField("rid", LongType(), context in ["right", "right_semi", "right_anti", "full"]),
            StructField("fileid", LongType(), True),
            *METADATA_SCHEMA,
        ]
    )
    expected = Counter((rid, fileid, *files.get(fileid, ("", -1, -1))) for rid, fileid in pairs)
    actual = Counter(tuple(row) for row in dataframe.collect())
    if context == "fetch":
        assert sum(actual.values()) == 1
        assert actual <= expected
    else:
        assert actual == expected


@pytest.mark.parametrize(
    ("extra_column", "retain_id"),
    [
        pytest.param("extra", False, id="narrowed"),
        pytest.param("x", False, id="lambda-name-collision"),
        pytest.param("extra", True, id="repeated-computed-column"),
    ],
)
@pytest.mark.parametrize("with_metadata", [False, True], ids=["control", "metadata"])
def test_input_file_metadata_streamed_join_narrowed_lambda(
    streamed_join_input, tmp_path, extra_column, retain_id, with_metadata
):
    spark, _, _ = streamed_join_input
    path = tmp_path / "narrowed.parquet"
    pq.write_table(pa.table({"id": [101, 103], "arr": [[1, 2], [3, 4]], extra_column: [800, 900]}), path)
    spark.read.parquet(str(path)).createOrReplaceTempView("input_file_narrowed_probe")
    # Spark 4.0.4: narrowing must preserve both the captured computed column and
    # the lambda parameter, even when a raw field has the parameter's name.
    selected_id = "t.id, " if retain_id else ""
    probe_id = "id, " if retain_id else ""
    metadata = (
        ", input_file_name() AS file_name, input_file_block_start() AS block_start, "
        "input_file_block_length() AS block_length"
        if with_metadata
        else ""
    )
    try:
        dataframe = spark.sql(
            f"SELECT {selected_id}t.mapped{metadata} FROM range(101,102) r CROSS JOIN "  # noqa: S608
            f"(SELECT {probe_id}transform(arr, x -> x + id) AS mapped "
            f"FROM (SELECT id + {extra_column} AS id, arr FROM input_file_narrowed_probe) q) t"
        )
        rows = dataframe.collect()
        assert sorted(row.mapped for row in rows) == [[902, 903], [1006, 1007]]
        if retain_id:
            # Selecting id again must not strand metadata when merging the full
            # projection would duplicate the source's id + extra expression.
            assert {row.id: row.mapped for row in rows} == {901: [902, 903], 1003: [1006, 1007]}
        if with_metadata:
            assert StructType([dataframe.schema[field.name] for field in METADATA_SCHEMA]) == METADATA_SCHEMA
            assert all(
                (row.file_name, row.block_start, row.block_length) == (path.as_uri(), 0, path.stat().st_size)
                for row in rows
            )
    finally:
        spark.catalog.dropTempView("input_file_narrowed_probe")


def test_input_file_metadata_filtered_file_build_uses_defaults(streamed_join_input):
    spark, _, _ = streamed_join_input
    dataframe = spark.sql(
        "SELECT t.id AS fileid,input_file_name() AS file_name,input_file_block_start() AS block_start,"
        "input_file_block_length() AS block_length FROM range(101,104) r "
        "JOIN input_file_streamed_join t ON r.id=t.id WHERE t.id=103"
    )
    assert dataframe.schema == StructType([StructField("fileid", LongType(), True), *METADATA_SCHEMA])
    assert [tuple(row) for row in dataframe.collect()] == [(103, "", -1, -1)]


@pytest.mark.parametrize("boundary", ["full_join", "probe_repartition"])
def test_input_file_metadata_join_preserves_materialized_values(streamed_join_input, boundary):
    spark, dataframe, files = streamed_join_input
    dataframe = dataframe.select("id", *_metadata_columns())
    if boundary == "probe_repartition":
        dataframe = dataframe.repartition(2)
    dataframe.createOrReplaceTempView("input_file_materialized_join")
    predicate = "r.id=t.id"
    pairs = [(101, 101), (103, 103)]
    if boundary == "full_join":
        predicate += " OR (r.id=101 AND t.id=105)"
        pairs += [(101, 105), (None, 107), (102, None)]
    join_type = "FULL" if boundary == "full_join" else "INNER"
    dataframe = spark.sql(
        "SELECT r.id AS rid,t.id AS fileid,t.file_name AS pre_file_name,"  # noqa: S608
        "t.block_start AS pre_block_start,t.block_length AS pre_block_length,"
        "input_file_name() AS file_name,input_file_block_start() AS block_start,"
        "input_file_block_length() AS block_length FROM range(101,104) r "
        f"{join_type} JOIN input_file_materialized_join t ON {predicate}"
    )
    assert dataframe.schema == StructType(
        [
            StructField("rid", LongType(), boundary == "full_join"),
            StructField("fileid", LongType(), True),
            *[StructField(f"pre_{field.name}", field.dataType, boundary == "full_join") for field in METADATA_SCHEMA],
            *METADATA_SCHEMA,
        ]
    )
    expected = Counter(
        (
            rid,
            fileid,
            *files.get(fileid, (None, None, None)),
            *(files[fileid] if boundary == "full_join" and fileid is not None else ("", -1, -1)),
        )
        for rid, fileid in pairs
    )
    assert Counter(tuple(row) for row in dataframe.collect()) == expected


def test_input_file_metadata_inside_lambda(parquet_metadata):
    dataframe, expected = parquet_metadata
    rows = dataframe.selectExpr(
        "id",
        "transform(arr, x -> concat(input_file_name(), cast(x AS string))) AS names",
        "transform(arr, x -> input_file_block_start() + x) AS starts",
        "transform(arr, x -> input_file_block_length() + x) AS lengths",
        "transform(arr, x -> transform(array(x, x + 1), "
        "y -> concat(input_file_name(), cast(x + y + id AS string)))) AS nested",
    ).collect()

    for row in rows:
        name, start, length = expected[row.id]
        assert row.names == [f"{name}{row.id}", f"{name}{row.id + 1}"]
        assert row.starts == [start + row.id, start + row.id + 1]
        assert row.lengths == [length + row.id, length + row.id + 1]
        assert row.nested == [
            [f"{name}{2 * value + row.id}", f"{name}{2 * value + row.id + 1}"] for value in [row.id, row.id + 1]
        ]


@pytest.mark.parametrize(
    "generator", ["explode", "explode_outer", "posexplode", "posexplode_outer", "inline", "inline_outer"]
)
@pytest.mark.parametrize("select_after", [False, True])
def test_input_file_metadata_survives_generators(parquet_metadata, generator, select_after):
    dataframe, files = parquet_metadata
    dataframe = dataframe.selectExpr("id", "CASE id WHEN 2 THEN array() WHEN 4 THEN NULL ELSE arr END AS arr")
    if generator.startswith("inline"):
        expression = f"{generator}(transform(arr, x -> named_struct('item', cast(x AS BIGINT)))) AS item"
    elif generator.startswith("posexplode"):
        expression = f"{generator}(arr) AS (pos, item)"
    else:
        expression = f"{generator}(arr) AS item"
    metadata = [
        "input_file_name() AS file_name",
        "input_file_block_start() AS block_start",
        "input_file_block_length() AS block_length",
    ]
    if select_after:
        dataframe = dataframe.selectExpr("id", expression).selectExpr("*", *metadata)
    else:
        dataframe = dataframe.selectExpr("id", expression, *metadata)
    rows = dataframe.collect()

    expected = [
        (key, item, *file)
        for key, file in files.items()
        for item in ([key, key + 1] if key % 2 else ([None] if generator.endswith("outer") else []))
    ]
    assert sorted((row.id, row.item, row.file_name, row.block_start, row.block_length) for row in rows) == expected
    if generator.startswith("posexplode"):
        assert sorted((row.id, row.pos) for row in rows) == [
            (key, position)
            for key in files
            for position in ([0, 1] if key % 2 else ([None] if generator.endswith("outer") else []))
        ]


@pytest.mark.parametrize("function", ["input_file_name", "input_file_block_start", "input_file_block_length"])
@pytest.mark.parametrize("grouping", ["expression", "alias", "ordinal"])
def test_input_file_metadata_sql_grouping(spark, parquet_metadata, function, grouping):
    dataframe, files = parquet_metadata
    index = ["input_file_name", "input_file_block_start", "input_file_block_length"].index(function)
    expected = Counter(metadata[index] for metadata in files.values())
    group = {"expression": f"{function}()", "alias": "metadata", "ordinal": "1"}[grouping]
    dataframe.createOrReplaceTempView("input_file_grouping")
    try:
        rows = spark.sql(
            f"SELECT {function}() AS metadata, count(*) AS n, max({function}()) AS maximum "  # noqa: S608
            f"FROM input_file_grouping GROUP BY {group}"
        ).collect()
        assert sorted(tuple(row) for row in rows) == sorted((value, count, value) for value, count in expected.items())
    finally:
        spark.catalog.dropTempView("input_file_grouping")


@pytest.mark.parametrize("function", ["input_file_name", "input_file_block_start", "input_file_block_length"])
@pytest.mark.parametrize("window", ["partition", "order", "argument"])
def test_input_file_metadata_in_window(parquet_metadata, function, window):
    dataframe, files = parquet_metadata
    index = ["input_file_name", "input_file_block_start", "input_file_block_length"].index(function)
    metadata = {key: value[index] for key, value in files.items()}
    if window == "partition":
        expression = f"row_number() OVER (PARTITION BY {function}() ORDER BY id)"
        expected = {
            key: sum(value == metadata[previous] for previous in files if previous <= key)
            for key, value in metadata.items()
        }
    elif window == "order":
        expression = f"row_number() OVER (ORDER BY {function}(), id)"
        expected = {
            key: position for position, key in enumerate(sorted(files, key=lambda key: (metadata[key], key)), 1)
        }
    else:
        expression = f"lag({function}()) OVER (ORDER BY id)"
        expected = {key: metadata.get(key - 1) for key in files}

    rows = dataframe.selectExpr("id", f"{expression} AS value").collect()
    assert {row.id: row.value for row in rows} == expected


@pytest.mark.parametrize("boundary", ["repartition", "sort", "aggregate"])
def test_input_file_metadata_after_shuffle_uses_defaults(parquet_metadata, boundary):
    dataframe, expected = parquet_metadata
    if boundary == "repartition":
        dataframe = dataframe.repartition(2)
    elif boundary == "sort":
        dataframe = dataframe.orderBy("id")
    else:
        dataframe = dataframe.groupBy("id").count()
    rows = dataframe.select(
        "id", F.input_file_name(), F.input_file_block_start(), F.input_file_block_length()
    ).collect()
    assert sorted(tuple(row) for row in rows) == [(key, "", -1, -1) for key in expected]


def test_input_file_metadata_materialized_before_shuffle(parquet_metadata):
    dataframe, expected = parquet_metadata
    rows = (
        dataframe.select("id", F.input_file_name(), F.input_file_block_start(), F.input_file_block_length())
        .repartition(2)
        .collect()
    )
    assert sorted(tuple(row) for row in rows) == [(key, *value) for key, value in expected.items()]


def test_input_file_name_escapes_uri_path(spark, tmp_path):
    directory = tmp_path / "space #hash %percent"
    directory.mkdir()
    path = directory / "file #100%.parquet"
    pq.write_table(pa.table({"id": [1]}), path)
    rows = spark.read.parquet(str(path)).select(F.input_file_name()).collect()
    assert [row[0] for row in rows] == [path.as_uri()]


@pytest.mark.parametrize("function", ["input_file_name", "input_file_block_start", "input_file_block_length"])
@pytest.mark.parametrize("nested_union", [False, True])
def test_input_file_metadata_rejects_multiple_file_sources(spark, parquet_metadata, function, nested_union):
    dataframe, _ = parquet_metadata
    joined = dataframe.alias("left").join(dataframe.alias("right"), "id").select("id")
    if nested_union:
        joined = joined.union(spark.range(5, 7))
    with pytest.raises(Exception, match="MULTI_SOURCES_UNSUPPORTED_FOR_EXPRESSION"):
        joined.selectExpr(f"{function}()").collect()


def test_input_file_metadata_can_be_materialized_before_join(parquet_metadata):
    dataframe, expected = parquet_metadata
    left = dataframe.select("id", F.input_file_name().alias("left_file"))
    right = dataframe.select("id", F.input_file_name().alias("right_file"))
    rows = left.join(right, "id").collect()
    assert {row.id: (row.left_file, row.right_file) for row in rows} == {
        key: (value[0], value[0]) for key, value in expected.items()
    }


def test_input_file_metadata_from_union_of_files(parquet_metadata):
    dataframe, expected = parquet_metadata
    rows = (
        dataframe.where("id <= 2")
        .union(dataframe.where("id > 2"))
        .select("id", F.input_file_name(), F.input_file_block_start(), F.input_file_block_length())
        .collect()
    )
    assert sorted(tuple(row) for row in rows) == [(key, *value) for key, value in expected.items()]


def _file_name(location: str) -> str:
    parsed = urlparse(location)
    path = unquote(parsed.path) if parsed.scheme else location
    return Path(path).name


def test_input_file_name_without_file_source(spark):
    dataframe = spark.range(2).select(
        F.input_file_name().alias("file_name"),
        F.input_file_block_start().alias("block_start"),
        F.input_file_block_length().alias("block_length"),
    )
    filtered = spark.range(2).where(F.input_file_name() == "").orderBy("id").collect()
    block_filtered = spark.range(2).where(F.input_file_block_start() == -1).orderBy("id").collect()
    grouped = spark.range(2).groupBy(F.input_file_name().alias("file_name")).count().collect()
    block_grouped = spark.range(2).groupBy(F.input_file_block_length().alias("block_length")).count().collect()

    assert [(row.file_name, row.block_start, row.block_length) for row in dataframe.collect()] == [
        ("", -1, -1),
        ("", -1, -1),
    ]
    assert dataframe.schema["file_name"].dataType.simpleString() == "string"
    assert not dataframe.schema["file_name"].nullable
    assert [row.id for row in filtered] == [0, 1]
    assert [row.id for row in block_filtered] == [0, 1]
    assert [(row.file_name, row["count"]) for row in grouped] == [("", 2)]
    assert [(row.block_length, row["count"]) for row in block_grouped] == [(-1, 2)]


def test_input_file_name_from_parquet_scan(spark, tmp_path):
    location = tmp_path / "input-file-name"
    spark.createDataFrame([(1,), (2,)], ["id"]).coalesce(1).write.mode("overwrite").parquet(str(location))
    [data_file] = location.glob("*.parquet")

    dataframe = spark.read.parquet(str(location)).select(
        "id",
        F.input_file_name().alias("file_name"),
        F.input_file_block_start().alias("block_start"),
        F.input_file_block_length().alias("block_length"),
    )
    rows = dataframe.orderBy("id").collect()

    assert [(row.id, _file_name(row.file_name), row.block_start, row.block_length) for row in rows] == [
        (1, data_file.name, 0, data_file.stat().st_size),
        (2, data_file.name, 0, data_file.stat().st_size),
    ]
    parsed = urlparse(rows[0].file_name)
    assert parsed.scheme == "file"
    assert Path(url2pathname(parsed.path)).resolve() == data_file.resolve()
    assert spark.read.parquet(str(location)).where(F.input_file_block_length() > 0).count() == len(rows)
    assert not dataframe.schema["file_name"].nullable
    assert not dataframe.schema["block_start"].nullable
    assert not dataframe.schema["block_length"].nullable


def test_input_file_block_metadata_from_parquet_splits(spark, tmp_path):
    data_file = tmp_path / "split-input-file.parquet"
    payloads = [f"{index:08d}-{'x' * PARQUET_SPLIT_PAYLOAD_SIZE}" for index in range(PARQUET_SPLIT_ROW_COUNT)]
    pq.write_table(
        pa.table({"id": range(PARQUET_SPLIT_ROW_COUNT), "payload": payloads}),
        data_file,
        row_group_size=PARQUET_SPLIT_ROW_GROUP_SIZE,
        compression="NONE",
        use_dictionary=False,
    )
    file_size = data_file.stat().st_size
    assert file_size > DATAFUSION_REPARTITION_FILE_MIN_SIZE

    metadata_rows = (
        spark.read.parquet(str(data_file))
        .select(
            F.input_file_name().alias("file_name"),
            F.input_file_block_start().alias("block_start"),
            F.input_file_block_length().alias("block_length"),
        )
        .groupBy("file_name", "block_start", "block_length")
        .count()
        .orderBy("block_start")
        .collect()
    )

    assert len(metadata_rows) > 1
    assert sum(row["count"] for row in metadata_rows) == PARQUET_SPLIT_ROW_COUNT
    assert all(
        Path(url2pathname(urlparse(row.file_name).path)).resolve() == data_file.resolve() for row in metadata_rows
    )

    ranges = [(row.block_start, row.block_length) for row in metadata_rows]
    assert ranges[0][0] == 0
    assert any(block_start > 0 for block_start, _ in ranges)
    assert all(block_length > 0 for _, block_length in ranges)
    assert all(
        block_start + block_length == next_start for (block_start, block_length), (next_start, _) in pairwise(ranges)
    )
    assert sum(block_length for _, block_length in ranges) == file_size


@pytest.mark.parametrize("metadata_as_data", [False, True])
def test_input_file_metadata_from_delta_scan(spark, tmp_path, metadata_as_data):
    location = tmp_path / "input-file-name-delta"
    first_ids = [1, 2]
    second_ids = [3, 4, 5]
    spark.createDataFrame([(value,) for value in first_ids], ["id"]).coalesce(1).write.format("delta").mode(
        "overwrite"
    ).save(str(location))
    spark.createDataFrame([(value,) for value in second_ids], ["id"]).coalesce(1).write.format("delta").mode(
        "append"
    ).save(str(location))
    data_files = {path.resolve() for path in location.glob("*.parquet")}

    rows = (
        spark.read.format("delta")
        .option("metadataAsDataRead", str(metadata_as_data).lower())
        .load(str(location))
        .select(
            "id",
            F.input_file_name().alias("file_name"),
            F.input_file_block_start().alias("block_start"),
            F.input_file_block_length().alias("block_length"),
        )
        .orderBy("id")
        .collect()
    )

    assert [row.id for row in rows] == [*first_ids, *second_ids]
    row_files = {}
    for row in rows:
        parsed = urlparse(row.file_name)
        data_file = Path(url2pathname(parsed.path)).resolve()
        assert parsed.scheme == "file"
        assert data_file in data_files
        assert row.block_start == 0
        assert row.block_length == data_file.stat().st_size
        row_files[row.id] = data_file

    first_file = {row_files[value] for value in first_ids}
    second_file = {row_files[value] for value in second_ids}
    assert len(first_file) == 1
    assert len(second_file) == 1
    assert first_file.isdisjoint(second_file)
    assert first_file | second_file == data_files


@pytest.mark.parametrize("column_mapping", ["name", "id"])
def test_input_file_metadata_restores_delta_columns(spark, tmp_path, column_mapping):
    location = tmp_path / "mapped-input-file"
    source = spark.createDataFrame(
        [(1, "east", (10,), 100), (2, "west", (20,), 200)],
        "id long, part string, payload struct<value:long>, __sail_input_file_metadata_0 long",
    )
    (
        source.write.format("delta")
        .option("delta.columnMapping.mode", column_mapping)
        .partitionBy("part")
        .save(str(location))
    )
    data_files = {path.resolve() for path in location.rglob("*.parquet")}
    rows = (
        spark.read.format("delta")
        .option("metadataAsDataRead", "true")
        .load(str(location))
        .selectExpr(
            "id",
            "input_file_name() AS file_name",
            "concat(part, ':', input_file_name()) AS label",
            "payload.value + input_file_block_start() AS value",
            "__sail_input_file_metadata_0 + input_file_block_start() AS original",
            "transform(array(id), x -> x + input_file_block_start()) AS transformed",
        )
        .orderBy("id")
        .collect()
    )
    assert [row.id for row in rows] == [1, 2]
    for row, part in zip(rows, ["east", "west"], strict=True):
        data_file = Path(url2pathname(urlparse(row.file_name).path)).resolve()
        assert data_file in data_files
        assert row.label == f"{part}:{row.file_name}"
        assert row.value == row.id * 10
        assert row.original == row.id * 100
        assert row.transformed == [row.id]
