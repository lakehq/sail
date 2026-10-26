# ruff: noqa: S608
import json
from contextlib import contextmanager

import pytest

from pysail.testing.spark.utils.sql import escape_sql_string_literal


def _table_files(path):
    return {p.relative_to(path): p.read_bytes() for p in path.rglob("*") if p.is_file()}


@pytest.fixture
def partition_table(spark, tmp_path):
    table = "partition_clause"
    settings = {"spark.sql.caseSensitive": "false", "spark.sql.sources.partitionOverwriteMode": "dynamic"}
    previous = {key: spark.conf.get(key) for key in settings}

    def create(definition="(id INT, msg STRING) USING delta PARTITIONED BY (id)", query=""):
        spark.sql(f"CREATE TABLE {table} {definition} LOCATION '{escape_sql_string_literal(str(tmp_path))}' {query}")
        return table

    try:
        for key, value in settings.items():
            spark.conf.set(key, value)
        yield create
    finally:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
        finally:
            for key, value in previous.items():
                spark.conf.set(key, value)


@contextmanager
def _reject_unchanged(spark, table, path, error):
    before = spark.table(table)
    rows, schema, files = before.orderBy(before.columns).collect(), before.schema, _table_files(path)
    with pytest.raises(Exception, match=error):
        yield
    after = spark.table(table)
    assert after.orderBy(after.columns).collect() == rows
    assert after.schema == schema
    assert _table_files(path) == files


@pytest.mark.parametrize(
    ("mode", "columns", "projection"),
    [
        ("static", "", "1 AS unused_id, 'new' AS unused_msg"),
        ("dynamic", "", "1 AS unused_id, 'new' AS unused_msg"),
        ("dynamic", "BY NAME", "'new' AS msg, 1 AS id"),
        ("dynamic", "(msg, id)", "'new', 1"),
    ],
    ids=["static-position", "dynamic-position", "name", "columns"],
)
def test_insert_dynamic_partition_clause_order_and_mode(spark, partition_table, mode, columns, projection):
    table = partition_table()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", mode)
    spark.sql(f"INSERT INTO {table} VALUES (1, 'old'), (2, 'keep')")
    spark.sql(f"INSERT OVERWRITE {table} PARTITION (ID) {columns} SELECT {projection}")
    assert spark.table(table).orderBy("id").collect() == (
        [(1, "new"), (2, "keep")] if mode == "dynamic" else [(1, "new")]
    )


@pytest.mark.parametrize("clause", ["part, id", "id"])
def test_insert_dynamic_partition_clause_composite_and_subset(spark, partition_table, clause):
    table = partition_table("(id INT, msg STRING, part STRING) USING delta PARTITIONED BY (id, part)")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'old', 'a'), (1, 'keep-a', 'b'), (2, 'keep-b', 'a')")
    spark.sql(f"INSERT OVERWRITE {table} PARTITION ({clause}) SELECT 1, 'new', 'a'")
    assert spark.table(table).orderBy("msg").collect() == [(1, "keep-a", "b"), (2, "keep-b", "a"), (1, "new", "a")]


def test_insert_dynamic_partition_column_omission_is_not_unresolved_input(spark, tmp_path, partition_table):
    table = partition_table()
    spark.sql(f"INSERT INTO {table} VALUES (1, 'old'), (2, 'keep')")
    schema = spark.table(table).schema
    with _reject_unchanged(spark, table, tmp_path, "msg"):
        spark.sql(f"INSERT OVERWRITE {table} PARTITION (id) (msg) SELECT msg FROM (SELECT 1 AS id)")
    spark.sql(f"INSERT OVERWRITE {table} PARTITION (id) (id) SELECT 1")
    assert spark.table(table).orderBy("id").collect() == [(1, None), (2, "keep")]
    assert spark.table(table).schema == schema
    spark.sql(f"INSERT INTO {table} PARTITION (id) SELECT 1, 'appended'")
    assert spark.table(table).orderBy("id", "msg").collect() == [(1, None), (1, "appended"), (2, "keep")]


@pytest.mark.parametrize(
    ("clause", "partition_by", "error"),
    [
        ("unknown", "id, part", "non-partition column: unknown"),
        ("msg", "id, part", "non-partition column: msg"),
        ("id, ID", "id, part", "duplicate partition column"),
        ("id = 1", "id, part", "static or mixed PARTITION"),
        ("id, part = 'a'", "id, part", "static or mixed PARTITION"),
        ("id", "", "non-partition column: id"),
    ],
)
def test_insert_partition_clause_failure_preserves_state(spark, tmp_path, partition_table, clause, partition_by, error):
    partitioning = f"PARTITIONED BY ({partition_by})" if partition_by else ""
    table = partition_table(f"(id INT, msg STRING, part STRING) USING delta {partitioning}")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'old', 'a'), (2, 'keep', 'b')")
    with _reject_unchanged(spark, table, tmp_path, error):
        spark.sql(f"INSERT OVERWRITE {table} PARTITION ({clause}) SELECT 3, 'new', 'c'")


def test_insert_partition_clause_keeps_non_delta_unsupported(spark, tmp_path, partition_table):
    # Parquet represents the non-Delta provider guard.
    table = partition_table("(id INT, msg STRING) USING parquet PARTITIONED BY (id)")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'old')")
    with _reject_unchanged(spark, table, tmp_path, "PARTITION for write"):
        spark.sql(f"INSERT OVERWRITE {table} PARTITION (id) SELECT 2, 'new'")


@pytest.mark.parametrize("global_view", [False, True], ids=["local-query", "global-using"])
def test_insert_partition_clause_rejects_temporary_views(spark, tmp_path, partition_table, global_view):
    # Cover local shadowing and global qualification; both definitions use the temporary-view guard.
    table = partition_table()
    spark.sql(f"INSERT INTO {table} VALUES (1, 'old')")
    definition = (
        f"USING delta OPTIONS (path '{escape_sql_string_literal(str(tmp_path))}')"
        if global_view
        else "AS SELECT 0 AS id, 'view' AS msg"
    )
    spark.sql(f"CREATE {'GLOBAL ' if global_view else ''}TEMPORARY VIEW {table} {definition}")
    try:
        target = f"global_temp.{table}" if global_view else table
        with _reject_unchanged(spark, target, tmp_path, "PARTITION for writes to temporary views"):
            spark.sql(f"INSERT OVERWRITE {target} PARTITION (id) SELECT 2, 'new'")
        assert spark.read.format("delta").load(str(tmp_path)).collect() == [(1, "old")]
    finally:
        if global_view:
            spark.catalog.dropGlobalTempView(table)
        else:
            spark.catalog.dropTempView(table)


def test_insert_partition_clause_uses_registered_delta_metadata(spark, tmp_path, partition_table):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
    spark.createDataFrame([("old", 1)], "msg STRING, `Part.Id` INT").write.format("delta").partitionBy("Part.Id").save(
        str(tmp_path)
    )
    table = partition_table("USING delta")
    spark.sql(f"INSERT OVERWRITE {table} PARTITION (`Part.Id`) SELECT 'new', 2")
    assert spark.table(table).select("`Part.Id`", "msg").collect() == [(2, "new")]


def test_insert_partition_clause_uses_canonical_ctas_metadata(spark, tmp_path, partition_table):
    table = partition_table("USING delta PARTITIONED BY (ID)", "AS SELECT 1 AS id, 'old' AS msg")
    spark.conf.set("spark.sql.caseSensitive", "true")
    with _reject_unchanged(spark, table, tmp_path, "non-partition column: ID"):
        spark.sql(f"INSERT OVERWRITE {table} PARTITION (ID) SELECT 1, 'bad'")
    spark.sql(f"INSERT OVERWRITE {table} PARTITION (id) SELECT 1, 'new'")
    assert spark.table(table).select("id", "msg").collect() == [(1, "new")]


def test_insert_partition_clause_unicode_resolution(spark, tmp_path, partition_table):
    table = partition_table("(`É` INT, msg STRING) USING delta PARTITIONED BY (`É`)")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'old')")
    spark.sql(f"INSERT OVERWRITE {table} PARTITION (`é`) SELECT 1, 'new'")
    assert spark.table(table).select("É", "msg").collect() == [(1, "new")]
    with _reject_unchanged(spark, table, tmp_path, "duplicate partition column"):
        spark.sql(f"INSERT OVERWRITE {table} PARTITION (`É`, `é`) SELECT 1, 'bad'")
    spark.conf.set("spark.sql.caseSensitive", "true")
    with _reject_unchanged(spark, table, tmp_path, "non-partition column: é"):
        spark.sql(f"INSERT OVERWRITE {table} PARTITION (`é`) SELECT 1, 'bad'")


@pytest.mark.parametrize("operation", ["INSERT INTO", "INSERT OVERWRITE"])
def test_insert_partition_clause_requires_existing_table(spark, operation):
    table = "missing_partition_clause"
    assert not spark.catalog.tableExists(table)
    with pytest.raises(Exception, match="TABLE_OR_VIEW_NOT_FOUND"):
        spark.sql(f"{operation} {table} PARTITION (id) SELECT 1 AS id, 'new' AS msg")
    assert not spark.catalog.tableExists(table)


@pytest.mark.parametrize("mapped", [False, True])
@pytest.mark.parametrize("api", ["option", "v2"])
def test_dynamic_overwrite_replaces_only_written_partitions(spark, tmp_path, mapped, api):
    table = "dynamic_delta"
    properties = "'delta.columnMapping.mode'='name'," if mapped else ""
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, part STRING) USING delta PARTITIONED BY (part) "
            f"LOCATION '{tmp_path.as_uri()}' TBLPROPERTIES ({properties}'delta.enableChangeDataFeed'='true')"
        )
        spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, NULL), (4, 'a')")
        replacement = spark.createDataFrame([(10, "a"), (30, None)], "id INT, part STRING")
        if api == "option":
            replacement.write.format("delta").mode("overwrite").option("partitionOverwriteMode", "dynamic").save(
                str(tmp_path)
            )
        else:
            replacement.writeTo(table).overwritePartitions()
        assert [tuple(r) for r in spark.table(table).orderBy("id").collect()] == [(2, "b"), (10, "a"), (30, None)]
        logs = sorted((tmp_path / "_delta_log").glob("*.json"))
        actions = [json.loads(line) for line in logs[-1].read_text().splitlines()]
        info = next(a["commitInfo"] for a in actions if "commitInfo" in a)
        assert info["operation"] == "WRITE"
        assert info["operationParameters"]["mode"] == "Overwrite"
        changes = (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", int(logs[-1].stem))
            .load(str(tmp_path))
        )
        assert sorted((r.id, r["_change_type"]) for r in changes.collect()) == [
            (1, "delete"),
            (3, "delete"),
            (4, "delete"),
            (10, "insert"),
            (30, "insert"),
        ]
        spark.createDataFrame([], "id INT, part STRING").writeTo(table).overwritePartitions()
        assert [r.id for r in spark.table(table).orderBy("id").collect()] == [2, 10, 30]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_dynamic_overwrite_rejects_schema_replacement_and_append_only(spark, tmp_path):
    spark.createDataFrame([(1, "a")], "id INT, part STRING").write.format("delta").option(
        "delta.appendOnly", "true"
    ).partitionBy("part").save(str(tmp_path))
    writer = spark.createDataFrame([(2, "a")], "id INT, part STRING").write.format("delta").mode("overwrite")
    with pytest.raises(Exception, match="overwriteSchema"):
        writer.option("partitionOverwriteMode", "dynamic").option("overwriteSchema", "true").save(str(tmp_path))
    with pytest.raises(Exception, match=r"[Aa]ppend.only|appendOnly"):
        spark.createDataFrame([(2, "a")], "id INT, part STRING").write.format("delta").mode("overwrite").option(
            "partitionOverwriteMode", "dynamic"
        ).save(str(tmp_path))
    assert [r.id for r in spark.read.format("delta").load(str(tmp_path)).collect()] == [1]


@pytest.mark.parametrize("fmt", ["delta", "iceberg"])
def test_session_dynamic_overwrite_and_explicit_static_override(spark, tmp_path, fmt):
    table = "session_dynamic"
    key = "spark.sql.sources.partitionOverwriteMode"
    previous = spark.conf.get(key)
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, part STRING) USING {fmt} PARTITIONED BY (part) LOCATION '{tmp_path.as_uri()}'"
        )
        spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")
        spark.conf.set(key, "dynamic")
        spark.sql(f"INSERT OVERWRITE {table} VALUES (3, 'a')")
        assert [r.id for r in spark.table(table).orderBy("id").collect()] == [2, 3]
        option = "partitionOverwriteMode" if fmt == "delta" else "overwrite-mode"
        spark.createDataFrame([(4, "a")], "id INT, part STRING").write.format(fmt).mode("overwrite").option(
            option, "static"
        ).save(str(tmp_path))
        assert [r.id for r in spark.table(table).collect()] == [4]
    finally:
        spark.conf.set(key, previous)
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_dynamic_mode_on_unpartitioned_table_allows_schema_replacement(spark, tmp_path):
    spark.createDataFrame([(1,)], "id INT").write.format("delta").save(str(tmp_path))
    spark.createDataFrame([(2, "new")], "id INT, value STRING").write.format("delta").mode("overwrite").option(
        "partitionOverwriteMode", "dynamic"
    ).option("overwriteSchema", "true").save(str(tmp_path))
    assert [tuple(r) for r in spark.read.format("delta").load(str(tmp_path)).collect()] == [(2, "new")]


@pytest.mark.parametrize(
    ("partition_type", "first", "second"),
    [
        ("DOUBLE", "1.0", "2.0"),
        ("FLOAT", "1.0", "2.0"),
        ("TIMESTAMP", "TIMESTAMP '2024-01-01 01:02:03.000100'", "TIMESTAMP '2024-01-02 00:00:00'"),
        ("TIMESTAMP_NTZ", "TIMESTAMP_NTZ '2024-01-01 01:02:03.000100'", "TIMESTAMP_NTZ '2024-01-02 00:00:00'"),
    ],
)
@pytest.mark.parametrize("mapping", ["none", "name"])
@pytest.mark.integration
def test_dynamic_overwrite_matches_spark_partition_values(
    spark, delta_jvm_spark, tmp_path, partition_type, first, second, mapping
):
    delta_jvm_spark.sql(
        f"SELECT 1 AS id, CAST({first} AS {partition_type}) AS part "
        f"UNION ALL SELECT 2, CAST({second} AS {partition_type})"
    ).coalesce(1).write.format("delta").partitionBy("part").option("delta.columnMapping.mode", mapping).save(
        str(tmp_path)
    )
    assert [r.id for r in spark.read.format("delta").load(str(tmp_path)).orderBy("id").collect()] == [1, 2]

    spark.sql(f"SELECT 3 AS id, CAST({first} AS {partition_type}) AS part").write.format("delta").mode(
        "overwrite"
    ).option("partitionOverwriteMode", "dynamic").save(str(tmp_path))

    assert [r.id for r in spark.read.format("delta").load(str(tmp_path)).orderBy("id").collect()] == [2, 3]
    assert [r.id for r in delta_jvm_spark.sql(f"SELECT id FROM delta.`{tmp_path}` ORDER BY id").collect()] == [2, 3]


@pytest.mark.parametrize(
    ("initial_type", "updated_type", "new_value"),
    [("INT", "BIGINT", 3000000000), ("FLOAT", "DOUBLE", 1.00000001)],
)
def test_dynamic_overwrite_uses_widened_partition_type(spark, tmp_path, initial_type, updated_type, new_value):
    initial_value = 1 if initial_type == "INT" else 1.0
    spark.createDataFrame([(1, initial_value)], f"id INT, part {initial_type}").write.format("delta").partitionBy(
        "part"
    ).option("delta.enableTypeWidening", "true").save(str(tmp_path))
    spark.createDataFrame([(2, new_value)], f"id INT, part {updated_type}").write.format("delta").mode(
        "overwrite"
    ).option("partitionOverwriteMode", "dynamic").option("mergeSchema", "true").save(str(tmp_path))

    result = spark.read.format("delta").load(str(tmp_path))
    assert result.schema["part"].dataType.simpleString() == updated_type.lower()
    assert [r.id for r in result.orderBy("id").collect()] == [1, 2]


@pytest.mark.parametrize("partition_type", ["FLOAT", "DOUBLE"])
@pytest.mark.parametrize(("initial_zero", "replacement_zero"), [("-0.0", "0.0"), ("0.0", "-0.0")])
@pytest.mark.parametrize("mapping", ["none", "name"])
def test_dynamic_overwrite_matches_signed_zero_partitions(
    spark, tmp_path, partition_type, initial_zero, replacement_zero, mapping
):
    spark.sql(
        f"SELECT 1 AS id, CAST('{initial_zero}' AS {partition_type}) AS part "
        f"UNION ALL SELECT 2, CAST(1 AS {partition_type})"
    ).write.format("delta").partitionBy("part").option("delta.columnMapping.mode", mapping).save(str(tmp_path))
    logs = sorted((tmp_path / "_delta_log").glob("*.json"))
    initial_adds = [json.loads(line)["add"] for line in logs[0].read_text().splitlines() if "add" in json.loads(line)]
    zero_paths = {add["path"] for add in initial_adds if float(next(iter(add["partitionValues"].values()))) == 0.0}

    spark.sql(f"SELECT 3 AS id, CAST('{replacement_zero}' AS {partition_type}) AS part").write.format("delta").mode(
        "overwrite"
    ).option("partitionOverwriteMode", "dynamic").save(str(tmp_path))

    result = spark.read.format("delta").load(str(tmp_path))
    assert [r.id for r in result.orderBy("id").collect()] == [2, 3]
    latest_log = sorted((tmp_path / "_delta_log").glob("*.json"))[-1]
    removes = [
        json.loads(line)["remove"] for line in latest_log.read_text().splitlines() if "remove" in json.loads(line)
    ]
    assert {remove["path"] for remove in removes} == zero_paths
