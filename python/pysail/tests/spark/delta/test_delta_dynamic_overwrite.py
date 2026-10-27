# ruff: noqa: S608
import json

import pytest

from pysail.testing.spark.utils.sql import escape_sql_string_literal


@pytest.mark.parametrize("mapped", [False, True])
@pytest.mark.parametrize("api", ["option", "v2"])
def test_dynamic_overwrite_replaces_only_written_partitions(spark, tmp_path, mapped, api):
    table = "dynamic_delta"
    properties = "'delta.columnMapping.mode'='name'," if mapped else ""
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, part STRING) USING delta PARTITIONED BY (part) "
            f"LOCATION '{escape_sql_string_literal(str(tmp_path))}' TBLPROPERTIES ({properties}'delta.enableChangeDataFeed'='true')"
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
            f"CREATE TABLE {table} (id INT, part STRING) USING {fmt} PARTITIONED BY (part) LOCATION '{escape_sql_string_literal(str(tmp_path))}'"
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
