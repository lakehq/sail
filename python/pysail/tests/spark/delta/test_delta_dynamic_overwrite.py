import json

import pytest


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
        assert sorted((r.id, r._change_type) for r in changes.collect()) == [
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
    with pytest.raises(Exception, match="[Aa]ppend.only|appendOnly"):
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
