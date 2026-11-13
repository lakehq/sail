from __future__ import annotations

import json
from contextlib import nullcontext

import pytest
from pyspark.sql import Row

from pysail.testing._dev.spark.utils.jvm import classic_spark_mode
from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests._dev.spark.delta.utils import (
    _TABLE_NOT_VIEW,
    _active_files,
    _assert_unchanged,
    _effective,
    _settings,
    _tree,
)

_NATIVE_REJECTION = (
    r"(?i)(not implemented|not supported|unsupported|FULL mode only|parse error|parser error|syntax error"
    r"|^invalid argument: found (?:RESTORE|REORG|GENERATE|FEATURE|SHALLOW|CONVERT|USING) "
    r"at \d+:\d+ expected [^\n]+$)"
)


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/optimize/OptimizeCompactionSuite.scala#L722-L789
@pytest.mark.parametrize(
    ("suffix", "error"),
    [
        pytest.param("WHERE id = 1", "partition", id="data-column"),
        pytest.param("WHERE unknown = 1", "(?i)(unknown|not found|resolve)", id="unknown-column"),
        pytest.param("WHERE 1 + 1", "(?i)(boolean|predicate|type)", id="non-boolean"),
        pytest.param("WHERE part = (SELECT 'a')", "(?i)subquery", id="scalar-subquery"),
        pytest.param("WHERE part IN (SELECT 'a')", "(?i)subquery", id="in-subquery"),
        pytest.param("WHERE", "(?i)(parse|syntax|expected)", id="missing-predicate"),
    ],
)
def test_optimize_invalid_predicate_preserves_table(spark, semantic_table, suffix, error):
    name, path = semantic_table
    for _ in range(2):
        spark.sql(f"INSERT INTO {name} VALUES (1, 'a'), (2, NULL)").collect()  # noqa: S608
    rows = spark.table(name).orderBy("id").collect()
    schema = spark.table(name).schema
    before = _tree(path)
    with pytest.raises(Exception, match=error):
        spark.sql(f"OPTIMIZE {name} {suffix}").collect()
    assert _tree(path) == before
    assert spark.table(name).schema == schema
    assert spark.table(name).orderBy("id").collect() == rows


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/DeltaVacuumSuite.scala#L1022-L1038
@pytest.mark.parametrize("kind", ["missing", "empty", "parquet", "partition"])
@pytest.mark.parametrize("dry_run", [False, True], ids=["execute", "dry-run"])
def test_vacuum_invalid_target_preserves_files(spark, semantic_table, tmp_path, kind, dry_run):
    name, path = semantic_table
    spark.sql(f"INSERT INTO {name} VALUES (1, 'a')").collect()  # noqa: S608
    target = tmp_path / kind
    if kind == "empty":
        target.mkdir()
    elif kind == "parquet":
        spark.range(3).coalesce(1).write.parquet(str(target))
    elif kind == "partition":
        target = path / "part=a"
        assert target.is_dir()
    before = _tree(tmp_path)
    exists = target.exists()
    with _settings(spark, **{"spark.databricks.delta.retentionDurationCheck.enabled": "false"}):
        with pytest.raises(Exception, match=r"(?i)(not.*delta|delta.*not|no.*log|not found|does not exist)"):
            spark.sql(
                f"VACUUM '{escape_sql_string_literal(str(target))}' RETAIN 0 HOURS {'DRY RUN' if dry_run else ''}"
            ).collect()
    assert _tree(tmp_path) == before
    assert target.exists() == exists
    assert spark.table(name).collect() == [Row(id=1, part="a")]
    if kind == "parquet":
        assert spark.read.parquet(str(target)).orderBy("id").collect() == [Row(id=i) for i in range(3)]


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/DeltaVacuumSuite.scala#L939-L1018
@pytest.mark.parametrize(
    ("clause", "error"),
    [
        pytest.param("RETAIN -1 HOURS", r"^invalid argument: found - at \d+:\d+ expected number$", id="negative"),
        pytest.param("RETAIN 0 HOURS", "(?i)(retention|retain)", id="zero-guard"),
        pytest.param("RETAIN 1 HOURS", "(?i)(retention|retain)", id="short-guard"),
        pytest.param("RETAIN 168 HOURS RETAIN 200 HOURS", "(?i)duplicate", id="duplicate-retain"),
        pytest.param("DRY RUN DRY RUN", "(?i)duplicate", id="duplicate-dry-run"),
        pytest.param("FULL LITE", "(?i)duplicate", id="duplicate-mode"),
    ],
)
def test_vacuum_invalid_clauses_preserve_table(spark, semantic_table, clause, error):
    name, path = semantic_table
    spark.sql(f"INSERT INTO {name} VALUES (1, 'a')").collect()  # noqa: S608
    (path / "orphan.txt").write_bytes(b"must not delete")
    before = _tree(path)
    with _settings(
        spark,
        **{
            "spark.databricks.delta.retentionDurationCheck.enabled": "true",
            "spark.databricks.delta.vacuum.retentionWindowIgnore.enabled": "false",
        },
    ), pytest.raises(Exception, match=error):
        spark.sql(f"VACUUM {name} {clause}").collect()
    assert _tree(path) == before
    assert spark.table(name).collect() == [Row(id=1, part="a")]


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/DescribeDeltaDetailSuite.scala#L195-L213
@pytest.mark.parametrize("command", ["DESCRIBE DETAIL", "DESCRIBE HISTORY", "VACUUM", "OPTIMIZE"])
@pytest.mark.parametrize("temporary", [False, True], ids=["permanent", "temporary"])
def test_maintenance_rejects_views(spark, semantic_table, command, temporary):
    name, path = semantic_table
    spark.sql(f"INSERT INTO {name} VALUES (1, 'a')").collect()  # noqa: S608
    view = "maintenance_view"
    spark.sql(f"CREATE {'TEMPORARY ' if temporary else ''}VIEW {view} AS SELECT * FROM {name}").collect()
    try:
        before = _tree(path)
        with pytest.raises(Exception, match=_TABLE_NOT_VIEW):
            spark.sql(f"{command} {view}").collect()
        assert _tree(path) == before
        assert spark.table(view).collect() == [Row(id=1, part="a")]
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {view}").collect()


@pytest.mark.parametrize(
    ("limit", "error"),
    [
        pytest.param("-1", r"(?i)(limit|integer|range|parse|syntax)", id="-1"),
        pytest.param("1.5", r"^invalid argument: found \. at \d+:\d+ expected ';', or end of input$", id="1.5"),
        pytest.param("NULL", r"(?i)(limit|integer|range|parse|syntax)", id="NULL"),
        pytest.param("'1'", r"(?i)(limit|integer|range|parse|syntax)", id="'1'"),
        pytest.param(
            "2147483648",
            r"Cast error: Cannot cast string '2147483648' to value of Int32 type",
            id="2147483648",
        ),
    ],
)
def test_history_invalid_limits_preserve_table(spark, semantic_table, limit, error):
    name, path = semantic_table
    before = _tree(path)
    with pytest.raises(Exception, match=error):
        spark.sql(f"DESCRIBE HISTORY {name} LIMIT {limit}").collect()
    assert _tree(path) == before


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/RestoreTableSQLSuite.scala#L23-L79
# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/optimize/DeltaReorgSuite.scala#L42-L117
# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/DeltaGenerateSymlinkManifestSuite.scala#L54-L139
# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/DeltaProtocolVersionSuite.scala#L3134-L3190
@pytest.mark.parametrize(
    "command",
    [
        pytest.param("RESTORE TABLE {target} TO VERSION AS OF 0", id="restore-version"),
        pytest.param("RESTORE {target} TO TIMESTAMP AS OF '2020-01-01 00:00:00'", id="restore-time"),
        pytest.param("REORG TABLE {target} APPLY (PURGE)", id="reorg"),
        pytest.param("REORG TABLE {target} WHERE part = 'a' APPLY (PURGE)", id="reorg-partition"),
        pytest.param("GENERATE symlink_format_manifest FOR TABLE {target}", id="generate"),
        pytest.param("GENERATE invalid_mode FOR TABLE {target}", id="generate-invalid-mode"),
        pytest.param("ALTER TABLE {target} DROP FEATURE deletionVectors", id="drop-feature"),
        pytest.param("ALTER TABLE {target} DROP FEATURE deletionVectors TRUNCATE HISTORY", id="drop-feature-history"),
        pytest.param("OPTIMIZE {target} FULL", id="optimize-full"),
        pytest.param("OPTIMIZE {target} ZORDER BY (id)", id="zorder"),
        pytest.param("OPTIMIZE {target} ZORDER BY (part)", id="zorder-partition"),
        pytest.param("OPTIMIZE {target} ZORDER BY (missing)", id="zorder-missing"),
        pytest.param("VACUUM {target} LITE", id="vacuum-lite"),
        pytest.param("VACUUM {target} USING INVENTORY {target}", id="vacuum-inventory"),
    ],
)
@pytest.mark.parametrize("target_kind", ["name", "path"])
def test_unsupported_native_maintenance_preserves_state(spark, semantic_table, command, target_kind):
    name, path = semantic_table
    spark.sql(f"INSERT INTO {name} VALUES (1, 'a'), (2, 'b')").collect()  # noqa: S608
    spark.sql(f"INSERT INTO {name} VALUES (3, 'a')").collect()  # noqa: S608
    target = name if target_kind == "name" else f"delta.`{path}`"
    rows = spark.table(name).orderBy("id").collect()
    schema = spark.table(name).schema
    before = _tree(path)
    with pytest.raises(Exception, match=_NATIVE_REJECTION):
        spark.sql(command.format(target=target)).collect()
    assert _tree(path) == before
    assert spark.table(name).schema == schema
    assert spark.table(name).orderBy("id").collect() == rows
    assert spark.read.format("delta").option("versionAsOf", 1).load(str(path)).orderBy("id").collect() == rows[:2]


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/optimize/DeltaReorgSuite.scala#L75-L117
@pytest.mark.integration
@pytest.mark.parametrize("mapping", ["none", "name"])
@pytest.mark.parametrize(
    "command",
    [
        pytest.param("REORG TABLE {target} APPLY (PURGE)", id="reorg"),
        pytest.param("RESTORE TABLE {target} TO VERSION AS OF 0", id="restore"),
        pytest.param("ALTER TABLE {target} DROP FEATURE deletionVectors TRUNCATE HISTORY", id="drop-feature"),
        pytest.param("GENERATE symlink_format_manifest FOR TABLE {target}", id="generate"),
    ],
)
def test_unsupported_maintenance_preserves_external_dvs(spark, delta_jvm_spark, tmp_path, mapping, command):
    path = tmp_path / "external"
    target = f"delta.`{path}`"
    delta_jvm_spark.sql(
        f"CREATE TABLE {target} (id BIGINT, part STRING) USING delta PARTITIONED BY (part) "
        f"TBLPROPERTIES ('delta.columnMapping.mode' = '{mapping}', 'delta.enableDeletionVectors' = 'true')"
    ).collect()
    delta_jvm_spark.sql(f"INSERT INTO {target} VALUES (1, 'a'), (2, 'a'), (3, 'a')").collect()  # noqa: S608
    delta_jvm_spark.sql(f"DELETE FROM {target} WHERE id = 1").collect()  # noqa: S608
    assert any(file.deletionVector is not None for file in _active_files(path))
    before = _tree(path)
    schema = delta_jvm_spark.read.format("delta").load(str(path)).schema
    with pytest.raises(Exception, match=_NATIVE_REJECTION):
        spark.sql(command.format(target=target)).collect()
    assert _tree(path) == before
    for session in [spark, delta_jvm_spark]:
        with classic_spark_mode() if session is delta_jvm_spark else nullcontext():
            result = session.read.format("delta").load(str(path))
            assert result.schema == schema
            assert result.orderBy("id").collect() == [Row(id=2, part="a"), Row(id=3, part="a")]


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/CloneTableSQLSuite.scala#L43-L132
@pytest.mark.parametrize("destination_kind", ["missing", "delta", "parquet", "self"])
@pytest.mark.parametrize(
    "template",
    [
        pytest.param("CREATE TABLE {dest} SHALLOW CLONE {source}", id="create"),
        pytest.param("CREATE OR REPLACE TABLE {dest} SHALLOW CLONE {source}", id="replace"),
        pytest.param("CREATE TABLE IF NOT EXISTS {dest} SHALLOW CLONE {source}", id="if-not-exists"),
        pytest.param("CREATE TABLE {dest} SHALLOW CLONE {source} VERSION AS OF 1", id="version"),
        pytest.param("CREATE TABLE {dest} SHALLOW CLONE {source} TIMESTAMP AS OF '2020-01-01'", id="timestamp"),
    ],
)
def test_unsupported_clone_protects_both_tables(spark, semantic_table, tmp_path, destination_kind, template):
    name, source = semantic_table
    spark.sql(f"INSERT INTO {name} VALUES (1, 'a')").collect()  # noqa: S608
    dest = tmp_path / "destination"
    if destination_kind in ("delta", "parquet"):
        spark.range(10, 12).coalesce(1).write.format(destination_kind).save(str(dest))
    elif destination_kind == "self":
        dest = source
    before = _tree(tmp_path)
    exists = dest.exists()
    with pytest.raises(Exception, match=_NATIVE_REJECTION):
        spark.sql(template.format(dest=f"delta.`{dest}`", source=name)).collect()
    assert _tree(tmp_path) == before
    assert dest.exists() == exists
    assert spark.table(name).collect() == [Row(id=1, part="a")]
    if destination_kind in ("delta", "parquet"):
        assert spark.read.format(destination_kind).load(str(dest)).orderBy("id").collect() == [Row(id=10), Row(id=11)]


@pytest.mark.parametrize(
    "command",
    [
        pytest.param("RESTORE TABLE {target} TO VERSION AS OF 0", id="restore"),
        pytest.param("REORG TABLE {target} APPLY (PURGE)", id="reorg"),
        pytest.param("GENERATE symlink_format_manifest FOR TABLE {target}", id="generate"),
        pytest.param("ALTER TABLE {target} DROP FEATURE deletionVectors", id="drop-feature"),
        pytest.param("CREATE TABLE {dest} SHALLOW CLONE {target}", id="clone"),
        pytest.param("CONVERT TO DELTA parquet.`{missing}`", id="convert"),
    ],
)
def test_unsupported_native_missing_target_does_not_create_state(spark, semantic_table, tmp_path, command):
    name, _path = semantic_table
    spark.sql(f"INSERT INTO {name} VALUES (1, 'a')").collect()  # noqa: S608
    missing = tmp_path / "missing"
    dest = tmp_path / "destination"
    before = _tree(tmp_path)
    with pytest.raises(Exception, match=_NATIVE_REJECTION):
        spark.sql(command.format(target=f"delta.`{missing}`", dest=f"delta.`{dest}`", missing=missing)).collect()
    assert _tree(tmp_path) == before
    assert not missing.exists()
    assert not dest.exists()
    assert spark.table(name).collect() == [Row(id=1, part="a")]


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/DescribeDeltaDetailSuite.scala#L98-L140
@pytest.mark.parametrize("command", ["DESCRIBE DETAIL", "DESCRIBE HISTORY"])
@pytest.mark.parametrize("kind", ["missing", "parquet"])
def test_delta_inspection_invalid_target_protects_source(spark, tmp_path, command, kind):
    path = tmp_path / kind
    if kind == "parquet":
        spark.range(3).coalesce(1).write.parquet(str(path))
    before = _tree(tmp_path)
    exists = path.exists()
    with pytest.raises(Exception, match=r"(?i)(not.*delta|delta.*not|not supported|no.*log|not found|does not exist)"):
        spark.sql(f"{command} delta.`{path}`").collect()
    assert _tree(tmp_path) == before
    assert path.exists() == exists
    if kind == "parquet":
        assert spark.read.parquet(str(path)).orderBy("id").collect() == [Row(id=i) for i in range(3)]


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/ConvertToDeltaSQLSuite.scala#L25-L59
@pytest.mark.parametrize("partitioned", [False, True])
@pytest.mark.parametrize("statistics", ["", "NO STATISTICS"], ids=["statistics", "no-statistics"])
def test_unsupported_convert_preserves_parquet(spark, tmp_path, partitioned, statistics):
    path = tmp_path / "parquet"
    data = spark.createDataFrame([(1, "a"), (2, None)], "id LONG, part STRING")
    writer = data.coalesce(1).write
    if partitioned:
        writer = writer.partitionBy("part")
    writer.parquet(str(path))
    original = spark.read.parquet(str(path))
    rows, schema = original.orderBy("id").collect(), original.schema
    before = _tree(path)
    with pytest.raises(Exception, match=_NATIVE_REJECTION):
        spark.sql(
            f"CONVERT TO DELTA parquet.`{path}` {statistics} {'PARTITIONED BY (part STRING)' if partitioned else ''}"
        ).collect()
    assert _tree(path) == before
    assert not (path / "_delta_log").exists()
    preserved = spark.read.parquet(str(path))
    assert preserved.schema == schema
    assert preserved.orderBy("id").collect() == rows


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/IdentityColumnSyncSuite.scala#L58-L82
@pytest.mark.parametrize("keyword", ["ALTER", "CHANGE"])
@pytest.mark.parametrize("step", [-3, 3])
@pytest.mark.parametrize("start", [-1, 1])
@pytest.mark.parametrize("populated", [False, True], ids=["empty", "watermark"])
def test_sync_identity_rejection_preserves_high_watermark(spark, tmp_path, keyword, step, start, populated):
    path = tmp_path / "identity"
    name = "maintenance_identity"
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH {start} INCREMENT BY {step}), "
        f"value STRING) USING delta LOCATION '{escape_sql_string_literal(str(path))}'"
    ).collect()
    try:
        if populated:
            spark.sql(f"INSERT INTO {name} (value) VALUES ('generated')").collect()  # noqa: S608
            spark.sql(f"INSERT INTO {name} VALUES ({start + step * 10}, 'explicit')").collect()  # noqa: S608
        metadata = _effective(path, "metaData")
        field = next(field for field in json.loads(metadata["schemaString"])["fields"] if field["name"] == "id")
        watermark = field["metadata"].get("delta.identity.highWaterMark")
        assert (watermark is not None) == populated
        rows, schema, before = spark.table(name).orderBy("id").collect(), spark.table(name).schema, _tree(path)
        with pytest.raises(Exception, match=r"(?i)^invalid argument: found SYNC at \d+:\d+ expected "):
            spark.sql(f"ALTER TABLE {name} {keyword} COLUMN id SYNC IDENTITY").collect()
        _assert_unchanged(spark, name, path, before, rows, schema)
        assert _effective(path, "metaData") == metadata
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


# [CREDIT]: https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala#L1180-L1190
@pytest.mark.parametrize(
    "command",
    [
        pytest.param("DESCRIBE DETAIL {target}", id="detail"),
        pytest.param("DESCRIBE HISTORY {target}", id="history"),
        pytest.param("OPTIMIZE {target}", id="optimize"),
        pytest.param("VACUUM {target}", id="vacuum"),
        pytest.param(
            "ALTER TABLE {target} SET TBLPROPERTIES ('shadowing.marker' = 'changed')",
            id="set-properties",
        ),
        pytest.param(
            "ALTER TABLE {target} UNSET TBLPROPERTIES ('shadowing.marker')",
            id="unset-properties",
        ),
    ],
)
def test_maintenance_rejects_temporary_view_shadowing_table(spark, semantic_table, command):
    name, path = semantic_table
    spark.sql(f"INSERT INTO {name} VALUES (1, 'a'), (2, 'b')").collect()  # noqa: S608
    spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('shadowing.marker' = 'original')").collect()
    rows = spark.table(name).orderBy("id").collect()
    schema = spark.table(name).schema
    properties = spark.sql(f"SHOW TBLPROPERTIES {name}").orderBy("key").collect()
    before = _tree(path)
    spark.sql(f"CREATE TEMPORARY VIEW {name} AS SELECT CAST(99 AS BIGINT) AS id, 'view' AS part").collect()
    try:
        assert spark.table(name).collect() == [Row(id=99, part="view")]
        with pytest.raises(Exception, match=_TABLE_NOT_VIEW):
            spark.sql(command.format(target=name)).collect()
        assert spark.table(name).collect() == [Row(id=99, part="view")]
    finally:
        spark.catalog.dropTempView(name)
    assert spark.table(name).orderBy("id").collect() == rows
    assert spark.table(name).schema == schema
    assert spark.sql(f"SHOW TBLPROPERTIES {name}").orderBy("key").collect() == properties
    assert _tree(path) == before
