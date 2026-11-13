from __future__ import annotations

import re

import pytest
from pyspark.sql import Row

from pysail.testing._dev.spark.utils.jvm import classic_spark_mode
from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests._dev.spark.delta.utils import (
    _TABLE_NOT_VIEW,
    _active_files,
    _assert_unchanged,
    _commits,
    _effective,
    _settings,
    _tree,
)


@pytest.fixture
def lifecycle_table(spark, tmp_path):
    name = "maintenance_lifecycle"
    path = tmp_path / "table"
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT) USING delta LOCATION '{escape_sql_string_literal(str(path))}'"
    ).collect()
    try:
        yield name, path
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/DeltaAlterTableTests.scala#L105-L223
@pytest.mark.parametrize("mixed_case", [False, True], ids=["canonical", "mixed-case"])
def test_property_canonicalization_and_unset_restores_checkpoint_default(spark, lifecycle_table, mixed_case):
    name, path = lifecycle_table
    retention = "dEltA.lOgrEteNtiOndURaTion" if mixed_case else "delta.logRetentionDuration"
    interval = "DelTa.ChEckPoiNtinTervAl" if mixed_case else "delta.checkpointInterval"
    baseline = _effective(path, "metaData")["configuration"]
    for weeks, checkpoint_interval, key, value in [(1, 5, "key", "value1"), (2, 20, "kEy", "value2")]:
        spark.sql(
            f"ALTER TABLE {name} SET TBLPROPERTIES "
            f"('{retention}' = '{weeks} weeks', '{interval}' = '{checkpoint_interval}', '{key}' = '{value}')"
        ).collect()
        expected = {
            **baseline,
            "delta.logRetentionDuration": f"{weeks} weeks",
            "delta.checkpointInterval": str(checkpoint_interval),
            "key": "value1",
        }
        if weeks == 2:
            expected["kEy"] = "value2"
        assert _effective(path, "metaData")["configuration"] == expected
    spark.sql(f"ALTER TABLE {name} UNSET TBLPROPERTIES ('{interval}', 'kEy')").collect()
    assert _effective(path, "metaData")["configuration"] == {
        **baseline,
        "delta.logRetentionDuration": "2 weeks",
        "key": "value1",
    }
    # The first post-unset checkpoint must use the default interval 10, not the previous 20.
    for value in range(1, 8):
        spark.sql(f"INSERT INTO {name} VALUES ({value})").collect()  # noqa: S608
    assert max(_commits(path)) == 10
    assert [file.name for file in (path / "_delta_log").glob("*.checkpoint.parquet")] == [
        "00000000000000000010.checkpoint.parquet"
    ]
    assert spark.table(name).orderBy("id").collect() == [Row(id=value) for value in range(1, 8)]


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/DeltaAlterTableTests.scala#L135-L223
@pytest.mark.parametrize(
    ("key", "value", "error"),
    [
        ("delta.key", "value", r"(?i)(unknown.*config|config.*unknown)"),
        ("delta.randomPrefixLength", "-1", r"(?i)randomPrefixLength.*(greater|positive|invalid|integer)"),
        ("delta.randomPrefixLength", "value", r"(?i)randomPrefixLength.*(greater|positive|invalid|integer)"),
    ],
)
def test_property_validation_is_atomic(spark, lifecycle_table, key, value, error):
    name, path = lifecycle_table
    spark.sql(f"INSERT INTO {name} VALUES (1)").collect()  # noqa: S608
    rows, schema, before = spark.table(name).collect(), spark.table(name).schema, _tree(path)
    with _settings(spark, **{"spark.databricks.delta.allowArbitraryProperties.enabled": "false"}):
        with pytest.raises(Exception, match=error):
            spark.sql(
                f"ALTER TABLE {name} SET TBLPROPERTIES ('custom.valid' = 'must-not-persist', '{key}' = '{value}')"
            ).collect()
    _assert_unchanged(spark, name, path, before, rows, schema)


@pytest.mark.parametrize("sql_view", [False, True], ids=["dataframe-view", "sql-view"])
def test_property_update_rejects_temporary_view_without_mutation(spark, lifecycle_table, sql_view):
    name, path = lifecycle_table
    view = "maintenance_lifecycle_view"
    if sql_view:
        spark.sql(f"CREATE TEMP VIEW {view} AS SELECT * FROM {name}").collect()
    else:
        spark.table(name).createOrReplaceTempView(view)
    try:
        before, schema = _tree(path), spark.table(name).schema
        with pytest.raises(Exception, match=_TABLE_NOT_VIEW):
            spark.sql(f"ALTER TABLE {view} SET TBLPROPERTIES ('delta.checkpointInterval' = '20')").collect()
        _assert_unchanged(spark, name, path, before, [], schema)
    finally:
        spark.catalog.dropTempView(view)


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/python/delta/tests/test_deltatable.py#L1182-L1239
def test_protocol_upgrade_and_downgrade_preserve_rows(spark, lifecycle_table):
    name, path = lifecycle_table
    spark.sql(f"INSERT INTO {name} VALUES (1)").collect()  # noqa: S608
    files = _active_files(path)
    schema = spark.table(name).schema
    for writer_version in [3, 2]:
        spark.sql(
            f"ALTER TABLE {name} SET TBLPROPERTIES "
            f"('delta.minReaderVersion' = '1', 'delta.minWriterVersion' = '{writer_version}')"
        ).collect()
        assert _effective(path, "protocol") == {"minReaderVersion": 1, "minWriterVersion": 3}
        assert _active_files(path) == files
        assert spark.table(name).schema == schema
        assert spark.table(name).collect() == [Row(id=1)]
    spark.sql(f"INSERT INTO {name} VALUES (2)").collect()  # noqa: S608
    assert spark.table(name).orderBy("id").collect() == [Row(id=1), Row(id=2)]


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/python/delta/tests/test_deltatable.py#L1212-L1239
def test_feature_support_upgrades_reader_writer_dependencies(spark, lifecycle_table):
    name, path = lifecycle_table
    spark.sql(f"INSERT INTO {name} VALUES (1)").collect()  # noqa: S608
    files = _active_files(path)
    spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.feature.appendOnly' = 'supported')").collect()
    assert _effective(path, "protocol") == {"minReaderVersion": 1, "minWriterVersion": 2}
    spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.feature.deletionVectors' = 'supported')").collect()
    protocol = _effective(path, "protocol")
    assert protocol["minReaderVersion"] == 3
    assert protocol["minWriterVersion"] == 7
    assert set(protocol["readerFeatures"]) == {"deletionVectors"}
    assert set(protocol["writerFeatures"]) == {"appendOnly", "invariants", "deletionVectors"}
    assert set(protocol) == {"minReaderVersion", "minWriterVersion", "readerFeatures", "writerFeatures"}
    assert _active_files(path) == files
    assert spark.table(name).collect() == [Row(id=1)]
    spark.sql(f"INSERT INTO {name} VALUES (2)").collect()  # noqa: S608
    assert spark.table(name).orderBy("id").collect() == [Row(id=1), Row(id=2)]


@pytest.mark.parametrize("feature", ["abc", "checkpointProtection"])
def test_unsupported_feature_preserves_protocol_and_files(spark, lifecycle_table, feature):
    name, path = lifecycle_table
    spark.sql(f"INSERT INTO {name} VALUES (1)").collect()  # noqa: S608
    rows, schema, before = spark.table(name).collect(), spark.table(name).schema, _tree(path)
    with pytest.raises(Exception, match=r"(?i)(unsupported.*feature|feature.*not supported|unknown.*feature)"):
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.feature.{feature}' = 'supported')").collect()
    _assert_unchanged(spark, name, path, before, rows, schema)


@pytest.fixture
def property_table(spark, tmp_path):
    name = "property_protocol_lifecycle"
    path = tmp_path / "table"
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT) USING delta LOCATION '{escape_sql_string_literal(str(path))}'"
    ).collect()
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1)").collect()  # noqa: S608
        yield name, path
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/main/scala/org/apache/spark/sql/delta/DeltaConfig.scala#L186-L243
def test_reserved_property_configuration_is_live_and_restored(spark, property_table):
    name, path = property_table
    setting = "spark.databricks.delta.allowArbitraryProperties.enabled"
    previous = spark.conf.get(setting, None)
    with _settings(spark, **{setting: "true"}):
        spark.sql(
            f"ALTER TABLE {name} SET TBLPROPERTIES "
            "('DeLtA.customUnknown'='kept', 'checkpoint_interval'='custom', 'kEy'='value')"
        ).collect()
        configuration = _effective(path, "metaData")["configuration"]
        assert configuration["DeLtA.customUnknown"] == "kept"
        assert configuration["checkpoint_interval"] == "custom"
        assert configuration["kEy"] == "value"
        assert "delta.checkpointInterval" not in configuration
        with _settings(spark, **{setting: "false"}):
            before = _tree(path)
            with pytest.raises(Exception, match=r"(?i)unknown.*configuration"):
                spark.sql(
                    f"ALTER TABLE {name} SET TBLPROPERTIES "
                    "('custom.valid'='must-not-persist', 'delta.anotherUnknown'='rejected')"
                ).collect()
            assert _tree(path) == before
            # Existing unknown properties do not prevent unrelated valid alterations.
            spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('key'='distinct')").collect()
        assert spark.conf.get(setting) == "true"
    assert spark.conf.get(setting, None) == previous
    spark.sql(f"INSERT INTO {name} VALUES (2)").collect()  # noqa: S608
    assert _effective(path, "metaData")["configuration"]["checkpoint_interval"] == "custom"
    assert spark.table(name).orderBy("id").collect() == [Row(id=1), Row(id=2)]
    spark.sql(f"ALTER TABLE {name} UNSET TBLPROPERTIES ('checkpoint_interval')").collect()
    assert "checkpoint_interval" not in _effective(path, "metaData")["configuration"]


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/DeltaConfigSuite.scala#L36-L77
@pytest.mark.parametrize(
    "interval",
    ["5 MINUTES", "inTERval 5 minutes", "-1 day 86400001 milliseconds", "1 day -1 microseconds"],
)
def test_retention_calendar_interval_forms(spark, property_table, interval):
    name, path = property_table
    spark.sql(
        f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.logRetentionDuration'='{interval}', 'delta.deletedFileRetentionDuration'='0 seconds')"
    ).collect()
    assert _effective(path, "metaData")["configuration"]["delta.logRetentionDuration"] == interval
    spark.sql(f"INSERT INTO {name} VALUES (2)").collect()  # noqa: S608
    assert spark.table(name).orderBy("id").collect() == [Row(id=1), Row(id=2)]


@pytest.mark.parametrize(
    ("key", "value", "error"),
    [
        ("delta.logRetentionDuration", "1 day garbage", "(?i)interval"),
        ("delta.logRetentionDuration", "1 day -86400001 milliseconds", "(?i)interval"),
        ("delta.logRetentionDuration", "1 month", "(?i)interval"),
        ("delta.randomPrefixLength", "0", "randomPrefixLength"),
        ("delta.randomPrefixLength", "2147483648", "randomPrefixLength"),
        ("delta.minReaderVersion", "4", "minReaderVersion"),
        ("delta.minWriterVersion", "0", "minWriterVersion"),
        ("delta.feature.appendOnly", "invalid", "feature"),
        ("delta.feature.rowTracking", "supported", "(?i)(unsupported|not supported)"),
    ],
)
def test_invalid_properties_remain_atomic_with_arbitrary_properties_enabled(spark, property_table, key, value, error):
    name, path = property_table
    before = _tree(path)
    with _settings(spark, **{"spark.databricks.delta.allowArbitraryProperties.enabled": "true"}):
        with pytest.raises(Exception, match=error):
            spark.sql(
                f"ALTER TABLE {name} SET TBLPROPERTIES ('custom.valid'='must-not-persist', '{key}'='{value}')"
            ).collect()
    assert _tree(path) == before
    spark.sql(f"INSERT INTO {name} VALUES (2)").collect()  # noqa: S608
    assert spark.table(name).orderBy("id").collect() == [Row(id=1), Row(id=2)]


def test_random_prefix_length_does_not_enable_unsupported_layout(spark, property_table):
    name, path = property_table
    spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.randomPrefixLength'='5')").collect()
    assert _effective(path, "metaData")["configuration"]["delta.randomPrefixLength"] == "5"
    before = _tree(path)
    with _settings(spark, **{"spark.databricks.delta.allowArbitraryProperties.enabled": "true"}):
        with pytest.raises(Exception, match="randomizeFilePrefixes.*not supported"):
            spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.randomizeFilePrefixes'='true')").collect()
    assert _tree(path) == before
    spark.sql(f"INSERT INTO {name} VALUES (2)").collect()  # noqa: S608
    assert spark.table(name).orderBy("id").collect() == [Row(id=1), Row(id=2)]


@pytest.mark.integration
@pytest.mark.parametrize("writer_version", [2, 3])
def test_alter_protocol_metadata_and_alternating_writes(spark, delta_jvm_spark, property_table, writer_version):
    name, path = property_table
    spark.sql(
        f"ALTER TABLE {name} SET TBLPROPERTIES "
        f"('DeLtA.minWriterVersion'='{writer_version}', 'DELTA.FEATURE.APPENDONLY'='supported')"
    ).collect()
    assert _effective(path, "protocol") == {"minReaderVersion": 1, "minWriterVersion": writer_version}
    assert _effective(path, "metaData")["configuration"] == {}
    spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.feature.deletionVectors'='supported')").collect()
    protocol = _effective(path, "protocol")
    writers = {"appendOnly", "invariants", "deletionVectors"}
    if writer_version == 3:
        writers.add("checkConstraints")
    assert protocol["minReaderVersion"] == 3
    assert protocol["minWriterVersion"] == 7
    assert set(protocol["readerFeatures"]) == {"deletionVectors"}
    assert set(protocol["writerFeatures"]) == writers
    assert _effective(path, "metaData")["configuration"] == {}
    spark.sql(
        f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.minReaderVersion'='1', 'delta.minWriterVersion'='2')"
    ).collect()
    assert _effective(path, "protocol") == protocol
    assert _effective(path, "metaData")["configuration"] == {}
    with classic_spark_mode():
        assert delta_jvm_spark.sql(f"SELECT id FROM delta.`{path}`").collect() == [Row(id=1)]
        delta_jvm_spark.sql(f"INSERT INTO delta.`{path}` VALUES (2)").collect()  # noqa: S608
    spark.sql(f"INSERT INTO {name} VALUES (3)").collect()  # noqa: S608
    expected = [Row(id=1), Row(id=2), Row(id=3)]
    assert spark.table(name).orderBy("id").collect() == expected
    with classic_spark_mode():
        assert delta_jvm_spark.sql(f"SELECT id FROM delta.`{path}` ORDER BY id").collect() == expected


@pytest.mark.parametrize("key", ["checkpoint_interval", "append_only"])
def test_custom_alias_looking_property_survives_named_and_path_writes(spark, property_table, tmp_path, key):
    name, path = property_table
    spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('{key}'='custom')").collect()
    spark.sql(f"INSERT INTO {name} VALUES (2)").collect()  # noqa: S608
    spark.range(3, 4).write.format("delta").mode("append").save(str(path))
    assert _effective(path, "metaData")["configuration"] == {key: "custom"}
    assert spark.table(name).orderBy("id").collect() == [Row(id=1), Row(id=2), Row(id=3)]
    # Explicit DataFrame options retain their aliases; only persisted custom keys are literal.
    canonical, value = (
        ("delta.checkpointInterval", "2") if key == "checkpoint_interval" else ("delta.appendOnly", "true")
    )
    option_path = tmp_path / "explicit_option"
    spark.range(1).write.format("delta").option(key, value).save(str(option_path))
    assert _effective(option_path, "metaData")["configuration"] == {canonical: value}


# SQL CREATE aliases are a Sail extension; persisted custom ALTER keys remain literal.
@pytest.mark.parametrize("ctas", [False, True], ids=["create", "ctas"])
@pytest.mark.parametrize("aliases", [False, True], ids=["canonical", "aliases"])
def test_create_property_aliases_preserve_storage_behavior(spark, tmp_path, ctas, aliases):
    name = "create_property_aliases"
    path = tmp_path / "table"
    canonical = ["delta.appendOnly", "delta.checkpointInterval", "delta.columnMapping.mode"]
    keys = ["append_only", "checkpoint_interval", "column_mapping_mode"] if aliases else canonical
    properties = ", ".join(f"'{key}'='{value}'" for key, value in zip(keys, ["true", "2", "name"], strict=True))
    schema = "" if ctas else "(id BIGINT)"
    query = " AS SELECT CAST(1 AS BIGINT) AS id" if ctas else ""
    spark.sql(
        f"CREATE TABLE {name} {schema} USING delta LOCATION '{escape_sql_string_literal(str(path))}' "
        f"TBLPROPERTIES ({properties}){query}"
    ).collect()
    try:
        if not ctas:
            spark.sql(f"INSERT INTO {name} VALUES (1)").collect()  # noqa: S608
        configuration = _effective(path, "metaData")["configuration"]
        assert {key: configuration[key] for key in canonical} == dict(
            zip(canonical, ["true", "2", "name"], strict=True)
        )
        assert not {"append_only", "checkpoint_interval", "column_mapping_mode"} & configuration.keys()
        assert _effective(path, "metaData")["configuration"]["delta.columnMapping.maxColumnId"] == "1"
        before = _tree(path)
        with pytest.raises(Exception, match="(?i)append.only"):
            spark.sql(f"DELETE FROM {name} WHERE id = 1").collect()  # noqa: S608
        assert _tree(path) == before
        for value in (2, 3):
            spark.sql(f"INSERT INTO {name} VALUES ({value})").collect()  # noqa: S608
        assert spark.table(name).orderBy("id").collect() == [Row(id=1), Row(id=2), Row(id=3)]
        assert (path / "_delta_log" / "00000000000000000002.checkpoint.parquet").exists()
        assert _effective(path, "metaData")["configuration"] == configuration
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/main/scala/org/apache/spark/sql/delta/actions/actions.scala#L252-L256
@pytest.mark.parametrize("deletion_vectors", [False, True], ids=["explicit-invalid-pair", "invalid-writer-only"])
def test_invalid_protocol_request_rejected_before_merge(spark, property_table, deletion_vectors):
    name, path = property_table
    if deletion_vectors:
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.feature.deletionVectors'='supported')").collect()
        properties = "'delta.minWriterVersion'='2'"
    else:
        properties = "'delta.minReaderVersion'='3', 'delta.minWriterVersion'='2'"
    before = _tree(path)
    protocol = _effective(path, "protocol")
    with pytest.raises(Exception, match=r"(?i)(reader.*writer|read.*requires.*writ)"):
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ({properties}, 'custom.valid'='must-not-persist')").collect()
    assert _tree(path) == before
    assert _effective(path, "protocol") == protocol
    assert "custom.valid" not in _effective(path, "metaData")["configuration"]
    if deletion_vectors:
        spark.sql(
            f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.minReaderVersion'='1', 'delta.minWriterVersion'='2')"
        ).collect()
        assert _effective(path, "protocol") == protocol
    spark.sql(f"INSERT INTO {name} VALUES (2)").collect()  # noqa: S608
    assert spark.table(name).orderBy("id").collect() == [Row(id=1), Row(id=2)]


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/main/scala/org/apache/spark/sql/delta/DeltaConfig.scala#L351-L384
@pytest.mark.parametrize("entry", ["create", "ctas", "write", "alter"])
@pytest.mark.parametrize(
    ("log", "deleted", "enabled", "valid"),
    [
        ("1 day", None, True, False),
        (None, "31 days", True, False),
        ("1 day", "1 day", True, True),
        ("1 day", "2 days", True, False),
        ("1 day", None, False, True),
        ("0 microseconds", "1 microsecond", True, True),
    ],
)
def test_retention_compatibility(spark, tmp_path, entry, log, deleted, enabled, valid):
    path = tmp_path / "retention"
    name = "retention_compatibility"
    properties = {
        key: value
        for key, value in [("delta.logRetentionDuration", log), ("delta.deletedFileRetentionDuration", deleted)]
        if value is not None
    }
    values = ", ".join(f"'{key}'='{value}'" for key, value in properties.items())
    location = escape_sql_string_literal(str(path))
    if entry == "alter":
        spark.sql(f"CREATE TABLE {name} (id BIGINT) USING delta LOCATION '{location}'").collect()
        spark.sql(f"INSERT INTO {name} VALUES (1)").collect()  # noqa: S608
    before = _tree(path)
    setting = "spark.databricks.delta.vacuum.enforceDeletedFileAndLogRetentionDurationCompatibility"

    def apply():
        if entry == "write":
            spark.range(1, 2).write.format("delta").options(**properties).save(str(path))
        elif entry == "alter":
            spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ({values}, 'custom.marker'='committed')").collect()
        else:
            schema = "(id BIGINT)" if entry == "create" else ""
            query = "AS SELECT CAST(1 AS BIGINT) AS id" if entry == "ctas" else ""
            spark.sql(
                f"CREATE TABLE {name} {schema} USING delta LOCATION '{location}' TBLPROPERTIES ({values}) {query}"
            ).collect()

    try:
        with _settings(spark, **{setting: str(enabled).lower()}):
            if valid:
                apply()
                configuration = _effective(path, "metaData")["configuration"]
                assert all(configuration[key] == value for key, value in properties.items())
                expected = [] if entry == "create" else [Row(id=1)]
                assert spark.read.format("delta").load(str(path)).collect() == expected
            else:
                with pytest.raises(
                    Exception,
                    match=r"(?i)(logRetentionDuration.*deletedFileRetentionDuration|deletedFileRetentionDuration.*logRetentionDuration)",
                ):
                    apply()
                assert _tree(path) == before
                if entry == "alter":
                    assert spark.table(name).collect() == [Row(id=1)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


def test_retention_partial_alter_uses_defaults_and_restores_check(spark, property_table):
    name, path = property_table
    setting = "spark.databricks.delta.vacuum.enforceDeletedFileAndLogRetentionDurationCompatibility"
    with _settings(spark, **{setting: "true"}):
        spark.sql(
            f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.logRetentionDuration'='1 day', 'delta.deletedFileRetentionDuration'='1 day')"
        ).collect()
        before = _tree(path)
        with pytest.raises(Exception, match="logRetentionDuration"):
            spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.logRetentionDuration'='2 days')").collect()
        assert _tree(path) == before
        with _settings(spark, **{setting: "false"}):
            spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.logRetentionDuration'='2 days')").collect()
        assert spark.conf.get(setting) == "true"
        before = _tree(path)
        with pytest.raises(Exception, match="logRetentionDuration"):
            spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.logRetentionDuration'='3 days')").collect()
        assert _tree(path) == before
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('custom.marker'='unchanged-retention')").collect()
        assert _effective(path, "metaData")["configuration"]["delta.logRetentionDuration"] == "2 days"


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/rowid/RowTrackingUpdateSuite.scala#L90-L110
@pytest.mark.xfail(strict=True, reason="Delta row tracking is not supported")
def test_row_tracking_preserves_row_ids_across_append_and_update(spark, tmp_path):
    name = "row_tracking_properties"
    path = tmp_path / "row_tracking"
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT) USING delta LOCATION '{escape_sql_string_literal(str(path))}'"
    ).collect()
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1)").collect()  # noqa: S608
        spark.sql(
            f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.feature.rowTracking'='supported', 'delta.enableRowTracking'='true')"
        ).collect()
        assert {"rowTracking", "domainMetadata"} <= set(_effective(path, "protocol")["writerFeatures"])
        assert _effective(path, "metaData")["configuration"]["delta.enableRowTracking"] == "true"
        initial = spark.sql(f"SELECT _metadata.row_id AS row_id FROM {name}").first().row_id  # noqa: S608
        spark.sql(f"INSERT INTO {name} VALUES (2)").collect()  # noqa: S608
        spark.sql(f"UPDATE {name} SET id = 10 WHERE id = 1").collect()
        rows = spark.sql(f"SELECT id, _metadata.row_id AS row_id FROM {name} ORDER BY id").collect()  # noqa: S608
        assert [row.id for row in rows] == [2, 10]
        assert rows[1].row_id == initial
        assert rows[0].row_id != initial
        assert spark.read.format("delta").load(str(path)).orderBy("id").collect() == [Row(id=2), Row(id=10)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/DeltaAlterTableTests.scala#L225-L418
@pytest.mark.xfail(strict=True, reason="Delta random file prefixes are not supported")
@pytest.mark.parametrize("partitioned", [False, True])
def test_random_file_prefixes_follow_property_changes(spark, tmp_path, partitioned):
    name = "random_prefix_properties"
    path = tmp_path / "random_prefix"
    partition = "PARTITIONED BY (part)" if partitioned else ""
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, part STRING) USING delta {partition} LOCATION '{escape_sql_string_literal(str(path))}'"
    ).collect()
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1, 'x')").collect()  # noqa: S608
        initial = {file.path for file in _active_files(path)}
        spark.sql(
            f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.randomizeFilePrefixes'='true', 'delta.randomPrefixLength'='5')"
        ).collect()
        spark.sql(f"INSERT INTO {name} VALUES (2, 'y')").collect()  # noqa: S608
        randomized = {file.path for file in _active_files(path)} - initial
        assert randomized
        assert all(re.fullmatch(r"[A-Za-z0-9]{5}/.*\.parquet", file) for file in randomized)
        configuration = _effective(path, "metaData")["configuration"]
        assert configuration["delta.randomizeFilePrefixes"] == "true"
        assert configuration["delta.randomPrefixLength"] == "5"
        spark.sql(f"ALTER TABLE {name} UNSET TBLPROPERTIES ('delta.randomizeFilePrefixes')").collect()
        spark.sql(f"INSERT INTO {name} VALUES (3, 'z')").collect()  # noqa: S608
        restored = {file.path for file in _active_files(path)} - initial - randomized
        assert restored
        assert all(file.startswith("part=z/") if partitioned else "/" not in file for file in restored)
        assert "delta.randomizeFilePrefixes" not in _effective(path, "metaData")["configuration"]
        assert spark.read.format("delta").load(str(path)).orderBy("id").collect() == [
            Row(id=1, part="x"),
            Row(id=2, part="y"),
            Row(id=3, part="z"),
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()
