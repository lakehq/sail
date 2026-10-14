# ruff: noqa: S608, PLR2004
"""DDL interoperability for Delta and Iceberg tables registered in HMS."""

import json
from decimal import Decimal
from urllib.parse import urlparse

import boto3
import pytest

from pysail.tests.spark.catalog.hms.conftest import _describe_extended_properties, _reference_catalog_table
from pysail.tests.spark.catalog.hms.test_iceberg_catalog_commit import _metadata_location


def _reference_name(database, table, fmt):
    return f"iceberg.{database}.{table}" if fmt == "iceberg" else f"{database}.{table}"


def _s3_client(env):
    return boto3.client(
        "s3",
        endpoint_url=env["AWS_ENDPOINT"],
        aws_access_key_id=env["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=env["AWS_SECRET_ACCESS_KEY"],
        region_name=env["AWS_REGION"],
    )


def _iceberg_metadata(jvm_spark, database, table, env):
    location = _metadata_location(jvm_spark, database, table)
    url = urlparse(location)
    client = _s3_client(env)
    return location, json.loads(client.get_object(Bucket=url.netloc, Key=url.path.lstrip("/"))["Body"].read())


@pytest.mark.parametrize("fmt", ["delta", "iceberg"])
@pytest.mark.parametrize("creator", ["sail", "spark"])
def test_lakehouse_create_alter_drop(spark, jvm_spark, hms_s3_database, fmt, creator):
    table = "ddl_roundtrip"
    name = f"{hms_s3_database}.{table}"
    reference = _reference_name(hms_s3_database, table, fmt)
    properties = "'format-version'='3'" if fmt == "iceberg" else "'delta.enableTypeWidening'='true'"
    create_name = reference if creator == "spark" else name
    create_session = jvm_spark if creator == "spark" else spark
    create_session.sql(
        f"CREATE TABLE {create_name} (id INT, value STRING, part STRING) USING {fmt} "
        f"PARTITIONED BY (part) COMMENT 'lakehouse ddl' TBLPROPERTIES ({properties}, 'custom'='initial')"
    )
    location = _describe_extended_properties(spark, name)["Location"]
    assert spark.table(name).collect() == []
    assert jvm_spark.table(reference).collect() == []
    if fmt == "iceberg":
        assert _reference_catalog_table(jvm_spark, hms_s3_database, table).partitionColumnNames().isEmpty()

    spark.sql(f"INSERT INTO {name} VALUES (1, 'one', 'a')")
    spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('custom'='updated')")
    spark.sql(f"ALTER TABLE {name} ALTER COLUMN id TYPE BIGINT")
    spark.sql(f"ALTER TABLE {name} ALTER COLUMN value SET DEFAULT 'fallback'")
    spark.sql(f"INSERT INTO {name} (id, part) VALUES (2, 'b')")
    jvm_spark.sql(f"REFRESH TABLE {reference}")
    assert {r.key: r.value for r in jvm_spark.sql(f"SHOW TBLPROPERTIES {reference}").collect()}["custom"] == "updated"
    assert spark.table(name).schema["id"].dataType.simpleString() == "bigint"
    if creator == "sail" or fmt == "iceberg":
        assert (
            next(r.data_type for r in spark.sql(f"DESCRIBE TABLE {name}").collect() if r.col_name == "id") == "bigint"
        )
    assert jvm_spark.table(reference).schema["id"].dataType.simpleString() == "bigint"
    assert [(r.id, r.value, r.part) for r in jvm_spark.sql(f"SELECT * FROM {reference} ORDER BY id").collect()] == [
        (1, "one", "a"),
        (2, "fallback", "b"),
    ]
    jvm_spark.sql(f"INSERT INTO {reference} (id, part) VALUES (3, 'c')")
    assert spark.table(name).where("id = 3").first().value == "fallback"
    spark.sql(f"ALTER TABLE {name} ALTER COLUMN value DROP DEFAULT")
    spark.sql(f"INSERT INTO {name} VALUES (4, DEFAULT, 'd')")
    assert spark.table(name).where("id = 4").first().value is None
    spark.sql(f"ALTER TABLE {name} UNSET TBLPROPERTIES ('custom')")
    spark.sql(f"ALTER TABLE {name} UNSET TBLPROPERTIES IF EXISTS ('absent')")
    jvm_spark.sql(f"REFRESH TABLE {reference}")
    assert "custom" not in {r.key: r.value for r in jvm_spark.sql(f"SHOW TBLPROPERTIES {reference}").collect()}

    spark.sql(f"DROP TABLE {name}")
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    assert not spark.catalog.tableExists(name)
    # HMS DROP removes the registration; both formats remain readable by location.
    assert spark.read.format(fmt).load(location).count() == 4


@pytest.mark.parametrize("fmt", ["delta", "iceberg"])
def test_lakehouse_ctas_register_and_if_not_exists(spark, jvm_spark, hms_s3_database, fmt):
    name = f"{hms_s3_database}.ctas"
    partitioning = "PARTITIONED BY (bucket(4, id))" if fmt == "iceberg" else ""
    spark.sql(f"CREATE TABLE {name} USING {fmt} {partitioning} AS SELECT 1 AS id, 'a' AS value")
    location = _describe_extended_properties(spark, name)["Location"]
    spark.sql(f"CREATE TABLE IF NOT EXISTS {name} (different INT) USING {fmt}")
    assert [tuple(r) for r in spark.table(name).collect()] == [(1, "a")]
    with pytest.raises(Exception, match="already exists"):
        spark.sql(f"CREATE TABLE {name} (different INT) USING {fmt}")
    spark.sql(f"DROP TABLE {name}")
    spark.sql(f"CREATE TABLE {name} USING {fmt} LOCATION '{location}'")
    assert [(r.col_name, r.data_type) for r in spark.sql(f"DESCRIBE TABLE {name}").collect()][:2] == [
        ("id", "int"),
        ("value", "string"),
    ]
    spark.sql(f"INSERT INTO {name} VALUES (2, 'b')")
    reference = _reference_name(hms_s3_database, "ctas", fmt)
    jvm_spark.sql(f"REFRESH TABLE {reference}")
    assert [tuple(r) for r in jvm_spark.sql(f"SELECT * FROM {reference} ORDER BY id").collect()] == [(1, "a"), (2, "b")]


def test_iceberg_partition_transforms_and_metadata_ddl(spark, jvm_spark, hms_s3_database, hms_s3_env):
    table = "transforms"
    name = f"{hms_s3_database}.{table}"
    reference = f"iceberg.{name}"
    spark.sql(f"CREATE TABLE {name} (id INT, value STRING) USING iceberg PARTITIONED BY (bucket(4, id))")
    before, initial = _iceberg_metadata(jvm_spark, hms_s3_database, table, hms_s3_env)
    spark.sql(f"INSERT INTO {name} VALUES (1, 'a'), (2, 'b')")
    inserted_location, inserted = _iceberg_metadata(jvm_spark, hms_s3_database, table, hms_s3_env)
    url = urlparse(inserted_location)
    # A failed writer may leave a higher-numbered metadata file that was never committed.
    orphan = dict(inserted, properties={"uncommitted": "true"})
    _s3_client(hms_s3_env).put_object(
        Bucket=url.netloc,
        Key=f"{url.path.lstrip('/').rsplit('/', 1)[0]}/99999-uncommitted.metadata.json",
        Body=json.dumps(orphan).encode(),
    )
    spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('custom'='v')")
    after, metadata = _iceberg_metadata(jvm_spark, hms_s3_database, table, hms_s3_env)
    assert before != after
    assert metadata["table-uuid"] == initial["table-uuid"]
    assert metadata["partition-specs"] == initial["partition-specs"]
    assert metadata["partition-specs"][0]["fields"][0]["transform"] == "bucket[4]"
    assert metadata["properties"]["custom"] == "v"
    assert "uncommitted" not in metadata["properties"]
    assert metadata["current-snapshot-id"] == inserted["current-snapshot-id"]
    assert metadata["schemas"] == inserted["schemas"]
    assert metadata["metadata-log"][-1]["metadata-file"].startswith("s3://")
    assert not _reference_catalog_table(jvm_spark, hms_s3_database, table).partitionColumnNames().nonEmpty()
    assert [tuple(r) for r in jvm_spark.sql(f"SELECT * FROM {reference} ORDER BY id").collect()] == [(1, "a"), (2, "b")]
    jvm_spark.sql(f"ALTER TABLE {reference} ALTER COLUMN id TYPE BIGINT")
    assert spark.table(name).schema["id"].dataType.simpleString() == "bigint"
    assert next(r.data_type for r in spark.sql(f"DESCRIBE TABLE {name}").collect() if r.col_name == "id") == "bigint"


def test_iceberg_invalid_alter_preserves_committed_metadata(spark, jvm_spark, hms_s3_database, hms_s3_env):
    table = "invalid_ddl"
    name = f"{hms_s3_database}.{table}"
    spark.sql(f"CREATE TABLE {name} (id INT) USING iceberg")
    before, _ = _iceberg_metadata(jvm_spark, hms_s3_database, table, hms_s3_env)
    for operation, error in [
        ("ALTER COLUMN id TYPE STRING", "Cannot change Iceberg column"),
        ("ALTER COLUMN id SET DEFAULT 1", "format-version=3"),
        ("UNSET TBLPROPERTIES ('absent')", "not set"),
        ("SET TBLPROPERTIES ('metadata_location'='s3://invalid/metadata.json')", "reserved property"),
        ("SET TBLPROPERTIES ('spark.sql.sources.provider'='delta')", "Spark-internal"),
    ]:
        with pytest.raises(Exception, match=error):
            spark.sql(f"ALTER TABLE {name} {operation}")
        assert _metadata_location(jvm_spark, hms_s3_database, table) == before
    spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('format-version'='3')")
    spark.sql(f"ALTER TABLE {name} ALTER COLUMN id SET DEFAULT 7")
    spark.sql(f"INSERT INTO {name} VALUES (DEFAULT)")
    assert jvm_spark.table(f"iceberg.{name}").first().id == 7


def test_iceberg_type_promotion_preserves_defaults(spark, jvm_spark, hms_s3_database, hms_s3_env):
    table = "promote_defaults"
    name = f"{hms_s3_database}.{table}"
    reference = f"iceberg.{name}"
    jvm_spark.sql(
        f"CREATE TABLE {reference} (score FLOAT, amount DECIMAL(9, 2)) "
        "USING iceberg TBLPROPERTIES ('format-version'='3')"
    )
    iceberg = jvm_spark._jvm.org.apache.iceberg  # noqa: SLF001
    iceberg_table = iceberg.spark.Spark3Util.loadIcebergTable(jvm_spark._jsparkSession, reference)  # noqa: SLF001
    # Spark SQL cannot create initial defaults; the reference Iceberg API can.
    iceberg_table.updateSchema().addColumn(
        "id", iceberg.types.Types.IntegerType.get(), iceberg.expressions.Literal.of(7)
    ).commit()
    _, before = _iceberg_metadata(jvm_spark, hms_s3_database, table, hms_s3_env)
    spark.sql(f"ALTER TABLE {name} ALTER COLUMN id SET DEFAULT 9")
    spark.sql(f"ALTER TABLE {name} ALTER COLUMN score SET DEFAULT 1.5")
    spark.sql(f"ALTER TABLE {name} ALTER COLUMN amount SET DEFAULT 12.34")
    for column, target in [("id", "BIGINT"), ("score", "DOUBLE"), ("amount", "DECIMAL(18, 2)")]:
        spark.sql(f"ALTER TABLE {name} ALTER COLUMN {column} TYPE {target}")
    _, after = _iceberg_metadata(jvm_spark, hms_s3_database, table, hms_s3_env)
    fields = next(s["fields"] for s in after["schemas"] if s["schema-id"] == after["current-schema-id"])
    original_fields = next(s["fields"] for s in before["schemas"] if s["schema-id"] == before["current-schema-id"])
    assert [f["id"] for f in fields] == [f["id"] for f in original_fields]
    assert [f.get("initial-default") for f in fields] == [f.get("initial-default") for f in original_fields]
    assert {f["name"]: f["write-default"] for f in fields} == {"id": 9, "score": 1.5, "amount": "12.34"}
    spark.sql(f"INSERT INTO {name} VALUES (DEFAULT, DEFAULT, DEFAULT)")
    jvm_spark.sql(f"REFRESH TABLE {reference}")
    assert [tuple(r) for r in jvm_spark.sql(f"SELECT id, score, amount FROM {reference}").collect()] == [
        (9, 1.5, Decimal("12.34"))
    ]
    jvm_spark.sql(f"INSERT INTO {reference} VALUES (DEFAULT, DEFAULT, DEFAULT)")
    assert [tuple(r) for r in spark.sql(f"SELECT id, score, amount FROM {name}").collect()] == [
        (9, 1.5, Decimal("12.34"))
    ] * 2


def test_iceberg_typed_default_interoperability(spark, jvm_spark, hms_s3_database, hms_s3_env):
    table = "typed_defaults"
    name = f"{hms_s3_database}.{table}"
    reference = f"iceberg.{name}"
    spark.sql(
        f"CREATE TABLE {name} (id INT, amount DECIMAL(5, 2), d DATE, ts TIMESTAMP_NTZ) "
        "USING iceberg TBLPROPERTIES ('format-version'='3')"
    )
    for column, expression in [
        ("amount", "1.235"),
        ("d", "DATE '2026-01-02'"),
        ("ts", "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'"),
    ]:
        spark.sql(f"ALTER TABLE {name} ALTER COLUMN {column} SET DEFAULT {expression}")
    before, metadata = _iceberg_metadata(jvm_spark, hms_s3_database, table, hms_s3_env)
    fields = next(s["fields"] for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])
    with pytest.raises(Exception, match="Decimal literal cannot be represented"):
        spark.sql(f"ALTER TABLE {name} ALTER COLUMN amount SET DEFAULT 100000")
    assert _metadata_location(jvm_spark, hms_s3_database, table) == before
    spark.sql(f"INSERT INTO {name} (id) VALUES (1)")
    expected = tuple(
        jvm_spark.sql(
            "SELECT CAST(1.235 AS DECIMAL(5, 2)), DATE '2026-01-02', TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'"
        ).first()
    )
    jvm_spark.sql(f"REFRESH TABLE {reference}")
    assert [tuple(r) for r in jvm_spark.sql(f"SELECT amount, d, ts FROM {reference}").collect()] == [expected]

    iceberg = jvm_spark._jvm.org.apache.iceberg  # noqa: SLF001
    iceberg_table = iceberg.spark.Spark3Util.loadIcebergTable(jvm_spark._jsparkSession, reference)  # noqa: SLF001
    # The Iceberg API accepts typed values; Spark SQL resolves and casts its default expressions first.
    update = iceberg_table.updateSchema()
    for column, value in [("amount", Decimal("1.24")), ("d", "2026-01-02"), ("ts", "2026-01-02T03:04:05.123456")]:
        update.updateColumnDefault(column, iceberg.expressions.Literal.of(value))
    update.commit()
    _, metadata = _iceberg_metadata(jvm_spark, hms_s3_database, table, hms_s3_env)
    reference_fields = next(s["fields"] for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])
    assert reference_fields == fields
    jvm_spark.sql(f"REFRESH TABLE {reference}")
    jvm_spark.sql(f"INSERT INTO {reference} (id) VALUES (2)")
    assert [tuple(r) for r in spark.sql(f"SELECT amount, d, ts FROM {name}").collect()] == [expected, expected]


@pytest.mark.parametrize("fmt", ["delta", "iceberg"])
def test_lakehouse_column_changes(spark, jvm_spark, hms_s3_database, fmt):
    from pysail.testing.spark.ddl import exercise_lakehouse_column_changes

    name = f"{hms_s3_database}.column_changes"
    reference = _reference_name(hms_s3_database, "column_changes", fmt)
    properties = "TBLPROPERTIES ('delta.columnMapping.mode'='name')" if fmt == "delta" else ""
    spark.sql(f"CREATE TABLE {name} (id INT, value STRING) USING {fmt} {properties}")
    exercise_lakehouse_column_changes(spark, name)
    jvm_spark.sql(f"REFRESH TABLE {reference}")
    assert [tuple(row) for row in jvm_spark.sql(f"SELECT * FROM {reference} ORDER BY id").collect()] == [("one", 1), ("two", 2)]
    assert jvm_spark.table(reference).schema["renamed"].metadata["comment"] == "renamed column"


@pytest.mark.parametrize("fmt", ["delta", "iceberg"])
def test_lakehouse_table_rename(spark, jvm_spark, hms_s3_database, fmt):
    database = hms_s3_database
    table = f"{database}.rename_source"
    target = f"{database}.rename_target"
    reference = f"iceberg.{target}" if fmt == "iceberg" else target
    spark.sql(f"CREATE TABLE {table} (id INT) USING {fmt}")
    spark.sql(f"INSERT INTO {table} VALUES (1)")
    spark.sql(f"ALTER TABLE {table} RENAME TO rename_target")
    assert not spark.catalog.tableExists(table)
    assert jvm_spark.sql(f"SELECT id FROM {reference}").first().id == 1
    spark.sql(f"INSERT INTO {target} VALUES (2)")
    jvm_spark.sql(f"REFRESH TABLE {reference}")
    assert [r.id for r in jvm_spark.sql(f"SELECT id FROM {reference} ORDER BY id").collect()] == [1, 2]


@pytest.mark.parametrize("fmt", ["delta", "iceberg"])
def test_lakehouse_replace_columns(spark, jvm_spark, hms_s3_database, fmt):
    from pysail.testing.spark.ddl import exercise_lakehouse_replace_columns

    name = f"{hms_s3_database}.replace_columns"
    reference = _reference_name(hms_s3_database, "replace_columns", fmt)
    properties = "TBLPROPERTIES ('delta.columnMapping.mode'='name')" if fmt == "delta" else ""
    spark.sql(f"CREATE TABLE {name} (id INT, value STRING) USING {fmt} {properties}")
    expected = exercise_lakehouse_replace_columns(spark, name, fmt)
    jvm_spark.sql(f"REFRESH TABLE {reference}")
    assert [tuple(row) for row in jvm_spark.sql(f"SELECT * FROM {reference} ORDER BY id").collect()] == expected
    assert jvm_spark.table(reference).schema["value"].metadata["comment"] == "123"


@pytest.mark.parametrize("fmt", ["delta", "iceberg"])
def test_lakehouse_set_location(spark, jvm_spark, hms_s3_database, fmt):
    name = f"{hms_s3_database}.location_source"
    reference = _reference_name(hms_s3_database, "location_source", fmt)
    spark.sql(f"CREATE TABLE {name} (id INT) USING {fmt}")
    spark.sql(f"INSERT INTO {name} VALUES (1)")
    old_location = _describe_extended_properties(spark, name)["Location"]
    if fmt == "delta":
        target = f"{hms_s3_database}.location_target"
        spark.sql(f"CREATE TABLE {target} (id INT) USING delta")
        spark.sql(f"INSERT INTO {target} VALUES (2)")
        new_location = _describe_extended_properties(spark, target)["Location"]
        first = 2
    else:
        new_location = old_location + "_moved"
        first = 1
    spark.sql(f"ALTER TABLE {name} SET LOCATION '{new_location}'")
    assert _describe_extended_properties(spark, name)["Location"].rstrip("/") == new_location.rstrip("/")
    spark.sql(f"INSERT INTO {name} VALUES (3)")
    jvm_spark.sql(f"REFRESH TABLE {reference}")
    assert [r.id for r in jvm_spark.sql(f"SELECT id FROM {reference} ORDER BY id").collect()] == [first, 3]
    jvm_spark.sql(f"INSERT INTO {reference} VALUES (4)")
    assert [r.id for r in spark.table(name).orderBy("id").collect()] == [first, 3, 4]
    if fmt == "iceberg":
        iceberg = jvm_spark._jvm.org.apache.iceberg
        loaded = iceberg.spark.Spark3Util.loadIcebergTable(jvm_spark._jsparkSession, reference)
        assert loaded.location().rstrip("/") == new_location.rstrip("/")
    # Verify the same catalog operation in the reference runtime, then write through Sail.
    reference_location = old_location if fmt == "delta" else new_location + "_reference"
    jvm_spark.sql(f"ALTER TABLE {reference} SET LOCATION '{reference_location}'")
    spark.sql(f"INSERT INTO {name} VALUES (5)")
    jvm_spark.sql(f"REFRESH TABLE {reference}")
    expected = [1, 5] if fmt == "delta" else [1, 3, 4, 5]
    assert [r.id for r in jvm_spark.sql(f"SELECT id FROM {reference} ORDER BY id").collect()] == expected


def test_iceberg_write_order(spark, jvm_spark, hms_s3_database, hms_s3_env):
    from pysail.testing.spark.ddl import exercise_iceberg_write_order

    name = f"{hms_s3_database}.write_order"
    reference = f"iceberg.{name}"
    spark.sql(f"CREATE TABLE {name} (id INT, value INT) USING iceberg")
    exercise_iceberg_write_order(spark, name)
    _, metadata = _iceberg_metadata(jvm_spark, hms_s3_database, "write_order", hms_s3_env)
    order_id = metadata["default-sort-order-id"]
    assert order_id != 0
    jvm_spark.sql(f"REFRESH TABLE {reference}")
    assert jvm_spark.sql(f"SELECT * FROM {reference}").count() == 4
    jvm_spark.sql(f"ALTER TABLE {reference} WRITE UNORDERED")
    jvm_spark.sql(f"ALTER TABLE {reference} WRITE ORDERED BY id DESC NULLS LAST, value ASC")
    _, reference_metadata = _iceberg_metadata(jvm_spark, hms_s3_database, "write_order", hms_s3_env)
    assert reference_metadata["default-sort-order-id"] == order_id
    assert reference_metadata["sort-orders"] == metadata["sort-orders"]
    spark.sql(f"INSERT INTO {name} VALUES (5, 5)")
    assert spark.table(name).count() == 5


def test_iceberg_partition_evolution(spark, jvm_spark, hms_s3_database, hms_s3_env):
    from pysail.testing.spark.ddl import exercise_iceberg_partition_evolution

    name = f"{hms_s3_database}.partition_evolution"
    reference = f"iceberg.{name}"
    spark.sql(f"CREATE TABLE {name} (id INT, part STRING) USING iceberg PARTITIONED BY (part)")
    exercise_iceberg_partition_evolution(spark, name)
    jvm_spark.sql(f"REFRESH TABLE {reference}")
    assert [tuple(r) for r in jvm_spark.sql(f"SELECT * FROM {reference} ORDER BY id").collect()] == [(1, "zz"), (3, "updated"), (4, "longer")]
    _, metadata = _iceberg_metadata(jvm_spark, hms_s3_database, "partition_evolution", hms_s3_env)
    jvm_spark.sql(f"ALTER TABLE {reference} ADD PARTITION FIELD bucket(4, id) AS id_bucket")
    spark.sql(f"INSERT INTO {name} VALUES (5, 'after')")
    jvm_spark.sql(f"REFRESH TABLE {reference}")
    assert jvm_spark.sql(f"SELECT * FROM {reference}").count() == 4
    _, updated = _iceberg_metadata(jvm_spark, hms_s3_database, "partition_evolution", hms_s3_env)
    assert updated["partition-specs"][:len(metadata["partition-specs"])] == metadata["partition-specs"]


def test_iceberg_v2_merge_on_read_update(spark, jvm_spark, hms_s3_database):
    name = f"{hms_s3_database}.mor_update"
    reference = f"iceberg.{name}"
    spark.sql(f"CREATE TABLE {name} (id INT, value INT) USING iceberg PARTITIONED BY (bucket(2, id)) TBLPROPERTIES ('format-version'='2', 'write.update.mode'='merge-on-read')")
    spark.sql(f"INSERT INTO {name} VALUES (1, 10), (2, 20), (3, 30)")
    spark.sql(f"UPDATE {name} SET value=value+1 WHERE id<=2")
    jvm_spark.sql(f"REFRESH TABLE {reference}")
    assert [tuple(r) for r in jvm_spark.sql(f"SELECT * FROM {reference} ORDER BY id").collect()] == [(1, 11), (2, 21), (3, 30)]
    jvm_spark.sql(f"UPDATE {reference} SET value=value+1 WHERE id>=2")
    assert [tuple(r) for r in spark.sql(f"SELECT * FROM {name} ORDER BY id").collect()] == [(1, 11), (2, 22), (3, 31)]
