import pytest
import requests
from pyiceberg.catalog.rest import RestCatalog
from pyspark.errors import PySparkException

from pysail.testing.spark.session import spark_connect_server


@pytest.fixture(scope="module")
def remote(iceberg_rest_endpoint, seaweedfs_host_endpoint):
    catalogs = (
        f'[{{name="rest",type="iceberg-rest",uri="{iceberg_rest_endpoint}"}},'
        '{name="files",type="memory",initial_database=["default"]}]'
    )
    with spark_connect_server(
        envs={
            "SAIL_CATALOG__DEFAULT_CATALOG": "rest",
            "SAIL_CATALOG__LIST": catalogs,
            "AWS_ACCESS_KEY_ID": "admin",
            "AWS_SECRET_ACCESS_KEY": "password",
            "AWS_REGION": "us-east-1",
            "AWS_ENDPOINT": seaweedfs_host_endpoint,
            "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
            "AWS_ALLOW_HTTP": "true",
        }
    ) as server:
        yield server.remote


@pytest.fixture(scope="module")
def reference(iceberg_rest_endpoint, seaweedfs_host_endpoint):
    catalog = RestCatalog(
        "reference",
        uri=iceberg_rest_endpoint,
        **{
            "s3.endpoint": seaweedfs_host_endpoint,
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.region": "us-east-1",
        },
    )
    catalog.create_namespace("add_files")
    yield catalog
    catalog.drop_namespace("add_files")


@pytest.mark.parametrize("version", [1, 2, 3])
@pytest.mark.parametrize("catalog_source", [False, True], ids=["path", "named-file-table"])
def test_rest_add_files(spark, reference, version, catalog_source):
    identifier = f"add_files.t_{version}_{int(catalog_source)}"
    target = f"rest.{identifier}"
    source = f"s3://icebergdata/{identifier}/source"
    source_table = "files.default.parquet_input"
    spark.range(2).coalesce(1).write.mode("overwrite").parquet(source)
    spark.sql(f"CREATE TABLE {target} (id BIGINT) USING iceberg TBLPROPERTIES ('format-version'='{version}')")
    try:
        if catalog_source:
            spark.sql(f"CREATE TABLE {source_table} (id BIGINT) USING parquet LOCATION '{source}'")
        before = reference.load_table(identifier)
        argument = source_table if catalog_source else f"parquet.`{source}`"
        call = f"CALL rest.system.add_files('{target}', '{argument}')"
        assert tuple(spark.sql(call).first()) == (1, 1 if version == 1 or catalog_source else None)
        after = reference.load_table(identifier)
        assert after.metadata_location != before.metadata_location
        assert after.refs()["main"].snapshot_id == after.current_snapshot().snapshot_id
        assert sorted(after.scan().to_arrow()["id"].to_pylist()) == [0, 1]
        with pytest.raises(PySparkException, match="already referenced"):
            spark.sql(call).collect()
        with pytest.raises(PySparkException, match="Unsupported add_files source format: iceberg"):
            spark.sql(f"CALL rest.system.add_files('{target}', '{target}')").collect()
        assert reference.load_table(identifier).metadata_location == after.metadata_location
    finally:
        if catalog_source:
            spark.sql(f"DROP TABLE {source_table}")
        spark.sql(f"DROP TABLE {target}")


def test_rest_snapshot_file_table(spark, reference):
    source = "files.default.snapshot_input"
    target = "rest.add_files.snapshot_target"
    path = "s3://icebergdata/snapshot-input"
    spark.range(3).coalesce(1).write.mode("overwrite").parquet(path)
    spark.sql(f"CREATE TABLE {source} (id BIGINT) USING parquet LOCATION '{path}'")
    try:
        assert tuple(spark.sql(f"CALL rest.system.snapshot('{source}', '{target}')").first()) == (1,)
        snapshot = reference.load_table("add_files.snapshot_target")
        assert snapshot.properties["gc.enabled"] == "false"
        assert sorted(snapshot.scan().to_arrow()["id"].to_pylist()) == [0, 1, 2]
        assert spark.table(source).count() == 3
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {target}")
        spark.sql(f"DROP TABLE {source}")


@pytest.mark.parametrize("partitioned", [False, True])
@pytest.mark.usefixtures("reference")
def test_rest_snapshot_variant(spark, iceberg_rest_endpoint, partitioned):
    source = "files.default.variant_input"
    identifier = f"add_files.variant_{int(partitioned)}"
    target = f"rest.{identifier}"
    path = f"s3://icebergdata/{identifier}/input"
    partition = "PARTITIONED BY (id)" if partitioned else ""
    spark.sql(f"CREATE TABLE {source} (id BIGINT, data VARIANT) USING parquet {partition} LOCATION '{path}'")
    try:
        spark.sql(f"""INSERT INTO {source} (id, data) VALUES (1, parse_json('{{"key":123}}'))""")  # noqa: S608
        call = f"CALL rest.system.snapshot('{source}', '{target}', properties => map('format-version','3'))"
        assert spark.sql(call).first()[0] == 1
        assert spark.table(target).selectExpr("variant_get(data, '$.key', 'int')").first()[0] == 123
        table = identifier.split(".")[-1]
        response = requests.get(f"{iceberg_rest_endpoint}/v1/namespaces/add_files/tables/{table}", timeout=30)
        response.raise_for_status()
        metadata = response.json()["metadata"]
        assert metadata["format-version"] == 3
        assert next(f["type"] for f in metadata["schemas"][0]["fields"] if f["name"] == "data") == "variant"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {target}")
        spark.sql(f"DROP TABLE {source}")
