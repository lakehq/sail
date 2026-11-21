import pytest
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
