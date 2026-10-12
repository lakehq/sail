"""Nested CTAS schemas must agree with catalog metadata and Parquet field IDs."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.io.pyarrow import pyarrow_to_schema
from pyiceberg.table import StaticTable

from pysail.testing.spark.session import spark_connect_server


@pytest.fixture(scope="module", params=["apache", "lakekeeper"])
def catalog_options(request):
    if request.param == "lakekeeper":
        request.getfixturevalue("lakekeeper_warehouse_id")
        endpoint = request.getfixturevalue("lakekeeper_endpoint")
        return {"uri": f"{endpoint}/catalog", "warehouse": "demo"}
    return {"uri": request.getfixturevalue("iceberg_rest_endpoint")}


@pytest.fixture(scope="module")
def remote(catalog_options, seaweedfs_host_endpoint):
    options = ", ".join(f'{key}="{value}"' for key, value in catalog_options.items())
    with spark_connect_server(
        envs={
            "SAIL_CATALOG__LIST": f'[{{name="sail", type="iceberg-rest", {options}}}]',
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
def storage_properties(seaweedfs_host_endpoint):
    return {
        "s3.endpoint": seaweedfs_host_endpoint,
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
    }


@pytest.fixture(scope="module")
def reference_catalog(catalog_options, storage_properties):
    return RestCatalog("reference", **catalog_options, **storage_properties)


@pytest.fixture(scope="module", autouse=True)
def namespace(spark):
    spark.sql("CREATE NAMESPACE nested_ctas")
    yield
    spark.sql("DROP NAMESPACE nested_ctas CASCADE")


@pytest.mark.parametrize("creation", ["ctas", "dataframe", "copy"])
@pytest.mark.parametrize(
    ("nested", "partitioned"),
    [
        ("ARRAY('a')", False),
        ("NAMED_STRUCT('x', 1, 'y', ARRAY('inner'))", True),
        ("MAP('k', NAMED_STRUCT('x', 1, 'y', ARRAY('inner')))", False),
        ("ARRAY(NAMED_STRUCT('x', MAP('k', ARRAY('a')), 'y', NAMED_STRUCT('z', 1)))", True),
    ],
    ids=["array", "struct", "map", "deep"],
)
def test_create_nested_table_as_select(
    spark, reference_catalog, storage_properties, creation, nested, partitioned, request
):
    table = "nested_ctas.t_" + request.node.callspec.id.replace("-", "_")
    query = f"SELECT {nested} AS a, 'stablecoin' AS c, 7 AS n, TRUE AS b, 'tail' AS d"
    source = spark.sql(query)
    expected_rows = [row.asDict(recursive=True) for row in source.collect()]
    try:
        if creation == "copy":
            columns = ", ".join(f"{field.name} {field.dataType.simpleString()}" for field in source.schema)
            spark.sql(f"CREATE TABLE {table}_source ({columns}) USING iceberg")
            spark.sql(f"INSERT INTO {table}_source {query}")
            query = f"SELECT * FROM {table}_source"  # noqa: S608
        if creation in {"ctas", "copy"}:
            partition_clause = "PARTITIONED BY (n)" if partitioned else ""
            spark.sql(f"CREATE TABLE {table} USING iceberg {partition_clause} AS {query}")
        else:
            writer = source.writeTo(table).using("iceberg")
            if partitioned:
                writer = writer.partitionedBy("n")
            writer.create()

        reference = StaticTable.from_metadata(
            reference_catalog.load_table(table).metadata_location, properties=storage_properties
        )
        tasks = list(reference.scan().plan_files())
        assert tasks
        for task in tasks:
            with reference.io.new_input(task.file.file_path).open() as data:
                file_schema = pyarrow_to_schema(pq.read_schema(data))
            assert {name: file_schema.find_field(name).field_id for name in file_schema.column_names} == {
                name: reference.schema().find_field(name).field_id for name in reference.schema().column_names
            }
        assert [row.asDict(recursive=True) for row in spark.table(table).collect()] == expected_rows
        actual = reference.scan().to_arrow()
        assert actual.equals(pa.Table.from_pylist(expected_rows, schema=actual.schema))
        if partitioned:
            assert reference.spec().fields[0].source_id == reference.schema().find_field("n").field_id
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        if creation == "copy":
            spark.sql(f"DROP TABLE IF EXISTS {table}_source")
