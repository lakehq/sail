from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from pysail.tests.spark.iceberg.utils import create_catalog, create_sql_catalog

if TYPE_CHECKING:
    from pyiceberg.schema import Schema
    from pyiceberg.table import Table


@pytest.fixture(scope="session")
def iceberg_rest_config() -> dict[str, str] | None:
    if os.environ.get("TEST_PROFILE") != "integration":
        return None
    uri = os.environ.get("ICEBERG_REST_URI", "http://localhost:8181")
    s3_endpoint = os.environ.get("ICEBERG_S3_ENDPOINT", "http://localhost:9000")
    warehouse = os.environ.get("ICEBERG_WAREHOUSE", "s3://warehouse/")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "password")
    return {
        "uri": uri,
        "s3_endpoint": s3_endpoint,
        "s3_access_key": access_key,
        "s3_secret_key": secret_key,
        "warehouse": warehouse,
    }


@pytest.fixture
def catalog(tmp_path, iceberg_rest_config):
    if iceberg_rest_config:
        return create_catalog(tmp_path, iceberg_rest_config)
    return create_sql_catalog(tmp_path)


@pytest.fixture
def sql_catalog(tmp_path):
    return create_sql_catalog(tmp_path)


@pytest.fixture
def iceberg_table(catalog, request) -> Table:
    params = request.param
    schema: Schema = params["schema"]
    identifier: str = params["identifier"]
    partition_spec = params.get("partition_spec")
    table = catalog.create_table(identifier=identifier, schema=schema, partition_spec=partition_spec)
    try:
        yield table
    finally:
        catalog.drop_table(identifier)
