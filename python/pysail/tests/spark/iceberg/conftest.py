from typing import TYPE_CHECKING

import pytest
from pyiceberg.table import Table

from pysail.tests.spark.iceberg.utils import create_sql_catalog

if TYPE_CHECKING:
    from pyiceberg.schema import Schema


@pytest.fixture
def sql_catalog(tmp_path):
    return create_sql_catalog(tmp_path)


@pytest.fixture
def iceberg_table(sql_catalog, request) -> Table:
    params = request.param
    schema: Schema = params["schema"]
    identifier: str = params["identifier"]
    partition_spec = params.get("partition_spec")
    table = sql_catalog.create_table(identifier=identifier, schema=schema, partition_spec=partition_spec)
    try:
        yield table
    finally:
        sql_catalog.drop_table(identifier)
