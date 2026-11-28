# SPDX-License-Identifier: Apache-2.0

import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.tests._dev.spark.catalog.temp_view_utils import Table


@pytest.fixture(scope="package")
def remote():
    # The catalog tests rely on the default memory catalog of a particular name.
    # We explicitly configure it so that the tests are skipped when `SPARK_REMOTE` is set,
    # since the remote server started outside the test suite may have incompatible catalog configuration.
    with spark_connect_server(
        envs={
            "SAIL_CATALOG__DEFAULT_CATALOG": "",
            "SAIL_CATALOG__DEFAULT_DATABASE": '["default"]',
            "SAIL_CATALOG__LIST": '[{name="sail", type="memory", initial_database=["default"], initial_database_comment="default database"}]',
        }
    ) as server:
        yield server.remote


@pytest.fixture
def tables(spark, tmp_path):
    """Returns a function that creates a `Table`, which is dropped with its views after the test."""
    created = []

    def create(fmt, columns="id BIGINT", properties=""):
        created.append(Table(spark, tmp_path, fmt, columns, properties))
        return created[-1]

    yield create
    for table in created:
        table.drop()
