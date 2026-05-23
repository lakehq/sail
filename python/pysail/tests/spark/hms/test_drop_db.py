import contextlib
import uuid

import pytest
from pyspark.sql import SparkSession

from pysail.tests.spark.conftest import configure_spark_session, patch_spark_connect_session
from pysail.tests.spark.hms.conftest import _run_sail_hms_server

pytestmark = [
    pytest.mark.catalog_integration,
    pytest.mark.usefixtures("hms_s3_endpoint"),
]


def test_drop_db_sail(hms_s3_metastore_endpoint, hms_s3_env):
    with contextlib.contextmanager(_run_sail_hms_server)(hms_s3_metastore_endpoint, hms_s3_env) as remote:
        sail = (
            SparkSession.builder.remote(remote)
            .appName("test_drop_db_sail")
            .config("spark.sql.warehouse.dir", "s3://hms-warehouse/path_parity_s3/default")
            .create()
        )
        configure_spark_session(sail)
        patch_spark_connect_session(sail)

        uid = uuid.uuid4().hex[:8]
        database = f"test_fix_{uid}"
        table = f"tbl_{uid}"
        table_fqn = f"{database}.{table}"

        try:
            sail.sql(f"CREATE DATABASE {database}")
            sail.sql(f"CREATE TABLE {table_fqn} (id INT) USING PARQUET")
            sail.sql(f"INSERT INTO {table_fqn} VALUES (1)")  # noqa: S608
            sail.sql(f"DROP DATABASE {database} CASCADE")
        finally:
            sail.stop()
