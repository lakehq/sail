import pytest
from pyspark.sql import SparkSession
from pysail.tests.spark.hms.conftest import _run_sail_hms_server, _spark_s3_options
from pysail.tests.spark.conftest import configure_spark_session, patch_spark_connect_session
import contextlib
import uuid

pytestmark = pytest.mark.catalog_integration

def test_drop_db_sail(hms_s3_metastore_endpoint, hms_s3_env, hms_s3_endpoint):
    with contextlib.contextmanager(_run_sail_hms_server)(hms_s3_metastore_endpoint, hms_s3_env) as remote:
        sail = SparkSession.builder.remote(remote) \
            .appName("test_drop_db_sail") \
            .config("spark.sql.warehouse.dir", "s3://hms-warehouse/path_parity_s3/default") \
            .create()
        configure_spark_session(sail)
        patch_spark_connect_session(sail)

        uid = uuid.uuid4().hex[:8]
        database = f"test_fix_{uid}"
        table = f"tbl_{uid}"
        table_fqn = f"{database}.{table}"

        # Create database without explicit LOCATION -- our fix should provide one
        print("Creating database without explicit LOCATION")
        sail.sql(f"CREATE DATABASE {database}")

        # Create table without explicit LOCATION -- our fix should provide one
        print("Creating table without explicit LOCATION")
        sail.sql(f"CREATE TABLE {table_fqn} (id INT) USING PARQUET")

        # Insert data to make sure the table is properly materialized
        print("Inserting data")
        sail.sql(f"INSERT INTO {table_fqn} VALUES (1)")

        # Now drop the database CASCADE -- this should NOT NPE
        print("Dropping database CASCADE")
        try:
            sail.sql(f"DROP DATABASE {database} CASCADE")
            print("Drop successful - NPE fix works!")
        except Exception as e:
            print(f"Exception: {e}")
            raise e
        finally:
            sail.stop()
