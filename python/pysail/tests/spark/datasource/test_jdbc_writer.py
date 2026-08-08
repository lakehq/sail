import sys
import types

import pyarrow as pa
import pytest

from pysail.testing.spark.utils.common import pyspark_version

if pyspark_version() < (4, 1):
    pytest.skip("Python data source requires Spark 4.1+", allow_module_level=True)

from pysail.spark.datasource.jdbc import JdbcDataSource


def test_postgres_writer_streams_arrow_batches(monkeypatch):
    calls = []

    class Resource:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self):
            return self

        def adbc_ingest(self, table, values, **options):
            calls.append((table, values.column("id").to_pylist(), options))

        def commit(self):
            calls.append("commit")

    dbapi = types.ModuleType("adbc_driver_postgresql.dbapi")
    dbapi.connect = lambda dsn: calls.append(("connect", dsn)) or Resource()
    package = types.ModuleType("adbc_driver_postgresql")
    package.dbapi = dbapi
    monkeypatch.setitem(sys.modules, "adbc_driver_postgresql", package)
    monkeypatch.setitem(sys.modules, "adbc_driver_postgresql.dbapi", dbapi)

    datasource = JdbcDataSource(
        options={
            "url": "jdbc:postgresql://localhost:5432/db",
            "dbtable": "analytics.events",
        }
    )
    writer = datasource.writer(pa.schema({"id": pa.int64()}), overwrite=False)

    assert writer.write(iter([pa.record_batch({"id": [1]}), pa.record_batch({"id": [2]})])) is None
    assert calls == [
        ("connect", "postgresql://localhost:5432/db"),
        ("events", [1], {"mode": "append", "db_schema_name": "analytics"}),
        ("events", [2], {"mode": "append", "db_schema_name": "analytics"}),
        "commit",
    ]
