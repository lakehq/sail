"""Tests for JDBCArrowDataSource behaviors."""

from __future__ import annotations

import pyarrow as pa
import pytest

from pysail.jdbc.datasource import JDBCArrowDataSource
from pysail.jdbc.exceptions import InvalidOptionsError


class DummyBackend:
    """Simple backend stub for unit tests."""

    def __init__(self, batches):
        self._batches = batches
        self.calls = []

    def read_batches(self, connection_string, query, fetch_size):
        self.calls.append((connection_string, query, fetch_size))
        return self._batches


class TestJDBCArrowDataSource:
    """Validate JDBCArrowDataSource safeguards and behaviors."""

    def test_plan_partitions_runs_validation(self):
        """plan_partitions should surface option validation errors."""
        datasource = JDBCArrowDataSource()
        options = {
            "url": "jdbc:postgresql://localhost:5432/mydb",
            "dbtable": "orders",
            "numPartitions": "0",
        }

        with pytest.raises(
            InvalidOptionsError,
            match="numPartitions must be >= 1",
        ):
            datasource.plan_partitions(options)

    def test_infer_schema_uses_backend(self, monkeypatch):
        """infer_schema should return backend schema and build LIMIT query."""
        datasource = JDBCArrowDataSource()
        batch = pa.record_batch(
            [pa.array([1]), pa.array(["a"])],
            names=["id", "name"],
        )
        backend = DummyBackend([batch])
        monkeypatch.setattr(
            "pysail.jdbc.datasource.get_backend",
            lambda _engine: backend,
        )

        options = {
            "url": "jdbc:postgresql://localhost:5432/mydb",
            "dbtable": "orders",
        }

        schema = datasource.infer_schema(options)

        assert schema.names == ["id", "name"]
        assert backend.calls[0][1] == "SELECT * FROM orders LIMIT 1"
        assert backend.calls[0][2] == 1

    def test_plan_partitions_with_explicit_predicates(self, monkeypatch):
        """Explicit predicates should bypass planner construction."""
        datasource = JDBCArrowDataSource()

        def fail_planner(*_args, **_kwargs):
            pytest.fail("PartitionPlanner should not be created")

        monkeypatch.setattr("pysail.jdbc.datasource.PartitionPlanner", fail_planner)

        options = {
            "url": "jdbc:postgresql://localhost:5432/mydb",
            "dbtable": "orders",
            "predicates": "status='active',status='pending'",
        }

        partitions = datasource.plan_partitions(options)

        expected_count = len(options["predicates"].split(","))
        assert len(partitions) == expected_count
        assert partitions[0]["predicate"] == "status='active'"
        assert partitions[1]["predicate"] == "status='pending'"

    def test_read_partition_streams_backend_batches(self, monkeypatch):
        """read_partition should pass predicate and yield backend batches."""
        datasource = JDBCArrowDataSource()
        batch = pa.record_batch(
            [pa.array([1, 2])],
            names=["id"],
        )
        backend = DummyBackend([batch])
        monkeypatch.setattr(
            "pysail.jdbc.datasource.get_backend",
            lambda _engine: backend,
        )

        options = {
            "url": "jdbc:postgresql://localhost:5432/mydb",
            "dbtable": "orders",
            "fetchsize": str(128),
        }
        partition_spec = {
            "partition_id": 3,
            "predicate": '"id" >= 0 AND "id" < 10',
        }

        batches = list(datasource.read_partition(partition_spec, options))

        assert batches == [batch]
        connection_string, query, fetch_size = backend.calls[0]
        assert connection_string.startswith("postgresql://")
        assert query.endswith('WHERE "id" >= 0 AND "id" < 10')
        expected_fetch_size = int(options["fetchsize"])
        assert fetch_size == expected_fetch_size
