"""Internal integration tests for the RuntimeEnv-backed Python storage bridge."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlsplit

import pyarrow as pa
import pytest

from pysail.spark.datasource._object_store import _get_object_store
from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail internal object-store bridge")

try:
    from pyspark.sql.datasource import DataSource, DataSourceReader, DataSourceWriter, InputPartition
except ImportError:
    pytest.skip("Python DataSource API requires PySpark 4+", allow_module_level=True)


class _ObjectStorePartition(InputPartition):
    def __init__(self, path: str, output_path: str) -> None:
        super().__init__(0)
        self.path = path
        self.output_path = output_path


class _ObjectStoreReader(DataSourceReader):
    def __init__(self, path: str, output_path: str) -> None:
        self.path = path
        self.output_path = output_path

    def partitions(self) -> list[InputPartition]:
        store = _get_object_store()

        # Planning runs on the driver. This verifies that the same RuntimeEnv-backed
        # capability is available before the reader is serialized for workers.
        meta = store.head(self.path)
        assert meta.size > 0

        parent = f"{Path(self.path).parent}/"
        listed = next(item for item in store.list(parent) if item.size == meta.size)

        # Metadata locations must be valid inputs to the global proxy too. This
        # matters for remote stores, whose native ObjectMeta locations are only keys.
        assert store.read(meta.location) == store.read(self.path)
        assert store.read(listed.location) == store.read(self.path)

        return [_ObjectStorePartition(self.path, self.output_path)]

    def read(self, partition: InputPartition):
        assert isinstance(partition, _ObjectStorePartition)
        store = _get_object_store()

        data = store.read(partition.path)
        prefix = store.read_range(partition.path, 0, 5)
        first, second = store.read_ranges(partition.path, [(0, 5), (6, 10)])
        assert first == prefix
        assert second == b"from"
        assert b"".join(store.iter_bytes(partition.path, chunk_size=3)) == data
        with pytest.raises(ValueError, match="max_bytes"):
            store.read(partition.path, max_bytes=4)
        with pytest.raises(FileNotFoundError):
            store.head(f"{partition.path}.missing")
        meta = store.head(partition.path)

        # Exercise mutations through the worker's registry as well. The proxy
        # itself is deliberately not part of the cloudpickled reader.
        store.write(partition.output_path, b"worker-write")
        assert store.read(partition.output_path) == b"worker-write"
        store.delete(partition.output_path)

        yield pa.RecordBatch.from_arrays(
            [
                pa.array([data.decode()]),
                pa.array([prefix.decode()]),
                pa.array([meta.size], type=pa.int64()),
            ],
            names=["value", "prefix", "size"],
        )


class _ObjectStoreDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "sail_object_store_test"

    def _paths(self) -> tuple[str, str]:
        path = self.options.get("path")
        output_path = self.options.get("output_path")
        if not path or not output_path:
            message = "path and output_path are required"
            raise ValueError(message)
        return path, output_path

    def __init__(self, options):
        super().__init__(options)
        assert _get_object_store().head(self.options["path"]).size > 0

    def schema(self) -> str:
        path, _ = self._paths()
        store = _get_object_store()

        # Schema inference is a control-plane callback and must see the session
        # registry too. Returning DDL also protects the existing schema contract.
        assert store.read_range(path, 0, 5) == b"hello"
        return "value STRING, prefix STRING, size BIGINT"

    def reader(self, _schema: pa.Schema) -> DataSourceReader:
        path, output_path = self._paths()
        return _ObjectStoreReader(path, output_path)


class _ObjectStoreWriter(DataSourceWriter):
    def __init__(self, output_path: str, planning_marker: str) -> None:
        self.output_path = output_path
        self.planning_marker = planning_marker

    def write(self, iterator):
        store = _get_object_store()
        assert store.read(self.planning_marker) == b"planned"

        count = sum(1 for _ in iterator)
        store.write(self.output_path, str(count).encode())
        return {"count": count}

    def commit(self, messages):
        store = _get_object_store()
        assert messages
        payload = store.read(self.output_path)
        store.write(f"{self.output_path}.committed", payload)


class _ObjectStoreWriteDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "sail_object_store_write_test"

    def schema(self) -> str:
        return "id INT"

    def writer(self, _schema: pa.Schema, _overwrite):
        output_path = self.options["path"]
        planning_marker = f"{output_path}.planned"

        # writer() executes during planning on the driver.
        _get_object_store().write(planning_marker, b"planned")
        return _ObjectStoreWriter(output_path, planning_marker)


def test_object_store_is_callback_scoped():
    with pytest.raises(RuntimeError, match="only available while executing"):
        _get_object_store()


def test_python_datasource_uses_session_object_store(spark, tmp_path):
    source = tmp_path / "source.txt"
    output = tmp_path / "worker-output.txt"
    source.write_text("hello from the object store")

    spark.dataSource.register(_ObjectStoreDataSource)

    rows = (
        spark.read.format("sail_object_store_test")
        .option("path", str(source))
        .option("output_path", str(output))
        .load()
        .collect()
    )

    assert len(rows) == 1
    assert rows[0]["value"] == "hello from the object store"
    assert rows[0]["prefix"] == "hello"
    assert rows[0]["size"] == len(source.read_bytes())
    assert not output.exists()


def test_python_datasource_writer_uses_session_object_store(spark, tmp_path):
    output = tmp_path / "writer-output.txt"

    spark.dataSource.register(_ObjectStoreWriteDataSource)

    df = spark.createDataFrame([(1,), (2,), (3,)], ["id"]).coalesce(1)
    df.write.format("sail_object_store_write_test").mode("append").save(str(output))

    assert output.read_text() == "3"
    assert (tmp_path / "writer-output.txt.committed").read_text() == "3"
    assert (tmp_path / "writer-output.txt.planned").read_text() == "planned"


class _FailingObjectStoreWriter(_ObjectStoreWriter):
    def write(self, _iterator):
        _get_object_store().write(self.output_path, b"partial")
        message = "intentional storage writer failure"
        raise RuntimeError(message)

    def abort(self, _messages):
        store = _get_object_store()
        store.delete(self.output_path)
        store.write(f"{self.output_path}.aborted", b"cleaned")


class _FailingObjectStoreDataSource(_ObjectStoreWriteDataSource):
    @classmethod
    def name(cls):
        return "sail_object_store_abort_test"

    def writer(self, _schema, _overwrite):
        return _FailingObjectStoreWriter(self.options["path"], "")


def test_object_store_abort_has_fresh_storage_scope(spark, tmp_path):
    output = tmp_path / "failed-output"
    spark.dataSource.register(_FailingObjectStoreDataSource)
    with pytest.raises(Exception, match="intentional storage writer failure"):
        spark.range(1).coalesce(1).write.format("sail_object_store_abort_test").save(str(output))
    assert not output.exists()
    assert output.with_suffix(".aborted").read_bytes() == b"cleaned"


class _ParquetStorageReader(DataSourceReader):
    def __init__(self, path):
        self.path = path

    def partitions(self):
        return [_ObjectStorePartition(self.path, "")]

    def read(self, partition):
        import io

        import pyarrow.parquet as pq

        store = _get_object_store()

        class StorageFile(io.RawIOBase):
            def __init__(self):
                self.position = 0
                self.size = store.head(partition.path).size

            def readable(self):
                return True

            def seekable(self):
                return True

            def tell(self):
                return self.position

            def seek(self, offset, whence=0):
                position = offset + (0 if whence == 0 else self.position if whence == 1 else self.size)
                if position < 0:
                    message = "negative seek"
                    raise ValueError(message)
                self.position = position
                return position

            def read(self, size=-1):
                end = self.size if size < 0 else min(self.size, self.position + size)
                if self.position >= end:
                    return b""
                data = store.read_range(partition.path, self.position, end)
                self.position += len(data)
                return data

        with StorageFile() as source:
            yield from pq.ParquetFile(source).iter_batches(batch_size=2, use_threads=False)


class _ParquetStorageDataSource(DataSource):
    @classmethod
    def name(cls):
        return "sail_object_store_parquet_test"

    def schema(self):
        return "id BIGINT"

    def reader(self, _schema):
        return _ParquetStorageReader(self.options["path"])


def test_object_store_real_parquet_consumer(spark, tmp_path):
    import pyarrow.parquet as pq

    name = "special#%20[1].parquet" if os.name == "nt" else "special#?%20[1].parquet"
    path = tmp_path / name
    pq.write_table(pa.table({"id": [1, 2, 3, 4]}), path, row_group_size=2)
    spark.dataSource.register(_ParquetStorageDataSource)
    rows = spark.read.format("sail_object_store_parquet_test").load(str(path)).collect()
    assert [row.id for row in rows] == [1, 2, 3, 4]


class _GlobReader(DataSourceReader):
    def __init__(self, pattern):
        self.pattern = pattern

    def partitions(self):
        return [_ObjectStorePartition(meta.location, "") for meta in _get_object_store().glob(self.pattern)]

    def read(self, partition):
        value = int(_get_object_store().read(partition.path))
        yield pa.record_batch([pa.array([value], type=pa.int64())], names=["id"])


class _GlobDataSource(DataSource):
    @classmethod
    def name(cls):
        return "sail_object_store_glob_test"

    def schema(self):
        return "id BIGINT"

    def reader(self, _schema):
        return _GlobReader(self.options["path"])


@pytest.mark.parametrize("as_url", [False, True])
def test_object_store_glob_plans_file_partitions(spark, tmp_path, as_url):
    for name, value in [
        ("part-1.txt", "1"),
        ("part-2.txt", "2"),
        ("other.csv", "3"),
        (".hidden.txt", "4"),
        ("_hidden.txt", "5"),
    ]:
        (tmp_path / name).write_text(value)
    hidden = tmp_path / "_temporary"
    hidden.mkdir()
    (hidden / "part-3.txt").write_text("6")
    pattern = f"{tmp_path.as_uri() if as_url else tmp_path}/*.txt"
    spark.dataSource.register(_GlobDataSource)
    rows = spark.read.format("sail_object_store_glob_test").load(pattern).collect()
    assert sorted(row.id for row in rows) == [1, 2]


@pytest.mark.parametrize("relative", [False, True])
def test_object_store_filesystem_parent_segments(spark, tmp_path, relative):
    (tmp_path / "child").mkdir()
    prefix = os.path.relpath(tmp_path) if relative else str(tmp_path)
    path = f"{prefix}/child/../literal[1]#%.txt"

    class Source(DataSource):
        @classmethod
        def name(cls):
            return "parent_segments"

        def schema(self):
            store = _get_object_store()
            store.write(path, b"value")
            assert store.read(path) == b"value"
            assert store.read_range(path, 1, 3) == b"al"
            assert store.read_ranges(path, [(0, 1), (1, 3)]) == [b"v", b"al"]
            assert b"".join(store.iter_bytes(path, chunk_size=2)) == b"value"
            meta = store.head(path)
            assert store.read(meta.location) == b"value"
            assert [m.location for m in store.list(f"{prefix}/child/../")] == [meta.location]
            store.delete(path)
            with pytest.raises(FileNotFoundError):
                store.read(path)
            return "id BIGINT"

    spark.dataSource.register(Source)
    assert spark.read.format(Source.name()).load().schema.simpleString() == "struct<id:bigint>"


def test_object_store_negative_ranges_and_limits(spark, tmp_path):
    path = str(tmp_path / "data")

    class Source(DataSource):
        @classmethod
        def name(cls):
            return "negative_storage_arguments"

        def schema(self):
            store = _get_object_store()
            store.write(path, b"value")
            for action in [
                lambda: store.read(path, max_bytes=-1),
                lambda: store.read_range(path, -1, 2),
                lambda: store.read_range(path, 0, -1),
                lambda: store.read_range(path, 0, 1, max_bytes=-1),
                lambda: store.read_ranges(path, [(-1, 2)]),
                lambda: store.read_ranges(path, [(0, -1)]),
                lambda: store.read_ranges(path, [(0, 1)], max_bytes=-1),
                lambda: store.read_ranges(path, [], max_ranges=-1),
                lambda: store.iter_bytes(path, chunk_size=-1),
                lambda: store.iter_objects(path, batch_size=-1),
            ]:
                with pytest.raises(ValueError, match=r"non-negative|too many byte ranges"):
                    action()
            return "id BIGINT"

    spark.dataSource.register(Source)
    assert spark.read.format(Source.name()).load().schema.simpleString() == "struct<id:bigint>"


def test_object_store_glob_cache_isolates_stores(tmp_path):
    """Identical key prefixes in two stores must not share cached metadata."""
    prefix = (tmp_path / "objects").as_uri()
    memory_prefix = f"memory://{urlsplit(prefix).path}"

    class Source(DataSource):
        @classmethod
        def name(cls):
            return "glob_cache_isolation"

        def schema(self):
            store = _get_object_store()
            store.write(f"{prefix}/local.txt", b"local")
            store.write(f"{memory_prefix}/memory.txt", b"memory")
            local = store.glob(f"{prefix}/*")
            memory = store.glob(f"{memory_prefix}/*")
            assert [m.location for m in local] == [f"{prefix}/local.txt"]
            assert [m.location for m in memory] == [f"{memory_prefix}/memory.txt"]
            assert store.read(memory[0].location) == b"memory"
            # Repeating discovery must retain the correct results on cache hits.
            assert store.glob(f"{prefix}/*.txt") == local
            assert store.glob(f"{memory_prefix}/*.txt") == memory
            return "id BIGINT"

    with (
        spark_connect_server({"SAIL_EXECUTION__FILE_LISTING_CACHE__TYPE": "session"}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        session = sessions.create()
        session.dataSource.register(Source)
        assert session.read.format(Source.name()).load().schema.simpleString() == "struct<id:bigint>"
