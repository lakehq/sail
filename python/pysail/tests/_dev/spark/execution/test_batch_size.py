# SPDX-License-Identifier: Apache-2.0

import pyarrow as pa
import pytest

from pysail.testing.spark.session import spark_connect_server, spark_session_factory

pytestmark = pytest.mark.spark(
    version=">=4", skip_if_jvm=True, reason="Uses the Python DataSource API in Sail local-cluster mode"
)


@pytest.fixture(scope="module", params=[128, 512])
def batch_size(request):
    return request.param


@pytest.fixture(scope="module")
def remote(batch_size):
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_EXECUTION__BATCH_SIZE": str(batch_size),
        "SAIL_EXECUTION__DEFAULT_PARALLELISM": "1",
    }
    with spark_connect_server(envs=envs) as server:
        yield server.remote


@pytest.mark.spark(cross_version=True)
# A session override must reach the worker that runs the scan, not only the driver.
@pytest.mark.parametrize("session_batch_size", [None, 64])
def test_python_datasource_respects_execution_batch_size(remote, batch_size, session_batch_size):
    from pyspark.sql import datasource

    expected_batch_size = session_batch_size or batch_size
    row_count = expected_batch_size * 3 + 7

    class RowReader(datasource.DataSourceReader):
        def partitions(self):
            return [datasource.InputPartition(i) for i in range(2)]

        def read(self, partition):
            for i in range(row_count):
                yield (partition.value * row_count + i,)

    class RowSource(datasource.DataSource):
        @classmethod
        def name(cls):
            return "batch_size_test"

        def schema(self):
            return "id long"

        def reader(self, schema):  # noqa: ARG002
            return RowReader()

    def batch_lengths(batches):
        for batch in batches:
            yield pa.record_batch([pa.array([batch.num_rows], type=pa.int64())], names=["length"])

    with spark_session_factory(remote) as sessions:
        spark = sessions.create()
        if session_batch_size is not None:
            spark.sql(f"SET datafusion.execution.batch_size = {session_batch_size}").collect()
        spark.dataSource.register(RowSource)
        rows = spark.read.format(RowSource.name()).load().mapInArrow(batch_lengths, "length long").collect()
    # Each of the two partitions yields three full batches and a remainder.
    assert sorted(row.length for row in rows) == [7, 7] + [expected_batch_size] * 6
