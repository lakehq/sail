import pyarrow as pa
import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")

datasource = pytest.importorskip("pyspark.sql.datasource")


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


def test_python_datasource_respects_execution_batch_size(spark, batch_size):
    row_count = batch_size * 3 + 7

    class RowReader(datasource.DataSourceReader):
        def read(self, partition):  # noqa: ARG002
            for i in range(row_count):
                yield (i,)

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

    spark.dataSource.register(RowSource)
    rows = spark.read.format(RowSource.name()).load().mapInArrow(batch_lengths, "length long").collect()
    assert [row.length for row in rows] == [batch_size, batch_size, batch_size, 7]
