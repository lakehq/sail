from collections.abc import Iterator

import pandas as pd
import pyarrow as pa
import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.sql import Row, Window
from pyspark.sql.functions import PandasUDFType, pandas_udf, udtf
from pyspark.sql.types import BinaryType, StringType, StructType

from pysail.testing.spark.utils.common import pyspark_version

VALUES = [(0, b"", ""), (1, b"x" * 32, "y" * 32), (2, None, None)]


@pytest.fixture(scope="module")
def view_type_df(spark, tmp_path_factory):
    path = str(tmp_path_factory.mktemp("view-types") / "input.parquet")
    spark.createDataFrame(VALUES, "id int, value binary, text string").write.parquet(path)
    return spark.read.parquet(path)


def nested_value():
    return F.struct(
        "value",
        "text",
        F.array("value").alias("values"),
        F.create_map("text", "value").alias("mapping"),
    )


def expected_rows():
    return [Row(id=id_, value=value, text=text) for id_, value, text in VALUES]


def run_table_udtf(spark, df, name, function):
    view_name = f"{name}_input"
    spark.udtf.register(name, function)
    df.createOrReplaceTempView(view_name)
    try:
        return spark.sql(
            f"SELECT * FROM {name}(TABLE (SELECT id, value, text FROM {view_name})) ORDER BY id"  # noqa: S608
        ).collect()
    finally:
        spark.catalog.dropTempView(view_name)


def test_arrow_optimized_udf_normalizes_nested_view_types(view_type_df):
    extract = F.udf(lambda value: value["value"], "binary", useArrow=True)
    actual = (
        view_type_df.filter("text IS NOT NULL")
        .select("id", extract(nested_value()).alias("value"))
        .orderBy("id")
        .collect()
    )
    assert actual == [Row(id=id_, value=value) for id_, value, _ in VALUES[:2]]


@pytest.mark.skipif(pyspark_version() < (4, 1), reason="arrow_udf requires PySpark 4.1+")
@pytest.mark.parametrize("use_large", [False, True])
def test_arrow_udf_receives_spark_boundary_types(spark, view_type_df, use_large):
    from pyspark.sql.functions import arrow_udf

    key = "spark.sql.execution.arrow.useLargeVarTypes"
    previous = spark.conf.get(key)
    spark.conf.set(key, str(use_large).lower())
    try:
        binary_type = pa.large_binary() if use_large else pa.binary()
        string_type = pa.large_string() if use_large else pa.string()

        @arrow_udf("binary")
        def extract(value: pa.Array) -> pa.Array:
            assert value.type.field("value").type == binary_type
            assert value.type.field("text").type == string_type
            assert value.type.field("values").type.value_type == binary_type
            assert value.type.field("mapping").type.key_type == string_type
            assert value.type.field("mapping").type.item_type == binary_type
            return value.field("value")

        actual = (
            view_type_df.filter("text IS NOT NULL")
            .select("id", extract(nested_value()).alias("value"))
            .orderBy("id")
            .collect()
        )
        assert actual == [Row(id=id_, value=value) for id_, value, _ in VALUES[:2]]
    finally:
        spark.conf.set(key, previous)


def test_pandas_udfs_normalize_view_types(view_type_df):
    @pandas_udf("binary")
    def identity(value: pd.Series) -> pd.Series:
        return value

    @pandas_udf("binary", PandasUDFType.GROUPED_AGG)
    def first(value: pd.Series):
        return value.iloc[0]

    scalar = view_type_df.select("id", identity("value").alias("value")).orderBy("id").collect()
    aggregate = view_type_df.groupBy("id").agg(first("value").alias("value")).orderBy("id").collect()
    window = (
        view_type_df.select("id", first("value").over(Window.partitionBy("id")).alias("value")).orderBy("id").collect()
    )
    expected = [Row(id=id_, value=value) for id_, value, _ in VALUES]
    assert scalar == aggregate == window == expected


def test_map_udfs_normalize_view_types(view_type_df):
    pandas_rows = view_type_df.mapInPandas(lambda batches: batches, view_type_df.schema).orderBy("id").collect()
    assert pandas_rows == expected_rows()

    if pyspark_version() >= (4,):

        def identity(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            for batch in batches:
                assert batch.schema.field("value").type in (pa.binary(), pa.large_binary())
                assert batch.schema.field("text").type in (pa.string(), pa.large_string())
                yield batch

        arrow_rows = view_type_df.mapInArrow(identity, view_type_df.schema).orderBy("id").collect()
        assert arrow_rows == expected_rows()


@pytest.mark.skipif(pyspark_version() < (4, 1), reason="UDTF table arguments require PySpark 4.1+")
@pytest.mark.parametrize("use_arrow", [False, True])
def test_udtf_normalizes_view_type_table_argument(spark, view_type_df, use_arrow):
    @udtf(returnType="id int, value binary, text string", useArrow=use_arrow)
    class Echo:
        def eval(self, row: Row):
            yield tuple(row)

    name = f"view_type_echo_{'arrow' if use_arrow else 'row'}"
    assert run_table_udtf(spark, view_type_df, name, Echo) == expected_rows()


@pytest.mark.skipif(pyspark_version() < (4, 1), reason="arrow_udtf requires PySpark 4.1+")
@pytest.mark.parametrize("use_large", [False, True])
def test_arrow_udtf_receives_spark_boundary_types(spark, view_type_df, use_large):
    from pyspark.sql.functions import arrow_udtf

    key = "spark.sql.execution.arrow.useLargeVarTypes"
    previous = spark.conf.get(key)
    spark.conf.set(key, str(use_large).lower())
    try:
        binary_type = pa.large_binary() if use_large else pa.binary()
        string_type = pa.large_string() if use_large else pa.string()

        @arrow_udtf(returnType="id int, value binary, text string")
        class Echo:
            def eval(self, batch: pa.RecordBatch | pa.StructArray) -> Iterator[pa.Table]:
                data_type = batch.type if isinstance(batch, pa.StructArray) else pa.struct(batch.schema)
                assert data_type.field("value").type == binary_type
                assert data_type.field("text").type == string_type
                if isinstance(batch, pa.StructArray):
                    table = pa.Table.from_arrays(batch.flatten(), names=[field.name for field in batch.type])
                    for i in range(table.num_rows):
                        yield table.slice(i, 1)
                else:
                    yield pa.table(batch)

        name = f"view_type_arrow_echo_{'large' if use_large else 'standard'}"
        assert run_table_udtf(spark, view_type_df, name, Echo) == expected_rows()
    finally:
        spark.conf.set(key, previous)


@pytest.mark.skipif(pyspark_version() < (4, 1), reason="UDTF analyze requires PySpark 4.1+")
def test_udtf_analyze_normalizes_view_type_table_argument(spark, view_type_df):
    from pyspark.sql.udtf import AnalyzeArgument, AnalyzeResult

    @udtf
    class Echo:
        @staticmethod
        def analyze(arg: AnalyzeArgument) -> AnalyzeResult:
            assert isinstance(arg.dataType, StructType)
            assert isinstance(arg.dataType["value"].dataType, BinaryType)
            assert isinstance(arg.dataType["text"].dataType, StringType)
            return AnalyzeResult(arg.dataType)

        def eval(self, row: Row):
            yield tuple(row)

    assert run_table_udtf(spark, view_type_df, "view_type_analyze_echo", Echo) == expected_rows()
