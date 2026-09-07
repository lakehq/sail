from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import Window
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql import types as T  # noqa: N812

from pysail.testing.spark.utils.common import is_jvm_spark


def test_when_with_inferred_bigint_sequence_bound(spark):
    df = spark.createDataFrame([(3,), (1,), (5,)], ["c"])
    assert df.schema["c"].dataType == T.LongType()

    conditional = F.when(F.col("c") <= 0, F.lit(1)).otherwise(F.col("c"))
    assert df.select(conditional.alias("bound")).schema["bound"].dataType == T.LongType()

    result = df.select("c", F.explode(F.sequence(F.lit(0), conditional - 1)).alias("value"))
    assert result.schema["value"].dataType == T.LongType()
    assert [(row.c, row.value) for row in result.orderBy("c", "value").collect()] == [
        (1, 0),
        (3, 0),
        (3, 1),
        (3, 2),
        (5, 0),
        (5, 1),
        (5, 2),
        (5, 3),
        (5, 4),
    ]


@pytest.mark.skipif(is_jvm_spark(), reason="Sail-native numeric types have no Spark SQL counterparts")
@pytest.mark.parametrize("conditional", ["CASE WHEN p THEN x ELSE {other} END", "if(p, x, {other})"])
@pytest.mark.parametrize(
    ("input_type", "common_type", "positive", "negative", "wide", "fraction"),
    [
        (pa.float16(), pa.float32(), 1.5, -2.25, 3.75, -4.5),
        (
            pa.decimal32(5, 2),
            pa.decimal32(8, 3),
            Decimal("12.25"),
            Decimal("-2.75"),
            Decimal("12345.125"),
            Decimal("-0.005"),
        ),
        (
            pa.decimal64(10, 2),
            pa.decimal64(16, 4),
            Decimal("12345678.25"),
            Decimal("-2.75"),
            Decimal("123456789012.1250"),
            Decimal("-0.0005"),
        ),
    ],
    ids=["float16", "decimal32", "decimal64"],
)
def test_conditional_preserves_native_parquet_types(
    spark, tmp_path, conditional, input_type, common_type, positive, negative, wide, fraction
):
    source = tmp_path / "source.parquet"
    destination = tmp_path / "result"
    pq.write_table(
        pa.table(
            {
                "id": range(5),
                "p": [True, False, None, False, True],
                "x": pa.array([positive, negative, None, positive, negative], type=input_type),
                "y": pa.array([wide, fraction, None, wide, fraction], type=common_type),
            }
        ),
        source,
    )
    merged = conditional.format(other="y")
    nullable = conditional.format(other="NULL")
    expressions = f"id, {merged} AS merged, {nullable} AS nullable"
    if input_type == pa.float16():
        expressions += f", typeof({nullable}) AS nullable_type"
    if pa.types.is_decimal64(input_type):
        cross_width = conditional.format(other="CAST(2 AS DECIMAL(50,2))")
        expressions += f", ({cross_width}) / 3 AS division"

    # Parquet preserves Arrow types that Spark Connect cannot represent in query results.
    spark.sql(f"SELECT {expressions} FROM parquet.`{source}`").write.parquet(str(destination))
    result = pq.read_table(destination).sort_by([("id", "ascending")])
    assert result.schema.field("merged").type == common_type
    assert result.schema.field("nullable").type == input_type
    assert result["merged"].to_pylist() == [positive, fraction, None, wide, negative]
    assert result["nullable"].to_pylist() == [positive, None, None, None, negative]
    if input_type == pa.float16():
        assert result["nullable_type"].to_pylist() == ["half float"] * 5
    if pa.types.is_decimal64(input_type):
        assert result.schema.field("division").type == pa.float64()
        assert result["division"].to_pylist() == [float(positive) / 3, 2 / 3, 2 / 3, 2 / 3, float(negative) / 3]


@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="Sail does not yet coerce unsigned conditional window inputs for shiftrightunsigned",
    strict=True,
)
def test_unsigned_conditional_window_shift_from_parquet(spark, tmp_path):
    source = tmp_path / "unsigned_window.parquet"
    pq.write_table(
        pa.table(
            {
                "id": pa.array([0, 1, 2], type=pa.int32()),
                "p": pa.array([True, False, True]),
                "u32": pa.array([3, 3, 3], type=pa.uint32()),
                "u64": pa.array([5, 5, 5], type=pa.uint64()),
            }
        ),
        source,
    )
    frame = spark.read.parquet(str(source))
    conditional = F.when(F.col("p"), F.col("u32")).otherwise(F.col("u64"))
    previous = F.lag(conditional).over(Window.orderBy("id"))
    # TODO: Resolve unsigned window inputs before choosing the shift's integer type.
    # Spark reads these Parquet types as BIGINT/DECIMAL(20,0); Sail retains UInt32/64.
    result = frame.select("id", F.shiftrightunsigned(previous, 1).alias("value"))
    assert [(row.id, row.value) for row in result.orderBy("id").collect()] == [
        (0, None),
        (1, 1),
        (2, 2),
    ]
