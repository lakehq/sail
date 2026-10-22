from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import ArrayType, DoubleType, LongType, StructType

from pysail.tests.spark.dataframe.udt import PythonPoint, PythonPointUDT


# Ported from Spark 3 (3.5.9): pyspark/sql/tests/test_types.py, TypesTestsMixin.test_udf_with_udt.
def test_udf_with_udt_argument_and_result(spark):
    df = spark.createDataFrame([Row(label=1.0, point=PythonPoint(1.0, 2.0))])
    y = F.udf(lambda p: p.y, DoubleType())
    assert df.select(y(df.point)).first()[0] == 2.0  # noqa: PLR2004
    shift = F.udf(lambda p: PythonPoint(p.x + 1, p.y + 1), PythonPointUDT())
    point = df.select(shift(df.point)).first()[0]
    assert isinstance(point, PythonPoint)
    assert (point.x, point.y) == (2.0, 3.0)


# Ported from Spark 3 (3.5.9): pyspark/sql/tests/test_types.py, TypesTestsMixin.test_complex_nested_udt_in_df.
def test_udf_with_nested_udt(spark):
    schema = StructType().add("key", LongType()).add("val", PythonPointUDT())
    df = spark.createDataFrame([(i % 3, PythonPoint(float(i), float(i))) for i in range(10)], schema=schema)
    grouped = df.groupBy("key").agg(F.collect_list("val").alias("vals"))
    first = F.udf(lambda k, v: [(k, v[0])], ArrayType(df.schema))
    rows = grouped.select(first("key", "vals").alias("v")).collect()
    assert len(rows) == 3  # noqa: PLR2004
    for row in rows:
        [(key, point)] = row.v
        assert isinstance(point, PythonPoint)
        assert point.x % 3 == key


# Ported from Spark 3 (3.5.9): pyspark/sql/tests/test_types.py, TypesTestsMixin.test_udt_with_none.
def test_registered_udf_with_udt_result(spark):
    def point(x):
        if x > 0:
            return PythonPoint(float(x), float(x))
        return None

    spark.udf.register("udt_point", point, PythonPointUDT())
    try:
        rows = [r[0] for r in spark.range(0, 10, 1, 1).selectExpr("udt_point(id)").take(2)]
        assert rows[0] is None
        assert isinstance(rows[1], PythonPoint)
        assert (rows[1].x, rows[1].y) == (1.0, 1.0)
    finally:
        spark.sql("DROP TEMPORARY FUNCTION IF EXISTS udt_point")
