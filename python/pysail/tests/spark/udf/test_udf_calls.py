from pyspark.sql.functions import lit, udf, udtf
from pyspark.sql.types import IntegerType, StringType

QUERY = """
    SELECT a, b, c FROM VALUES
    (1, 1.0, 'x'), (2, 2.0, 'y'), (3, 3.0, 'z')
    AS tab(a, b, c)
"""


def test_udf_as_function(spark):
    df = spark.sql(QUERY)

    # The name matches the existing column case-insensitively, so the column is replaced.
    # The default return type is a string.
    out = df.withColumn("A", udf(lambda x: x + 1)(df.a))
    assert out.columns == ["A", "b", "c"]
    assert [r.A for r in out.orderBy("b").collect()] == ["2", "3", "4"]


def test_udf_return_type_as_ddl_string(spark):
    df = spark.sql(QUERY)

    out = df.withColumn("C", udf(lambda x: len(x), "int")(df.c))
    assert out.columns == ["a", "b", "C"]
    assert [r.C for r in out.orderBy("a").collect()] == [1, 1, 1]


def test_udf_return_type_as_data_type(spark):
    df = spark.sql(QUERY)

    out = df.withColumn("C", udf(lambda x: len(x), IntegerType())(df.c))
    assert out.columns == ["a", "b", "C"]
    assert [r.C for r in out.orderBy("a").collect()] == [1, 1, 1]


def test_udf_as_decorator(spark):
    df = spark.sql(QUERY)

    @udf(StringType())
    def fun(x):
        return x + "a"

    out = df.withColumn("A", fun(df.c))
    assert out.columns == ["A", "b", "c"]
    assert [r.A for r in out.orderBy("b").collect()] == ["xa", "ya", "za"]


def test_udtf_call_and_register(spark):
    class TestUDTF:
        def eval(self, x: int, y: int):
            yield x, x + 1
            yield y, y + 1

    fun = udtf(TestUDTF, returnType="a: int, b: int")
    assert sorted(tuple(r) for r in fun(lit(1), lit(1)).collect()) == [(1, 2), (1, 2)]

    spark.udtf.register("test_udtf", fun)
    rows = spark.sql("SELECT * FROM test_udtf(1, 2)").collect()
    assert sorted(tuple(r) for r in rows) == [(1, 2), (2, 3)]
