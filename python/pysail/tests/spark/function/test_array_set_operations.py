import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import Row


def test_array_set_operations_with_different_element_nullability(spark):
    df = spark.sql("SELECT array(1, 2, 3) AS a, array(2, NULL, 4) AS b")
    assert df.schema.simpleString() == "struct<a:array<int>,b:array<int>>"
    actual = df.select(
        F.array_except("a", "b").alias("except_ab"),
        F.array_except("b", "a").alias("except_ba"),
        F.array_union("a", "b").alias("union"),
        F.array_intersect("a", "b").alias("intersect"),
        F.arrays_overlap("a", "b").alias("overlap"),
    )
    assert actual.collect() == [
        Row(except_ab=[1, 3], except_ba=[None, 4], union=[1, 2, 3, None, 4], intersect=[2], overlap=True)
    ]
    assert actual.schema["except_ab"].dataType.containsNull is False
    assert actual.schema["except_ba"].dataType.containsNull is True
    assert actual.schema["union"].dataType.containsNull is True
