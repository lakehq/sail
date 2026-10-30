def test_same_semantics_of_sql(spark):
    assert spark.sql("SELECT 1").sameSemantics(spark.sql("SELECT 1"))
    assert not spark.sql("SELECT 1").sameSemantics(spark.sql("SELECT 2"))


def test_same_semantics_ignores_aliases(spark):
    df1 = spark.range(10)
    df2 = spark.range(10)
    assert df1.withColumn("col1", df1.id * 2).sameSemantics(df2.withColumn("col1", df2.id * 2))
    assert df1.withColumn("col1", df1.id * 2).sameSemantics(df2.withColumn("col0", df2.id * 2))
    assert not df1.withColumn("col1", df1.id * 2).sameSemantics(df2.withColumn("col1", df2.id + 2))


def test_semantic_hash(spark):
    assert spark.sql("SELECT 1").semanticHash() == spark.sql("SELECT 1").semanticHash()
    assert (
        spark.range(10).selectExpr("id as col0").semanticHash()
        == spark.range(10).selectExpr("id as col1").semanticHash()
    )
    assert spark.range(10).semanticHash() != spark.range(5).semanticHash()
