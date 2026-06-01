"""Tests for plan analysis features: sameSemantics, semanticHash, and inputFiles."""

from pyspark.sql.functions import col, lit


def test_same_semantics_equal(spark):
    """Two DataFrames with the same plan should be semantically equal."""
    df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    df2 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    assert df1.sameSemantics(df1)
    assert df2.sameSemantics(df2)


def test_same_semantics_filter(spark):
    """The same filter applied to the same base plan should be semantically equal."""
    base = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "name"])
    filtered1 = base.filter(col("id") > 1)
    filtered2 = base.filter(col("id") > 1)
    assert filtered1.sameSemantics(filtered2)


def test_same_semantics_different(spark):
    """Two DataFrames with different plans should not be semantically equal."""
    base = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    df1 = base.filter(col("id") > 1)
    df2 = base.filter(col("id") > lit(2))
    assert not df1.sameSemantics(df2)


def test_semantic_hash_returns_int(spark):
    """semanticHash should return an integer."""
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    result = df.semanticHash()
    assert isinstance(result, int)


def test_semantic_hash_same_plan(spark):
    """The same plan should produce the same semantic hash."""
    df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    df2 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    assert df1.semanticHash() == df2.semanticHash()


def test_semantic_hash_same_semantics_consistency(spark):
    """Plans that are semantically equal should have the same hash."""
    base = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "name"])
    df1 = base.filter(col("id") > 1)
    df2 = base.filter(col("id") > 1)
    assert df1.sameSemantics(df2)
    assert df1.semanticHash() == df2.semanticHash()


def test_input_files_parquet(spark, tmp_path):
    """inputFiles should return the parquet file paths."""
    data = [(1, "a"), (2, "b"), (3, "c")]
    df = spark.createDataFrame(data, ["id", "name"])
    path = str(tmp_path / "test_parquet")
    df.write.parquet(path)

    read_df = spark.read.parquet(path)
    files = read_df.inputFiles()
    assert len(files) > 0
    assert all(f.startswith("file:///") for f in files)
    assert all(".parquet" in f or "part-" in f for f in files)


def test_input_files_csv(spark, tmp_path):
    """inputFiles should return the CSV file paths."""
    data = [(1, "a"), (2, "b")]
    df = spark.createDataFrame(data, ["id", "name"])
    path = str(tmp_path / "test_csv")
    df.write.csv(path, header=True)

    read_df = spark.read.csv(path, header=True)
    files = read_df.inputFiles()
    assert len(files) > 0
    assert all(f.startswith("file:///") for f in files)


def test_input_files_in_memory(spark):
    """inputFiles should return an empty list for in-memory DataFrames."""
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    files = df.inputFiles()
    assert files == []
