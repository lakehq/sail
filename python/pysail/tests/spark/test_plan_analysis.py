"""Tests for plan analysis features."""


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


def test_input_files_union_distinct(spark, tmp_path):
    """inputFiles should return distinct file paths from both sides of a union."""
    left_path = str(tmp_path / "left_parquet")
    right_path = str(tmp_path / "right_parquet")
    spark.createDataFrame([(1, "a")], ["id", "name"]).write.parquet(left_path)
    spark.createDataFrame([(2, "b")], ["id", "name"]).write.parquet(right_path)

    read_df = spark.read.parquet(left_path).unionByName(spark.read.parquet(right_path))
    files = read_df.inputFiles()
    assert len(files) == len(set(files))
    assert any("left_parquet" in f for f in files)
    assert any("right_parquet" in f for f in files)
    assert all(f.startswith("file:///") for f in files)


def test_input_files_in_memory(spark):
    """inputFiles should return an empty list for in-memory DataFrames."""
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    files = df.inputFiles()
    assert files == []
