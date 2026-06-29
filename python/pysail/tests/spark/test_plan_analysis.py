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
