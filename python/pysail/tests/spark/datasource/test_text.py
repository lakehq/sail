from pyspark.sql import Row


def safe_sort_key(row):
    return tuple((v is not None, v) for v in row)


def test_text_read_write_basic(spark, sample_df, tmp_path):
    path = str(tmp_path / "text_basic")
    sample_df = sample_df.select("col1")
    sample_df.write.text(path)

    read_df = spark.read.text(path, wholetext=False)
    new_rows = [Row(value=(r["col1"] if r["col1"] is not None else "")) for r in sample_df.collect()]
    new_df = spark.createDataFrame(new_rows)
    assert new_df.count() == read_df.count()
    assert sorted(new_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

    read_df = spark.read.text(path, wholetext=True)
    joined = "\n".join([r["col1"] if r["col1"] is not None else "\n" for r in sample_df.collect()])
    joined_df = spark.createDataFrame([Row(value=joined)])
    assert joined_df.count() == read_df.count()
    assert sorted(joined_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)


def test_text_read_write_compressed(spark, sample_df, tmp_path):
    path = str(tmp_path / "text_compressed_gzip")
    sample_df = sample_df.select("col1")
    sample_df.write.option("compression", "gzip").text(path)

    read_df = spark.read.format("text").option("whole_text", False).load(path)
    new_rows = [Row(value=(r["col1"] if r["col1"] is not None else "")) for r in sample_df.collect()]
    new_df = spark.createDataFrame(new_rows)
    assert new_df.count() == read_df.count()
    assert sorted(new_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)
    assert len(list((tmp_path / "text_compressed_gzip").glob("*.txt.gz"))) > 0

    read_df = spark.read.format("text").option("whole_text", True).load(path)
    joined = "\n".join([r["col1"] if r["col1"] is not None else "\n" for r in sample_df.collect()])
    joined_df = spark.createDataFrame([Row(value=joined)])
    assert joined_df.count() == read_df.count()
    assert sorted(joined_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)


def test_text_write_options(spark, sample_df, tmp_path):
    path = str(tmp_path / "text_write_options")
    sample_df = sample_df.select("col1")
    sample_df.write.option("line_sep", ";").option("compression", "gzip").text(path)

    read_df = spark.read.option("line_sep", ";").text(path)
    new_rows = [Row(value=(r["col1"] if r["col1"] is not None else "")) for r in sample_df.collect()]
    new_df = spark.createDataFrame(new_rows)
    assert new_df.count() == read_df.count()
    assert sorted(new_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)


def test_text_read_options(spark, sample_df, tmp_path):
    path = str(tmp_path / "text_write_options")
    sample_df = sample_df.select("col1")
    sample_df.write.option("line_sep", ";").option("compression", "gzip").text(path)

    read_df = spark.read.format("text").option("line_sep", ";").option("whole_text", False).load(path)
    new_rows = [Row(value=(r["col1"] if r["col1"] is not None else "")) for r in sample_df.collect()]
    new_df = spark.createDataFrame(new_rows)
    assert new_df.count() == read_df.count()
    assert sorted(new_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

    read_df = spark.read.format("text").option("line_sep", ";").option("whole_text", True).load(path)
    joined = ";".join([r["col1"] if r["col1"] is not None else ";" for r in sample_df.collect()])
    joined_df = spark.createDataFrame([Row(value=joined)])
    assert joined_df.count() == read_df.count()
    assert sorted(joined_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

    read_df = spark.read.text(path, wholetext=False)
    assert joined_df.count() == read_df.count()
    assert sorted(joined_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)


def test_text_read_projections(spark, sample_df, tmp_path):
    path = str(tmp_path / "text_projections")
    sample_df = sample_df.select("col1")
    sample_df.write.text(path)

    read_df = spark.read.text(path)

    projected_df = read_df.select("value")
    assert projected_df.columns == ["value"]
    assert projected_df.count() == sample_df.count()

    count = read_df.select("value").count()
    assert count == sample_df.count()

    values = [r.value for r in projected_df.collect()]
    expected = [r["col1"] if r["col1"] is not None else "" for r in sample_df.collect()]
    assert sorted(values) == sorted(expected)
