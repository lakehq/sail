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


def test_text_path_glob_filter(spark, tmp_path):
    path = tmp_path / "text_path_glob_filter"
    path.mkdir()
    (path / "keep.txt").write_text("keep\n")
    (path / "drop.txt").write_text("drop\n")

    df = spark.read.option("pathGlobFilter", "keep.*").text(str(path))

    assert df.collect() == [Row(value="keep")]


def test_text_scan_keeps_spark_file_partition_groups(spark, tmp_path):
    path = tmp_path / "text_file_partition_groups"
    path.mkdir()
    (path / "a.txt").write_text("aaaaaaa\n")
    (path / "b.txt").write_text("bbbbbb\n")
    (path / "c.txt").write_text("cc\n")
    (path / "d.txt").write_text("d\n")

    settings = {
        "spark.sql.files.maxPartitionBytes": "10",
        "spark.sql.files.openCostInBytes": "0",
        "spark.sql.files.minPartitionNum": "2",
        "spark.sql.files.maxPartitionNum": "100",
    }
    previous = {key: spark.conf.get(key, None) for key in settings}
    try:
        for key, value in settings.items():
            spark.conf.set(key, value)
        rows = (
            spark.read.text(str(path))
            .selectExpr("value", "spark_partition_id() AS pid")
            .collect()
        )
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)

    assert {row.value: row.pid for row in rows} == {
        "aaaaaaa": 0,
        "bbbbbb": 1,
        "cc": 1,
        "d": 2,
    }


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


def test_text_read_partition_columns(spark, tmp_path):
    root = tmp_path / "text_partitioned"
    (root / "country=us" / "city=sf").mkdir(parents=True)
    (root / "country=us" / "city=ny").mkdir(parents=True)
    (root / "country=ca" / "city=to").mkdir(parents=True)
    (root / "country=us" / "city=sf" / "part-0.txt").write_text("golden\nbridge\n", encoding="utf-8")
    (root / "country=us" / "city=ny" / "part-0.txt").write_text("hudson\n", encoding="utf-8")
    (root / "country=ca" / "city=to" / "part-0.txt").write_text("harbour\n", encoding="utf-8")

    read_df = spark.read.text(str(root)).select("value", "country", "city")

    rows = {(row.value, row.country, row.city) for row in read_df.collect()}
    assert rows == {
        ("golden", "us", "sf"),
        ("bridge", "us", "sf"),
        ("hudson", "us", "ny"),
        ("harbour", "ca", "to"),
    }
    expected_us_rows = 3
    assert read_df.filter("country = 'us'").count() == expected_us_rows
