"""Tests for the DataFrame ``inputFiles`` snapshot of constituent files."""


def test_input_files_single_file(spark, tmp_path):
    """Return the single file backing a one-partition dataset."""
    spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}]).repartition(1).write.json(
        str(tmp_path), mode="overwrite"
    )
    df = spark.read.format("json").load(str(tmp_path))

    files = df.inputFiles()
    assert len(files) == 1
    assert all(isinstance(f, str) for f in files)
    assert files[0].endswith(".json")


def test_input_files_multiple_files(spark, tmp_path):
    """Return one entry per file when the dataset spans multiple partitions."""
    partitions = 3
    spark.range(0, 10, 1, partitions).write.json(str(tmp_path), mode="overwrite")
    df = spark.read.format("json").load(str(tmp_path))

    assert len(df.inputFiles()) == partitions


def test_input_files_union_is_deduplicated(spark, tmp_path):
    """Files shared by both sides of a union are reported only once."""
    partitions = 3
    spark.range(0, 10, 1, partitions).write.json(str(tmp_path), mode="overwrite")
    df = spark.read.format("json").load(str(tmp_path))

    # The union references the same source twice; the files must not be double-counted.
    assert len(df.union(df).inputFiles()) == partitions


def test_input_files_excludes_hidden_markers(spark, tmp_path):
    """Hidden files such as ``_SUCCESS`` and ``.crc`` are not part of the dataset."""
    spark.range(3).repartition(1).write.json(str(tmp_path), mode="overwrite")
    (tmp_path / "_SUCCESS").touch()
    (tmp_path / ".crc").touch()
    df = spark.read.format("json").load(str(tmp_path))

    files = df.inputFiles()
    assert len(files) == 1
    assert all("_SUCCESS" not in f and not f.endswith("/.crc") for f in files)


def test_input_files_are_percent_encoded(spark, tmp_path):
    """Returned URIs percent-encode reserved characters such as spaces."""
    path = str(tmp_path / "a b")
    spark.range(1).repartition(1).write.json(path, mode="overwrite")
    df = spark.read.format("json").load(path)

    files = df.inputFiles()
    assert len(files) == 1
    assert "%20" in files[0]
    assert " " not in files[0]


def test_input_files_ignores_eliminated_scans(spark, tmp_path):
    """A scan removed by optimization (``WHERE false``) contributes no files."""
    spark.range(5).repartition(1).write.json(str(tmp_path), mode="overwrite")
    df = spark.read.format("json").load(str(tmp_path))

    assert df.filter("1 = 0").inputFiles() == []
    assert len(df.inputFiles()) == 1


def test_input_files_excludes_nested_hidden_directories(spark, tmp_path):
    """A hidden directory nested below a visible one is excluded from reads and inputFiles."""
    rows = 3
    visible = tmp_path / "visible"
    visible.mkdir()
    (visible / "good.json").write_text("".join(f'{{"id": {i}}}\n' for i in range(rows)))
    (visible / "_hidden").mkdir()
    (visible / "_hidden" / "bad.json").write_text('{"id": 99}\n')
    df = spark.read.format("json").load(str(tmp_path))

    # The nested hidden file must be excluded from both the scan and inputFiles.
    assert df.count() == rows
    files = df.inputFiles()
    assert len(files) == 1
    assert all("_hidden" not in f for f in files)


def test_input_files_excludes_explicitly_targeted_hidden_file(spark, tmp_path):
    """An explicitly targeted hidden file is ignored, as in Spark."""
    hidden = tmp_path / "_data.json"
    hidden.write_text('{"id": 1}\n')
    df = spark.read.schema("id long").json(str(hidden))

    assert df.count() == 0
    assert df.inputFiles() == []


def test_input_files_honors_path_glob_filter(spark, tmp_path):
    """The binary ``pathGlobFilter`` restricts the files that compose the dataset."""
    (tmp_path / "keep.png").write_bytes(b"\x89PNG")
    (tmp_path / "drop.pdf").write_bytes(b"%PDF")
    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.png").load(str(tmp_path))

    files = df.inputFiles()
    assert len(files) == 1
    assert files[0].endswith("keep.png")
