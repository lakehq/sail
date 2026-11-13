from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import unquote, urlparse
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.testing.spark.utils.sql import escape_sql_string_literal

VALIDATE_PARTITIONS = "spark.sql.sources.validatePartitionColumns"
ANSI = "spark.sql.ansi.enabled"
TYPED_ALIAS_FILTERS = [
    ("p = 1 AND q = 2", [0, 1, 2, 3]),
    ("p IS NULL", [5]),
    ("p <=> NULL", [5]),
    ("p = 1 OR q = 2", [0, 1, 2, 3, 5, 6]),
    ("NOT (p = 1 AND q = 2)", [4]),
    ("p = 1 AND q IS NULL", [6]),
    ("p <> 1", [4]),
]


@pytest.fixture
def partition_settings(spark):
    previous = {key: spark.conf.get(key, None) for key in [VALIDATE_PARTITIONS, ANSI]}

    def configure(validate, *, ansi=True):
        if validate is None:
            spark.conf.unset(VALIDATE_PARTITIONS)
        else:
            spark.conf.set(VALIDATE_PARTITIONS, str(validate).lower())
        spark.conf.set(ANSI, str(ansi).lower())

    try:
        yield configure
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


def seed_partitions(path, directories):
    for rid, directory in enumerate(directories):
        parent = path / directory
        parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"rid": pa.array([rid], type=pa.int32())}), parent / f"{rid}.parquet")
    return file_bytes(path)


def file_bytes(path):
    return {file.relative_to(path): file.read_bytes() for file in path.rglob("*") if file.is_file()}


@contextmanager
def typed_read(spark, path, entry):
    if entry == "path":
        yield spark.read.schema("rid INT, p INT, q INT").parquet(str(path))
    else:
        name = f"partition_validation_{uuid4().hex}"
        try:
            # A fresh file-backed catalog relation lets discovery use the current session setting.
            spark.sql(
                f"CREATE TABLE {name} (rid INT, p INT, q INT) USING parquet "
                f"LOCATION '{escape_sql_string_literal(str(path))}'"
            )
            yield spark.table(name)
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("explicit_schema", [False, True], ids=["inferred", "explicit"])
@pytest.mark.parametrize("multiple_columns", [False, True], ids=["single-key", "multiple-keys"])
@pytest.mark.parametrize(
    "spellings",
    [
        ("p=keep",),
        ("%70=keep",),
        ("p=%6Beep",),
        ("p=%6beep",),
        ("p=keep", "%70=keep", "p=%6Beep", "p=%6beep"),
    ],
    ids=["canonical", "encoded-name", "encoded-value-upper", "encoded-value-lower", "mixed"],
)
def test_partition_filters_match_decoded_directory_aliases(
    spark, tmp_path, explicit_schema, multiple_columns, spellings
):
    # Spark 4.3 accepts these spellings as the same decoded partition identity.
    path = tmp_path / "data"
    expected = []
    layouts = [(spelling, "keep", "selected") for spelling in spellings] + [("p=other", "other", "selected")]
    if multiple_columns:
        layouts += [("p=keep", "keep", "excluded")]
    for rid, (directory, partition, second) in enumerate(layouts):
        parent = path / directory
        if multiple_columns:
            parent /= f"q={second}"
        parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"rid": pa.array([rid], type=pa.int32())}), parent / f"{rid}.parquet")
        expected.append((rid, partition, second) if multiple_columns else (rid, partition))

    before = {file.relative_to(path): file.read_bytes() for file in path.rglob("*.parquet")}
    reader = spark.read
    if explicit_schema:
        reader = reader.schema("rid INT, p STRING" + (", q STRING" if multiple_columns else ""))
    frame = reader.parquet(str(path))
    columns = ["rid", "p", "q"] if multiple_columns else ["rid", "p"]
    assert Counter(tuple(row) for row in frame.select(*columns).collect()) == Counter(expected)
    predicate = "p = 'keep'" + (" AND q = 'selected'" if multiple_columns else "")
    selected = [row for row in expected if row[1] == "keep" and (not multiple_columns or row[2] == "selected")]
    assert Counter(tuple(row) for row in frame.where(predicate).select(*columns).collect()) == Counter(selected)
    assert {file.relative_to(path): file.read_bytes() for file in path.rglob("*.parquet")} == before


@pytest.mark.parametrize("entry", ["path", "catalog"])
@pytest.mark.parametrize("ansi", [True, False])
@pytest.mark.parametrize(
    ("bad_directory", "bad_values"),
    [
        ("p=1/q=invalid", (1, None)),
        ("p=9/q=invalid", (9, None)),
        ("p=invalid/q=3", (None, 3)),
        ("p=__HIVE_DEFAULT_PARTITION__/q=invalid", (None, None)),
    ],
    ids=["included", "excluded", "leading", "null-leading"],
)
def test_partition_validation_session_toggles(
    spark, tmp_path, partition_settings, entry, ansi, bad_directory, bad_values
):
    # Spark 4.3 FileIndexSuite SPARK-26263: validation, not ANSI, controls null-on-bad-cast.
    path = tmp_path / "data"
    before = seed_partitions(path, ["p=1/q=2", bad_directory])
    expected = [(0, 1, 2), (1, *bad_values)]
    for validate in [None, False, True, False]:
        partition_settings(validate, ansi=ansi)
        if validate is not False:
            # Construct within raises: Spark fails earlier than Sail's physical planning.
            with (
                pytest.raises(Exception, match=r"(?i)cast|invalid.partition|invalid.*value"),
                typed_read(spark, path, entry) as frame,
            ):
                frame.where("p = 1").limit(1).collect()
            with (
                pytest.raises(Exception, match=r"(?i)cast|invalid.partition|invalid.*value"),
                typed_read(spark, path, entry) as frame,
            ):
                frame.limit(1).collect()
        else:
            with typed_read(spark, path, entry) as frame:
                assert Counter(tuple(row) for row in frame.select("rid", "p", "q").collect()) == Counter(expected)
                assert frame.schema["p"].nullable
                assert frame.schema["q"].nullable
            for predicate, selected in [
                ("q IS NULL", [row for row in expected if row[2] is None]),
                ("p <=> NULL", [row for row in expected if row[1] is None]),
                ("p = 1 OR q = 3", [row for row in expected if row[1] == 1 or row[2] == 3]),  # noqa: PLR2004
                ("NOT (p = 1)", [row for row in expected if row[1] is not None and row[1] != 1]),
            ]:
                with typed_read(spark, path, entry) as frame:
                    assert Counter(
                        tuple(row) for row in frame.where(predicate).select("rid", "p", "q").collect()
                    ) == Counter(selected)
        assert file_bytes(path) == before


@pytest.mark.parametrize("entry", ["path", "catalog"])
@pytest.mark.parametrize(("predicate", "ids"), TYPED_ALIAS_FILTERS)
def test_typed_aliases_at_both_levels_and_three_valued_filters(
    spark, tmp_path, partition_settings, entry, predicate, ids
):
    partition_settings(None)
    path = tmp_path / "data"
    before = seed_partitions(
        path,
        [
            "p=1/q=2",
            "%70=1/%71=2",
            "p=1/%71=2",
            "%70=1/q=2",
            "p=9/q=3",
            "p=__HIVE_DEFAULT_PARTITION__/q=2",
            "p=1/q=__HIVE_DEFAULT_PARTITION__",
        ],
    )
    with typed_read(spark, path, entry) as frame:
        assert sorted(row.rid for row in frame.where(predicate).select("rid").collect()) == ids
    assert file_bytes(path) == before


@pytest.mark.spark(
    skip_if_jvm=True,
    reason="Sail decodes numeric partition-value aliases; Spark 4.3 casts raw encoded numeric text",
)
@pytest.mark.parametrize("entry", ["path", "catalog"])
@pytest.mark.parametrize(("predicate", "ids"), TYPED_ALIAS_FILTERS)
def test_sail_numeric_partition_value_alias_extension(spark, tmp_path, partition_settings, entry, predicate, ids):
    partition_settings(None)
    path = tmp_path / "data"
    before = seed_partitions(
        path,
        [
            "p=1/q=2",
            "%70=1/%71=2",
            "p=%31/q=%32",
            "%70=%31/%71=%32",
            "p=9/q=3",
            "p=__HIVE_DEFAULT_PARTITION__/q=2",
            "p=1/q=__HIVE_DEFAULT_PARTITION__",
        ],
    )
    with typed_read(spark, path, entry) as frame:
        assert sorted(row.rid for row in frame.where(predicate).select("rid").collect()) == ids
    assert file_bytes(path) == before


@pytest.mark.parametrize("validate", [True, False])
def test_explicit_nonnullable_hive_fields_become_nullable(spark, tmp_path, partition_settings, validate):
    partition_settings(validate)
    path = tmp_path / "data"
    before = seed_partitions(path, ["p=1/q=2", "p=__HIVE_DEFAULT_PARTITION__/q=__HIVE_DEFAULT_PARTITION__"])
    schema = StructType(
        [
            StructField("rid", IntegerType(), True),
            StructField("p", IntegerType(), False),
            StructField("q", IntegerType(), False),
        ]
    )
    frame = spark.read.schema(schema).parquet(str(path))
    assert frame.schema == StructType([StructField(field.name, field.dataType, True) for field in schema])
    assert Counter(tuple(row) for row in frame.select("rid", "p", "q").collect()) == Counter(
        [(0, 1, 2), (1, None, None)]
    )
    assert file_bytes(path) == before


@pytest.mark.parametrize("validate", [True, False])
def test_validation_is_session_only_not_a_reader_option(spark, tmp_path, partition_settings, validate):
    partition_settings(validate)
    path = tmp_path / "data"
    before = seed_partitions(path, ["p=1/q=invalid"])
    reader = spark.read.schema("rid INT, p INT, q INT").option("validatePartitionColumns", str(not validate).lower())
    if validate:
        with pytest.raises(Exception, match=r"(?i)cast|invalid.partition|invalid.*value"):
            reader.parquet(str(path)).collect()
    else:
        assert reader.parquet(str(path)).select("rid", "p", "q").collect() == [(0, 1, None)]
    assert file_bytes(path) == before


@pytest.mark.parametrize("validate", [None, True, False])
def test_inferred_partition_types_remain_independent_of_validation(spark, tmp_path, partition_settings, validate):
    partition_settings(validate)
    path = tmp_path / "data"
    before = seed_partitions(path, ["p=1/q=2", "p=9/q=invalid"])
    frame = spark.read.parquet(str(path))
    # Sail intentionally still infers STRING partition fields; validation is not type inference.
    p_type = IntegerType() if is_jvm_spark() else StringType()
    assert frame.schema["p"].dataType == p_type
    assert frame.schema["q"].dataType == StringType()
    assert frame.schema["p"].nullable
    assert frame.schema["q"].nullable
    expected = [(0, 1, "2"), (1, 9, "invalid")] if is_jvm_spark() else [(0, "1", "2"), (1, "9", "invalid")]
    assert Counter(tuple(row) for row in frame.select("rid", "p", "q").collect()) == Counter(expected)
    assert file_bytes(path) == before


@pytest.mark.parametrize("extra", ["empty", "hidden-only", "incomplete", "zero-size"])
@pytest.mark.parametrize("glob", [None, "0.parquet"])
def test_partition_listing_file_membership_and_input_files(spark, tmp_path, partition_settings, extra, glob):
    partition_settings(None)
    path = tmp_path / "data"
    seed_partitions(path, ["p=1/q=2", "p=9/q=3"])
    extra_dir = path / ("p=9" if extra == "incomplete" else "p=9/q=invalid")
    extra_dir.mkdir(parents=True, exist_ok=True)
    if extra in {"hidden-only", "incomplete"}:
        (extra_dir / "_hidden.parquet").write_bytes(b"not parquet")
    elif extra == "zero-size":
        # Zero-size files remain in inputFiles but contribute no rows. Keep a valid key so
        # this isolates file membership from partition-value validation in Spark.
        (path / "p=9/q=3/zero.parquet").touch()
    before = file_bytes(path)
    reader = spark.read.schema("rid INT, p INT, q INT").option("ignoreCorruptFiles", "true")
    if glob:
        reader = reader.option("pathGlobFilter", glob)
    frame = reader.parquet(str(path))
    expected = [(0, 1, 2)] if glob else [(0, 1, 2), (1, 9, 3)]
    assert Counter(tuple(row) for row in frame.select("rid", "p", "q").collect()) == Counter(expected)
    assert frame.where("p = 1").select("rid").collect() == [(0,)]
    actual_files = {Path(unquote(urlparse(file).path)).relative_to(path) for file in frame.inputFiles()}
    expected_files = {Path("p=1/q=2/0.parquet")}
    if not glob:
        expected_files.add(Path("p=9/q=3/1.parquet"))
        if extra == "zero-size":
            expected_files.add(Path("p=9/q=3/zero.parquet"))
    assert actual_files == expected_files
    assert {
        Path(unquote(urlparse(file).path)).relative_to(path) for file in frame.where("p = 1").inputFiles()
    } == expected_files
    assert file_bytes(path) == before


@pytest.mark.parametrize("validate", [True, False])
@pytest.mark.parametrize("member", ["empty", "hidden-only", "file", "nonmatching-file"])
@pytest.mark.parametrize(
    ("schema", "bad_directory", "predicate"),
    [
        ("rid INT, p STRING, q INT", "p=bad/q=3", "CAST(p AS INT) = 1"),
        ("rid INT, p INT, q STRING", "p=1/q=bad", "p = 1 AND CAST(q AS INT) = 2"),
    ],
    ids=["root-cast", "suffix-cast"],
)
def test_partition_predicate_errors_require_data_membership(
    spark, tmp_path, partition_settings, validate, member, schema, bad_directory, predicate
):
    partition_settings(validate, ansi=True)
    path = tmp_path / "data"
    seed_partitions(path, ["p=1/q=2", "p=9/q=3"])
    bad = path / bad_directory
    bad.mkdir(parents=True)
    if member == "hidden-only":
        (bad / "_hidden.parquet").write_bytes(b"not parquet")
    elif member in {"file", "nonmatching-file"}:
        (bad / "bad.parquet").write_bytes((path / "p=1/q=2/0.parquet").read_bytes())
    before = file_bytes(path)
    reader = spark.read.schema(schema)
    if member == "nonmatching-file":
        reader = reader.option("pathGlobFilter", "0.parquet")
    if member in {"file", "nonmatching-file"}:
        with pytest.raises(Exception, match=r"(?i)cast|invalid"):
            reader.parquet(str(path)).where(predicate).select("rid").collect()
    else:
        assert reader.parquet(str(path)).where(predicate).select("rid").collect() == [(0,)]
    assert file_bytes(path) == before


@pytest.mark.parametrize("validate", [None, True, False], ids=["default", "strict", "relaxed"])
@pytest.mark.parametrize("predicate", ["TRUE", "p = 1"], ids=["unfiltered", "partition-filtered"])
def test_filename_glob_does_not_hide_invalid_typed_partitions(spark, tmp_path, partition_settings, validate, predicate):
    partition_settings(validate)
    path = tmp_path / "data"
    before = seed_partitions(path, ["p=1/q=2", "p=9/q=invalid"])
    reader = spark.read.schema("rid INT, p INT, q INT").option("pathGlobFilter", "0.parquet")
    if validate is not False:
        with pytest.raises(Exception, match=r"(?i)cast|invalid.partition|invalid.*value"):
            reader.parquet(str(path)).where(predicate).collect()
    else:
        frame = reader.parquet(str(path)).where(predicate)
        assert frame.select("rid", "p", "q").collect() == [(0, 1, 2)]
        assert {Path(unquote(urlparse(file).path)).relative_to(path) for file in frame.inputFiles()} == {
            Path("p=1/q=2/0.parquet")
        }
    assert file_bytes(path) == before
