"""`hex` over the Arrow storage shapes of one Spark type.

The SQL `.feature` suite builds its inputs from literals and `VALUES`, which Sail stores as plain
`Utf8`/`Int32`/`Float64`. A column read from a file or from a partitioned table can come back in
another Arrow shape of the SAME Spark type (a dictionary-encoded string is the common one), and a
function whose signature lists Arrow types one by one rejects it. Spark has a single STRING type, so
every shape must give the same answer.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def _read(spark, tmp_path, table):
    path = str(tmp_path / "data.parquet")
    pq.write_table(table, path)
    return spark.read.parquet(path)


@pytest.fixture(params=["true", "false"], ids=["ansi_on", "ansi_off"])
def ansi(spark, request):
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", request.param)
    yield request.param
    spark.conf.set("spark.sql.ansi.enabled", previous)


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        pytest.param(["a", "bb", "a", None], ["61", "6262", "61", None], id="string"),
        pytest.param(
            ["niño", "😀", "niño", None], ["6E69C3B16F", "F09F9880", "6E69C3B16F", None], id="string_multibyte"
        ),
        pytest.param([1, 255, 1, None], ["1", "FF", "1", None], id="bigint"),
        pytest.param([1.5, 255.9, 1.5, None], ["1", "FF", "1", None], id="double"),
    ],
)
def test_dictionary_encoded_column(spark, tmp_path, ansi, values, expected):  # noqa: ARG001
    df = pd.DataFrame({"n": [1, 2, 3, 4], "c": pd.Categorical(values)})
    rows = _read(spark, tmp_path, pa.Table.from_pandas(df)).selectExpr("n", "hex(c) AS h").orderBy("n").collect()
    assert [r["h"] for r in rows] == expected


def test_dictionary_encoded_binary_column(spark, tmp_path, ansi):  # noqa: ARG001
    table = pa.table({"n": pa.array([1, 2, 3]), "b": pa.array([b"\x00\xff", b"ab", b"\x00\xff"]).dictionary_encode()})
    rows = _read(spark, tmp_path, table).selectExpr("n", "hex(b) AS h").orderBy("n").collect()
    assert [r["h"] for r in rows] == ["00FF", "6162", "00FF"]


def test_partition_column(spark, tmp_path, ansi):  # noqa: ARG001
    path = str(tmp_path / "part")
    spark.range(4).selectExpr("id", "IF(id % 2 = 0, 'a', 'bb') AS p").write.partitionBy("p").parquet(path)
    rows = spark.read.parquet(path).selectExpr("id", "hex(p) AS h").orderBy("id").collect()
    assert [r["h"] for r in rows] == ["61", "6262", "61", "6262"]


# `unhex`, `to_binary` and `try_to_binary` decode the same text as `hex` encodes, so they must also
# accept every Arrow shape of a STRING or a BINARY. Expected values were measured on the Spark JVM.
DECODE_VALUES = ["41", "ZZ", None, "", "abc", "4142"]
DECODED = ["41", None, None, "", "0ABC", "4142"]


@pytest.mark.parametrize("call", ["unhex(c)", "try_to_binary(c)", "try_to_binary(c, 'hex')"])
def test_dictionary_encoded_column_is_decoded(spark, tmp_path, ansi, call):  # noqa: ARG001
    df = pd.DataFrame({"n": list(range(6)), "c": pd.Categorical(DECODE_VALUES)})
    rows = _read(spark, tmp_path, pa.Table.from_pandas(df)).selectExpr("n", f"hex({call}) AS h").orderBy("n").collect()
    assert [r["h"] for r in rows] == DECODED


def test_dictionary_encoded_column_is_decoded_strictly(spark, tmp_path, ansi):  # noqa: ARG001
    df = pd.DataFrame({"n": list(range(6)), "c": pd.Categorical(DECODE_VALUES)})
    valid = _read(spark, tmp_path, pa.Table.from_pandas(df)).where("n IN (0, 2, 3, 5)")
    rows = valid.selectExpr("n", "hex(to_binary(c, 'hex')) AS h").orderBy("n").collect()
    assert [r["h"] for r in rows] == ["41", None, "", "4142"]


def test_dictionary_encoded_column_with_a_bad_value_raises_when_strict(spark, tmp_path, ansi):  # noqa: ARG001
    df = pd.DataFrame({"n": [1, 2], "c": pd.Categorical(["41", "ZZ"])})
    with pytest.raises(Exception, match="CONVERSION_INVALID_INPUT"):
        _read(spark, tmp_path, pa.Table.from_pandas(df)).selectExpr("hex(to_binary(c)) AS h").collect()


def test_dictionary_encoded_binary_column_is_decoded(spark, tmp_path, ansi):  # noqa: ARG001
    table = pa.table({"n": pa.array([1, 2, 3]), "b": pa.array([b"41", b"4142", b"41"]).dictionary_encode()})
    rows = _read(spark, tmp_path, table).selectExpr("n", "hex(unhex(b)) AS h").orderBy("n").collect()
    assert [r["h"] for r in rows] == ["41", "4142", "41"]


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        pytest.param([1.5, 255.9, 1.5, None], ["312E35", "3235352E39", "312E35", None], id="double"),
        pytest.param([1, 255, 1, None], ["31", "323535", "31", None], id="bigint"),
    ],
)
def test_dictionary_encoded_column_is_encoded_as_utf8(spark, tmp_path, ansi, values, expected):  # noqa: ARG001
    df = pd.DataFrame({"n": [1, 2, 3, 4], "c": pd.Categorical(values)})
    rows = (
        _read(spark, tmp_path, pa.Table.from_pandas(df))
        .selectExpr("n", "hex(to_binary(c, 'utf-8')) AS h")
        .orderBy("n")
        .collect()
    )
    assert [r["h"] for r in rows] == expected


def test_partition_column_is_decoded(spark, tmp_path, ansi):  # noqa: ARG001
    path = str(tmp_path / "part")
    spark.range(4).selectExpr("id", "IF(id % 2 = 0, '41', '4142') AS p").write.partitionBy("p").parquet(path)
    rows = spark.read.parquet(path).selectExpr("id", "hex(unhex(p)) AS h").orderBy("id").collect()
    assert [r["h"] for r in rows] == ["41", "4142", "41", "4142"]


def test_plain_string_and_binary_columns(spark, tmp_path, ansi):  # noqa: ARG001
    table = pa.table({"s": ["41", "ZZ"], "b": [b"41", b"ZZ"]})
    path = str(tmp_path / "plain.parquet")
    pq.write_table(table, path, use_dictionary=False)
    rows = (
        spark.read.parquet(path)
        .selectExpr("hex(s) AS a", "hex(unhex(s)) AS b", "hex(try_to_binary(s)) AS c", "hex(unhex(b)) AS d")
        .orderBy("a")
        .collect()
    )
    assert [tuple(r) for r in rows] == [("3431", "41", "41", "41"), ("5A5A", None, None, None)]


# A limit with an offset hands the function a slice of the batch: the rows it sees do not start at
# the first row of the buffers. Both a plain and a dictionary-encoded column are read from Parquet.
@pytest.mark.parametrize("dictionary", [False, True], ids=["plain", "dictionary"])
@pytest.mark.parametrize("kind", ["string", "binary"])
def test_sliced_column_is_hexed_and_decoded(spark, tmp_path, ansi, dictionary, kind):  # noqa: ARG001
    values = [b"a", b"bb", None, b"c"] if kind == "binary" else ["a", "bb", None, "c"]
    table = pa.table({"n": pa.array([1, 2, 3, 4]), "c": pa.array(values)})
    path = str(tmp_path / "data.parquet")
    pq.write_table(table, path, use_dictionary=dictionary)
    spark.read.parquet(path).createOrReplaceTempView("sliced")
    rows = spark.sql(
        "SELECT hex(c) AS h FROM (SELECT n, c FROM sliced ORDER BY n LIMIT 3 OFFSET 1) ORDER BY n"
    ).collect()
    assert [r["h"] for r in rows] == ["6262", None, "63"]


@pytest.mark.parametrize("dictionary", [False, True], ids=["plain", "dictionary"])
@pytest.mark.parametrize("call", ["unhex(c)", "try_to_binary(c)", "try_to_binary(c, 'hex')"])
def test_sliced_column_is_decoded_leniently(spark, tmp_path, ansi, dictionary, call):  # noqa: ARG001
    table = pa.table({"n": pa.array([1, 2, 3, 4]), "c": pa.array(["41", "ZZ", None, "4142"])})
    path = str(tmp_path / "data.parquet")
    pq.write_table(table, path, use_dictionary=dictionary)
    spark.read.parquet(path).createOrReplaceTempView("sliced_hex")
    rows = spark.sql(
        f"SELECT hex({call}) AS h FROM (SELECT n, c FROM sliced_hex ORDER BY n LIMIT 3 OFFSET 1) ORDER BY n"  # noqa: S608
    ).collect()
    assert [r["h"] for r in rows] == [None, None, "4142"]
