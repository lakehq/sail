import pytest
from pyspark.sql import functions as F  # noqa: N812


@pytest.mark.parametrize("binary_inputs", [False, True])
def test_aes_encrypt_column_arguments(spark, binary_inputs):
    df = spark.createDataFrame(
        [
            ("hello", "abcdefghijklmnop", "ECB", "PKCS", "", ""),
            ("world", "1234567890abcdef", "CBC", "DEFAULT", "1234567890123456", ""),
            ("foo", "fedcba0987654321", "GCM", "NONE", "123456789012", "tag-a"),
            ("bar", "0123456789abcdef", "GCM", "DEFAULT", "abcdefghijkl", "tag-b"),
        ],
        ["input", "key", "mode", "padding", "iv", "aad"],
    )
    if binary_inputs:
        for name in ["input", "key", "iv", "aad"]:
            df = df.withColumn(name, F.col(name).cast("binary"))

    actual = (
        df.select(
            "*",
            F.hex(F.aes_encrypt("input", "key", "mode", "padding", "iv", "aad")).alias("encrypted"),
        )
        .orderBy("input")
        .collect()
    )
    assert [row.encrypted for row in actual] == [
        "6162636465666768696A6B6C87B30EF120A58CAAF19575DB6C31F2FAAA60C9",
        "313233343536373839303132DF04DA52F624B2D606D361386DFEE900858833",
        "0805FFCC36FF55DBBFD3ADDAEDF13EA9",
        "313233343536373839303132333435364983708A8C04F73C2795EBDE50CE5B9C",
    ]
    assert df.limit(0).select(F.aes_encrypt("input", "key", "mode", "padding", "iv", "aad")).collect() == []


@pytest.mark.parametrize("argument", ["input", "key", "mode", "padding", "iv", "aad"])
def test_aes_encrypt_null_argument(spark, argument):
    names = ["input", "key", "mode", "padding", "iv", "aad"]
    null_row = ["foo", "fedcba0987654321", "GCM", "NONE", "123456789012", "tag-a"]
    null_row[names.index(argument)] = None
    df = spark.createDataFrame(
        [
            (0, *null_row),
            (1, "bar", "0123456789abcdef", "GCM", "DEFAULT", "abcdefghijkl", "tag-b"),
        ],
        ["id", *names],
    )
    actual = df.select("id", F.hex(F.aes_encrypt(*names)).alias("encrypted")).orderBy("id").collect()
    assert [row.encrypted for row in actual] == [
        None,
        "6162636465666768696A6B6C87B30EF120A58CAAF19575DB6C31F2FAAA60C9",
    ]


@pytest.mark.parametrize("mode", ["GCM", "CBC", "ECB"])
def test_aes_encrypt_defaults(spark, mode):
    data = [("hello", "abcdefghijklmnop"), ("world", "abcdefghijklmnop12345678")]
    df = spark.createDataFrame(data, ["input", "key"])
    # Exercise omitted mode, padding, IV, and AAD through the SQL and Python APIs.
    sql_args = "input, key" if mode == "GCM" else f"input, key, '{mode}'"
    py_mode = None if mode == "GCM" else F.lit(mode)
    for encrypted in [
        df.select("input", F.aes_encrypt("input", "key", py_mode).alias("encrypted")).orderBy("input").collect(),
        df.selectExpr("input", f"aes_encrypt({sql_args}) AS encrypted").orderBy("input").collect(),
    ]:
        for (text, key), row in zip(data, encrypted, strict=True):
            ciphertext = bytes(row.encrypted).hex()
            decrypted = spark.sql(
                f"SELECT CAST(aes_decrypt(unhex('{ciphertext}'), '{key}', '{mode}') AS STRING) AS decrypted"
            ).first()
            assert decrypted.decrypted == text
        if mode != "ECB":
            iv_length = 12 if mode == "GCM" else 16
            assert encrypted[0].encrypted[:iv_length] != encrypted[1].encrypted[:iv_length]


@pytest.mark.parametrize("decrypt", [F.aes_decrypt, F.try_aes_decrypt])
@pytest.mark.parametrize("binary_inputs", [False, True])
def test_aes_decrypt_column_arguments(spark, decrypt, binary_inputs):
    names = ["input", "key", "mode", "padding", "aad"]
    df = spark.createDataFrame(
        [
            ("0805FFCC36FF55DBBFD3ADDAEDF13EA9", "abcdefghijklmnop", "ECB", "PKCS", ""),
            (
                "313233343536373839303132333435364983708A8C04F73C2795EBDE50CE5B9C",
                "1234567890abcdef",
                "CBC",
                "DEFAULT",
                "",
            ),
            (
                "313233343536373839303132DF04DA52F624B2D606D361386DFEE900858833",
                "fedcba0987654321",
                "GCM",
                "NONE",
                "tag-a",
            ),
            (
                "6162636465666768696A6B6C87B30EF120A58CAAF19575DB6C31F2FAAA60C9",
                "0123456789abcdef",
                "GCM",
                "DEFAULT",
                "tag-b",
            ),
        ],
        names,
    ).withColumn("input", F.unhex("input"))
    if binary_inputs:
        for name in ["key", "aad"]:
            df = df.withColumn(name, F.col(name).cast("binary"))
    actual = df.select(decrypt(*names).cast("string").alias("decrypted")).orderBy("decrypted").collect()
    assert [row.decrypted for row in actual] == ["bar", "foo", "hello", "world"]
    assert df.limit(0).select(decrypt(*names)).collect() == []


@pytest.mark.parametrize("decrypt", [F.aes_decrypt, F.try_aes_decrypt])
@pytest.mark.parametrize("argument", ["input", "key", "mode", "padding", "aad"])
def test_aes_decrypt_null_argument(spark, decrypt, argument):
    names = ["input", "key", "mode", "padding", "aad"]
    row = [
        "6162636465666768696A6B6C87B30EF120A58CAAF19575DB6C31F2FAAA60C9",
        "0123456789abcdef",
        "GCM",
        "DEFAULT",
        "tag-b",
    ]
    null_row = row.copy()
    null_row[names.index(argument)] = None
    df = spark.createDataFrame([(0, *null_row), (1, *row)], ["id", *names]).withColumn("input", F.unhex("input"))
    actual = df.select("id", decrypt(*names).cast("string").alias("decrypted")).orderBy("id").collect()
    assert [row.decrypted for row in actual] == [None, "bar"]


@pytest.mark.parametrize(
    ("argument", "invalid"),
    [("input", "00"), ("key", "short"), ("mode", "UNKNOWN"), ("padding", "PKCS"), ("aad", "wrong")],
)
def test_try_aes_decrypt_preserves_valid_rows(spark, argument, invalid):
    names = ["input", "key", "mode", "padding", "aad"]
    row = [
        "6162636465666768696A6B6C87B30EF120A58CAAF19575DB6C31F2FAAA60C9",
        "0123456789abcdef",
        "GCM",
        "DEFAULT",
        "tag-b",
    ]
    invalid_row = row.copy()
    invalid_row[names.index(argument)] = invalid
    df = spark.createDataFrame([(0, *row), (1, *invalid_row), (2, *row)], ["id", *names]).withColumn(
        "input", F.unhex("input")
    )
    with pytest.raises(Exception, match=r"(?i)(aes|gcm)"):
        df.select(F.aes_decrypt(*names)).collect()
    actual = df.select("id", F.try_aes_decrypt(*names).cast("string").alias("decrypted")).orderBy("id").collect()
    assert [row.decrypted for row in actual] == ["bar", None, "bar"]
