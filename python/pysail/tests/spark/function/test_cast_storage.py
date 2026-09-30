"""CAST values and schemas across computed columns and Parquet storage.

Expectations measured with Spark 4.2 Connect and UTC in both session and client.
"""

import json

import pytest

from pysail.testing.spark.utils.common import pyspark_version

CASES = [
    (
        "variant_date",
        "values",
        "SELECT CAST(DATE '2024-03-05' AS VARIANT) v",
        "CAST(v AS STRING)",
        [["2024-03-05"]],
        "root\n |-- result: string (nullable = true)\n",
    ),
    (
        "variant_date",
        "parquet",
        "SELECT CAST(DATE '2024-03-05' AS VARIANT) v",
        "CAST(v AS STRING)",
        [["2024-03-05"]],
        "root\n |-- result: string (nullable = true)\n",
    ),
    (
        "variant_timestamp",
        "values",
        "SELECT CAST(TIMESTAMP '2024-03-05 06:07:08' AS VARIANT) v",
        "CAST(v AS STRING)",
        [["2024-03-05 06:07:08"]],
        "root\n |-- result: string (nullable = true)\n",
    ),
    (
        "variant_timestamp",
        "parquet",
        "SELECT CAST(TIMESTAMP '2024-03-05 06:07:08' AS VARIANT) v",
        "CAST(v AS STRING)",
        [["2024-03-05 06:07:08"]],
        "root\n |-- result: string (nullable = true)\n",
    ),
    (
        "variant_binary",
        "values",
        "SELECT CAST(X'616263' AS VARIANT) v",
        "CAST(v AS STRING)",
        [["abc"]],
        "root\n |-- result: string (nullable = true)\n",
    ),
    (
        "variant_binary",
        "parquet",
        "SELECT CAST(X'616263' AS VARIANT) v",
        "CAST(v AS STRING)",
        [["abc"]],
        "root\n |-- result: string (nullable = true)\n",
    ),
    (
        "variant_timestamp_value",
        "values",
        "SELECT parse_json('1') v",
        "CAST(v AS TIMESTAMP)",
        [["1970-01-01 00:00:01"]],
        "root\n |-- result: timestamp (nullable = true)\n",
    ),
    (
        "variant_timestamp_value",
        "parquet",
        "SELECT parse_json('1') v",
        "CAST(v AS TIMESTAMP)",
        [["1970-01-01 00:00:01"]],
        "root\n |-- result: timestamp (nullable = true)\n",
    ),
    (
        "variant_array",
        "values",
        "SELECT parse_json('[1,2,null]') v",
        "CAST(v AS ARRAY<INT>)",
        [[[1, 2, None]]],
        "root\n |-- result: array (nullable = true)\n |    |-- element: integer (containsNull = true)\n",
    ),
    (
        "variant_array",
        "parquet",
        "SELECT parse_json('[1,2,null]') v",
        "CAST(v AS ARRAY<INT>)",
        [[[1, 2, None]]],
        "root\n |-- result: array (nullable = true)\n |    |-- element: integer (containsNull = true)\n",
    ),
    (
        "variant_struct",
        "values",
        "SELECT parse_json('{\"a\":1}') v",
        "CAST(v AS STRUCT<a:INT>)",
        [[[1]]],
        "root\n |-- result: struct (nullable = true)\n |    |-- a: integer (nullable = true)\n",
    ),
    (
        "variant_struct",
        "parquet",
        "SELECT parse_json('{\"a\":1}') v",
        "CAST(v AS STRUCT<a:INT>)",
        [[[1]]],
        "root\n |-- result: struct (nullable = true)\n |    |-- a: integer (nullable = true)\n",
    ),
    (
        "variant_strings",
        "values",
        "SELECT id, parse_json(v) v FROM VALUES (0, '\"hi\"'), (1, 'null'), (2, '{\"a\":1}'), (3, CAST(NULL AS STRING)) AS t(id,v)",
        "CAST(v AS STRING)",
        [["hi"], [None], ['{"a":1}'], [None]],
        "root\n |-- result: string (nullable = true)\n",
    ),
    (
        "variant_strings",
        "parquet",
        "SELECT id, parse_json(v) v FROM VALUES (0, '\"hi\"'), (1, 'null'), (2, '{\"a\":1}'), (3, CAST(NULL AS STRING)) AS t(id,v)",
        "CAST(v AS STRING)",
        [["hi"], [None], ['{"a":1}'], [None]],
        "root\n |-- result: string (nullable = true)\n",
    ),
    (
        "decimal_column",
        "values",
        "SELECT id, CAST(v AS DECIMAL(38,37)) v FROM VALUES (0, '1'), (1, '-1'), (2, CAST(NULL AS STRING)) AS t(id,v)",
        "CAST(v AS DOUBLE)",
        [[1.0], [-1.0], [None]],
        "root\n |-- result: double (nullable = true)\n",
    ),
    (
        "decimal_column",
        "parquet",
        "SELECT id, CAST(v AS DECIMAL(38,37)) v FROM VALUES (0, '1'), (1, '-1'), (2, CAST(NULL AS STRING)) AS t(id,v)",
        "CAST(v AS DOUBLE)",
        [[1.0], [-1.0], [None]],
        "root\n |-- result: double (nullable = true)\n",
    ),
]


CASES.append(
    (
        "variant_timestamp_mixed",
        "values",
        "SELECT id, parse_json(v) v FROM VALUES (0,'1'), (1,'-1.23456789'), (2,'null'), (3,CAST(NULL AS STRING)), (4,'\"2024-01-02 03:04:05\"'), (5,'9223372036855') t(id,v)",
        "unix_micros(TRY_CAST(v AS TIMESTAMP))",
        [[1000000], [-1234567], [None], [None], [1704164645000000], [None]],
        "root\n |-- result: long (nullable = true)\n",
    )
)

CASES.append(
    (
        "variant_timestamp_mixed",
        "parquet",
        "SELECT id, parse_json(v) v FROM VALUES (0,'1'), (1,'-1.23456789'), (2,'null'), (3,CAST(NULL AS STRING)), (4,'\"2024-01-02 03:04:05\"'), (5,'9223372036855') t(id,v)",
        "unix_micros(TRY_CAST(v AS TIMESTAMP))",
        [[1000000], [-1234567], [None], [None], [1704164645000000], [None]],
        "root\n |-- result: long (nullable = true)\n",
    )
)


CASES.extend(
    [
        (
            "timestamp_seconds",
            "values",
            "SELECT id, timestamp_micros(v) AS v FROM VALUES (0,-1000001L),(1,-1L),(2,0L),(3,1999999L),(4,CAST(NULL AS BIGINT)) AS t(id,v)",
            "CAST(v AS BIGINT)",
            [[-2], [-1], [0], [1], [None]],
            "root\n |-- result: long (nullable = true)\n",
        ),
        (
            "timestamp_seconds",
            "parquet",
            "SELECT id, timestamp_micros(v) AS v FROM VALUES (0,-1000001L),(1,-1L),(2,0L),(3,1999999L),(4,CAST(NULL AS BIGINT)) AS t(id,v)",
            "CAST(v AS BIGINT)",
            [[-2], [-1], [0], [1], [None]],
            "root\n |-- result: long (nullable = true)\n",
        ),
        (
            "time_seconds",
            "values",
            "SELECT * FROM VALUES (0,TIME '00:00:00'),(1,TIME '00:00:01.999999'),(2,TIME '23:59:59.999999'),(3,CAST(NULL AS TIME)) AS t(id,v)",
            "CAST(v AS BIGINT)",
            [[0], [1], [86399], [None]],
            "root\n |-- result: long (nullable = true)\n",
        ),
        (
            "time_seconds",
            "parquet",
            "SELECT * FROM VALUES (0,TIME '00:00:00'),(1,TIME '00:00:01.999999'),(2,TIME '23:59:59.999999'),(3,CAST(NULL AS TIME)) AS t(id,v)",
            "CAST(v AS BIGINT)",
            [[0], [1], [86399], [None]],
            "root\n |-- result: long (nullable = true)\n",
        ),
    ]
)


@pytest.fixture
def cast_time_type(spark, _name):
    if _name != "time_seconds":
        yield
        return
    if pyspark_version() < (4, 2):
        pytest.skip("TIME storage coverage requires Spark 4.2+")
    key = "spark.sql.timeType.enabled"
    previous = spark.conf.get(key, "false")
    spark.conf.set(key, "true")
    try:
        yield
    finally:
        spark.conf.set(key, previous)

@pytest.mark.usefixtures("local_timezone", "cast_time_type")
@pytest.mark.parametrize("local_timezone", ["UTC"], indirect=True)
@pytest.mark.parametrize(
    ("_name", "storage", "source", "expression", "expected_values", "expected_tree"),
    CASES,
    ids=[f"{case[0]}-{case[1]}" for case in CASES],
)
def test_cast_storage(spark, tmp_path, _name, storage, source, expression, expected_values, expected_tree):
    frame = spark.sql(source)
    if storage == "parquet":
        path = str(tmp_path / "data")
        frame.write.parquet(path)
        frame = spark.read.parquet(path)
    if "id" in frame.columns:
        frame = frame.orderBy("id")
    result = frame.selectExpr(f"{expression} AS result")
    assert result.schema.treeString() == expected_tree
    values = json.loads(json.dumps([list(row) for row in result.collect()], default=str))
    assert values == expected_values
