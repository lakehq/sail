"""Every file format is a path of its own, and so is every way of materialising what comes back.

A value a query builds correctly can still lose its type on the way to a file, on the way back, or
only when the rows become Python objects. The lenses are kept apart because they fail apart: Sail
reads `time(0)` back from Parquet with the right schema and breaks only at `collect()`, and it
writes an `INTERVAL DAY` whose qualifier is gone once the file is read again.

Every expectation here was measured on the Spark 4.2 JVM over Spark Connect and is compared as text
(`str(value)`), so one table serves the three materialisers. A combination Spark refuses to write
pins the refusal instead of a value.

These tests write and read with the SAME engine. Reading one engine's files with the other is a
separate matter this harness cannot express, since a run talks to a single engine.
"""

import contextlib
import os
import time

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

_SAIL_BUG = pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
# `TIME` is refused outright unless the flag is on, and the client only knows the type from 4.2.
_NEEDS_TIME = pytest.mark.skipif(pyspark_version() < (4, 2), reason="the TIME type requires Spark 4.2+")
# `DataFrame.toArrow` landed in the PySpark client in 4.0.
_NEEDS_TO_ARROW = pytest.mark.skipif(pyspark_version() < (4,), reason="toArrow requires PySpark 4+")
# The VARIANT type reaches the PySpark client in 4.0; before that the schema comes back as a struct.
_NEEDS_VARIANT = pytest.mark.skipif(pyspark_version() < (4,), reason="VARIANT requires PySpark 4+")


@pytest.fixture(autouse=True)
def _session(spark):
    """Open the TIME type, and put back every setting a test here changes.

    Spark refuses a TIME column with [UNSUPPORTED_TIME_TYPE] unless the flag is on, and the rebase
    tests below set a writer mode that would otherwise leak into whatever runs next.
    """
    keys = ("spark.sql.timeType.enabled", "spark.sql.parquet.datetimeRebaseModeInWrite")
    previous = {}
    for key in keys:
        try:
            previous[key] = spark.conf.get(key)
        except Exception:  # noqa: BLE001
            previous[key] = None
    with contextlib.suppress(Exception):
        spark.conf.set("spark.sql.timeType.enabled", "true")
    yield
    for key, value in previous.items():
        if value is not None:
            with contextlib.suppress(Exception):
                spark.conf.set(key, value)


# One expression per case; every format reads the same value back.
EXPRESSION = {
    "boolean": "true",
    "tinyint": "CAST(1 AS TINYINT)",
    "smallint": "CAST(1 AS SMALLINT)",
    "int": "1",
    "bigint": "1L",
    "float": "CAST(0.1 AS FLOAT)",
    "double": "0.1D",
    "string": "'x'",
    "binary": "CAST('x' AS BINARY)",
    "decimal(10,2)": "CAST(1.5 AS DECIMAL(10,2))",
    "decimal(38,0)": "CAST(1 AS DECIMAL(38,0))",
    "decimal(38,38)": "CAST(0.1 AS DECIMAL(38,38))",
    "decimal(1,1)": "CAST(0.1 AS DECIMAL(1,1))",
    "date": "DATE '2024-03-05'",
    "timestamp": "TIMESTAMP '2024-03-05 06:07:08.9'",
    "timestamp_ntz": "TIMESTAMP_NTZ '2024-03-05 06:07:08.9'",
    "time(0)": "CAST('06:07:08' AS TIME(0))",
    "time(3)": "CAST('06:07:08.123' AS TIME(3))",
    "time(6)": "CAST('06:07:08.123456' AS TIME(6))",
    "interval day": "INTERVAL '5' DAY",
    "interval day to second": "INTERVAL '1 02:03:04.5' DAY TO SECOND",
    "interval year to month": "INTERVAL '3-4' YEAR TO MONTH",
    "interval calendar": "make_interval(0, 1, 0, 2, 3, 0, 0)",
    "array": "array(1, 2)",
    "array with a null": "array('a', NULL)",
    "map": "map('k', 1)",
    "struct": "named_struct('a', 1, 'b', 'x')",
    "struct in an array": "array(named_struct('a', 1))",
    "variant": "parse_json('{\"a\":1}')",
}

# (format, case): what Spark 4.2 gives back, and whether Sail differs.
SCHEMA = {
    ("parquet", "boolean"): ("struct<v:boolean>", False),
    ("json", "boolean"): ("struct<v:boolean>", False),
    ("csv", "boolean"): ("struct<v:string>", False),
    ("parquet", "tinyint"): ("struct<v:tinyint>", False),
    ("json", "tinyint"): ("struct<v:bigint>", False),
    ("csv", "tinyint"): ("struct<v:string>", False),
    ("parquet", "smallint"): ("struct<v:smallint>", False),
    ("json", "smallint"): ("struct<v:bigint>", False),
    ("csv", "smallint"): ("struct<v:string>", False),
    ("parquet", "int"): ("struct<v:int>", False),
    ("json", "int"): ("struct<v:bigint>", False),
    ("csv", "int"): ("struct<v:string>", False),
    ("parquet", "bigint"): ("struct<v:bigint>", False),
    ("json", "bigint"): ("struct<v:bigint>", False),
    ("csv", "bigint"): ("struct<v:string>", False),
    ("parquet", "float"): ("struct<v:float>", False),
    ("json", "float"): ("struct<v:double>", False),
    ("csv", "float"): ("struct<v:string>", False),
    ("parquet", "double"): ("struct<v:double>", False),
    ("json", "double"): ("struct<v:double>", False),
    ("csv", "double"): ("struct<v:string>", False),
    ("parquet", "string"): ("struct<v:string>", False),
    ("json", "string"): ("struct<v:string>", False),
    ("csv", "string"): ("struct<v:string>", False),
    ("parquet", "binary"): ("struct<v:binary>", False),
    ("json", "binary"): ("struct<v:string>", False),
    ("csv", "binary"): ("struct<v:string>", False),
    ("parquet", "decimal(10,2)"): ("struct<v:decimal(10,2)>", False),
    ("json", "decimal(10,2)"): ("struct<v:double>", False),
    ("csv", "decimal(10,2)"): ("struct<v:string>", False),
    ("parquet", "decimal(38,0)"): ("struct<v:decimal(38,0)>", False),
    ("json", "decimal(38,0)"): ("struct<v:bigint>", False),
    ("csv", "decimal(38,0)"): ("struct<v:string>", False),
    ("parquet", "decimal(38,38)"): ("struct<v:decimal(38,38)>", False),
    ("json", "decimal(38,38)"): ("struct<v:double>", False),
    ("csv", "decimal(38,38)"): ("struct<v:string>", False),
    ("parquet", "decimal(1,1)"): ("struct<v:decimal(1,1)>", False),
    ("json", "decimal(1,1)"): ("struct<v:double>", False),
    ("csv", "decimal(1,1)"): ("struct<v:string>", False),
    ("parquet", "date"): ("struct<v:date>", False),
    ("json", "date"): ("struct<v:string>", False),
    ("csv", "date"): ("struct<v:string>", False),
    ("parquet", "timestamp"): ("struct<v:timestamp>", False),
    ("json", "timestamp"): ("struct<v:string>", False),
    ("csv", "timestamp"): ("struct<v:string>", False),
    ("parquet", "timestamp_ntz"): ("struct<v:timestamp_ntz>", False),
    ("json", "timestamp_ntz"): ("struct<v:string>", False),
    ("csv", "timestamp_ntz"): ("struct<v:string>", False),
    ("parquet", "time(0)"): ("struct<v:time(0)>", False),
    ("json", "time(0)"): ("struct<v:string>", False),
    ("csv", "time(0)"): ("struct<v:string>", False),
    ("parquet", "time(3)"): ("struct<v:time(3)>", False),
    ("json", "time(3)"): ("struct<v:string>", False),
    ("csv", "time(3)"): ("struct<v:string>", False),
    ("parquet", "time(6)"): ("struct<v:time(6)>", False),
    ("json", "time(6)"): ("struct<v:string>", False),
    ("csv", "time(6)"): ("struct<v:string>", False),
    ("parquet", "interval day"): ("struct<v:interval day>", True),
    ("json", "interval day"): ("struct<v:string>", False),
    ("csv", "interval day"): ("struct<v:string>", False),
    ("parquet", "interval day to second"): ("struct<v:interval day to second>", False),
    ("json", "interval day to second"): ("struct<v:string>", False),
    ("csv", "interval day to second"): ("struct<v:string>", False),
    ("parquet", "interval year to month"): ("struct<v:interval year to month>", False),
    ("json", "interval year to month"): ("struct<v:string>", False),
    ("csv", "interval year to month"): ("struct<v:string>", False),
    ("parquet", "array"): ("struct<v:array<int>>", False),
    ("json", "array"): ("struct<v:array<bigint>>", False),
    ("parquet", "array with a null"): ("struct<v:array<string>>", False),
    ("json", "array with a null"): ("struct<v:array<string>>", False),
    ("parquet", "map"): ("struct<v:map<string,int>>", False),
    ("json", "map"): ("struct<v:struct<k:bigint>>", False),
    ("parquet", "struct"): ("struct<v:struct<a:int,b:string>>", False),
    ("json", "struct"): ("struct<v:struct<a:bigint,b:string>>", False),
    ("parquet", "struct in an array"): ("struct<v:array<struct<a:int>>>", False),
    ("json", "struct in an array"): ("struct<v:array<struct<a:bigint>>>", False),
    ("parquet", "variant"): ("struct<v:variant>", False),
    ("json", "variant"): ("struct<v:struct<a:bigint>>", True),
}

# (format, case): what Spark 4.2 gives back, and whether Sail differs.
COLLECT = {
    ("parquet", "boolean"): ("True", False),
    ("json", "boolean"): ("True", False),
    ("csv", "boolean"): ("true", False),
    ("parquet", "tinyint"): ("1", False),
    ("json", "tinyint"): ("1", False),
    ("csv", "tinyint"): ("1", False),
    ("parquet", "smallint"): ("1", False),
    ("json", "smallint"): ("1", False),
    ("csv", "smallint"): ("1", False),
    ("parquet", "int"): ("1", False),
    ("json", "int"): ("1", False),
    ("csv", "int"): ("1", False),
    ("parquet", "bigint"): ("1", False),
    ("json", "bigint"): ("1", False),
    ("csv", "bigint"): ("1", False),
    ("parquet", "float"): ("0.10000000149011612", False),
    ("json", "float"): ("0.1", False),
    ("csv", "float"): ("0.1", False),
    ("parquet", "double"): ("0.1", False),
    ("json", "double"): ("0.1", False),
    ("csv", "double"): ("0.1", False),
    ("parquet", "string"): ("x", False),
    ("json", "string"): ("x", False),
    ("csv", "string"): ("x", False),
    ("parquet", "binary"): ("b'x'", False),
    ("json", "binary"): ("eA==", True),
    ("csv", "binary"): ("[78]", True),
    ("parquet", "decimal(10,2)"): ("1.50", False),
    ("json", "decimal(10,2)"): ("1.5", False),
    ("csv", "decimal(10,2)"): ("1.50", False),
    ("parquet", "decimal(38,0)"): ("1", False),
    ("json", "decimal(38,0)"): ("1", False),
    ("csv", "decimal(38,0)"): ("1", False),
    ("parquet", "decimal(38,38)"): ("0.10000000000000000000000000000000000000", False),
    ("json", "decimal(38,38)"): ("0.1", False),
    ("csv", "decimal(38,38)"): ("0.10000000000000000000000000000000000000", False),
    ("parquet", "decimal(1,1)"): ("0.1", False),
    ("json", "decimal(1,1)"): ("0.1", False),
    ("csv", "decimal(1,1)"): ("0.1", False),
    ("parquet", "date"): ("2024-03-05", False),
    ("json", "date"): ("2024-03-05", False),
    ("csv", "date"): ("2024-03-05", False),
    ("parquet", "timestamp"): ("2024-03-05 06:07:08.900000", False),
    ("json", "timestamp"): ("2024-03-05T06:07:08.900Z", False),
    ("csv", "timestamp"): ("2024-03-05T06:07:08.900Z", False),
    ("parquet", "timestamp_ntz"): ("2024-03-05 06:07:08.900000", False),
    ("json", "timestamp_ntz"): ("2024-03-05T06:07:08.900", False),
    ("csv", "timestamp_ntz"): ("2024-03-05T06:07:08.900", False),
    ("parquet", "time(0)"): ("06:07:08", True),
    ("json", "time(0)"): ("06:07:08", False),
    ("csv", "time(0)"): ("06:07:08", False),
    ("parquet", "time(3)"): ("06:07:08.123000", True),
    ("json", "time(3)"): ("06:07:08.123", False),
    ("csv", "time(3)"): ("06:07:08.123", False),
    ("parquet", "time(6)"): ("06:07:08.123456", False),
    ("json", "time(6)"): ("06:07:08.123456", False),
    ("csv", "time(6)"): ("06:07:08.123456", False),
    ("parquet", "interval day"): ("5 days, 0:00:00", False),
    ("json", "interval day"): ("INTERVAL '5' DAY", True),
    ("csv", "interval day"): ("INTERVAL '5' DAY", True),
    ("parquet", "interval day to second"): ("1 day, 2:03:04.500000", False),
    ("json", "interval day to second"): ("INTERVAL '1 02:03:04.5' DAY TO SECOND", True),
    ("csv", "interval day to second"): ("INTERVAL '1 02:03:04.5' DAY TO SECOND", True),
    ("parquet", "interval year to month"): ("!NOT_IMPLEMENTED", True),
    ("json", "interval year to month"): ("INTERVAL '3-4' YEAR TO MONTH", True),
    ("csv", "interval year to month"): ("INTERVAL '3-4' YEAR TO MONTH", True),
    ("parquet", "array"): ("[1, 2]", False),
    ("json", "array"): ("[1, 2]", False),
    ("parquet", "array with a null"): ("['a', None]", False),
    ("json", "array with a null"): ("['a', None]", False),
    ("parquet", "map"): ("{'k': 1}", False),
    ("json", "map"): ("Row(k=1)", False),
    ("parquet", "struct"): ("Row(a=1, b='x')", False),
    ("json", "struct"): ("Row(a=1, b='x')", False),
    ("parquet", "struct in an array"): ("[Row(a=1)]", False),
    ("json", "struct in an array"): ("[Row(a=1)]", False),
    ("parquet", "variant"): ('{"a":1}', False),
    ("json", "variant"): ("Row(a=1)", True),
}

# (format, case): what Spark 4.2 gives back, and whether Sail differs.
TO_ARROW = {
    ("parquet", "boolean"): ("True", False),
    ("json", "boolean"): ("True", False),
    ("csv", "boolean"): ("true", False),
    ("parquet", "tinyint"): ("1", False),
    ("json", "tinyint"): ("1", False),
    ("csv", "tinyint"): ("1", False),
    ("parquet", "smallint"): ("1", False),
    ("json", "smallint"): ("1", False),
    ("csv", "smallint"): ("1", False),
    ("parquet", "int"): ("1", False),
    ("json", "int"): ("1", False),
    ("csv", "int"): ("1", False),
    ("parquet", "bigint"): ("1", False),
    ("json", "bigint"): ("1", False),
    ("csv", "bigint"): ("1", False),
    ("parquet", "float"): ("0.10000000149011612", False),
    ("json", "float"): ("0.1", False),
    ("csv", "float"): ("0.1", False),
    ("parquet", "double"): ("0.1", False),
    ("json", "double"): ("0.1", False),
    ("csv", "double"): ("0.1", False),
    ("parquet", "string"): ("x", False),
    ("json", "string"): ("x", False),
    ("csv", "string"): ("x", False),
    ("parquet", "binary"): ("b'x'", False),
    ("json", "binary"): ("eA==", True),
    ("csv", "binary"): ("[78]", True),
    ("parquet", "decimal(10,2)"): ("1.50", False),
    ("json", "decimal(10,2)"): ("1.5", False),
    ("csv", "decimal(10,2)"): ("1.50", False),
    ("parquet", "decimal(38,0)"): ("1", False),
    ("json", "decimal(38,0)"): ("1", False),
    ("csv", "decimal(38,0)"): ("1", False),
    ("parquet", "decimal(38,38)"): ("0.10000000000000000000000000000000000000", False),
    ("json", "decimal(38,38)"): ("0.1", False),
    ("csv", "decimal(38,38)"): ("0.10000000000000000000000000000000000000", False),
    ("parquet", "decimal(1,1)"): ("0.1", False),
    ("json", "decimal(1,1)"): ("0.1", False),
    ("csv", "decimal(1,1)"): ("0.1", False),
    ("parquet", "date"): ("2024-03-05", False),
    ("json", "date"): ("2024-03-05", False),
    ("csv", "date"): ("2024-03-05", False),
    ("parquet", "timestamp"): ("2024-03-05 06:07:08.900000+00:00", False),
    ("json", "timestamp"): ("2024-03-05T06:07:08.900Z", False),
    ("csv", "timestamp"): ("2024-03-05T06:07:08.900Z", False),
    ("parquet", "timestamp_ntz"): ("2024-03-05 06:07:08.900000", False),
    ("json", "timestamp_ntz"): ("2024-03-05T06:07:08.900", False),
    ("csv", "timestamp_ntz"): ("2024-03-05T06:07:08.900", False),
    ("parquet", "time(0)"): ("06:07:08", False),
    ("json", "time(0)"): ("06:07:08", False),
    ("csv", "time(0)"): ("06:07:08", False),
    ("parquet", "time(3)"): ("06:07:08.123000", False),
    ("json", "time(3)"): ("06:07:08.123", False),
    ("csv", "time(3)"): ("06:07:08.123", False),
    ("parquet", "time(6)"): ("06:07:08.123456", False),
    ("json", "time(6)"): ("06:07:08.123456", False),
    ("csv", "time(6)"): ("06:07:08.123456", False),
    ("parquet", "interval day"): ("5 days, 0:00:00", False),
    ("json", "interval day"): ("INTERVAL '5' DAY", True),
    ("csv", "interval day"): ("INTERVAL '5' DAY", True),
    ("parquet", "interval day to second"): ("1 day, 2:03:04.500000", False),
    ("json", "interval day to second"): ("INTERVAL '1 02:03:04.5' DAY TO SECOND", True),
    ("csv", "interval day to second"): ("INTERVAL '1 02:03:04.5' DAY TO SECOND", True),
    ("parquet", "interval year to month"): ("!UNSUPPORTED_DATA_TYPE_FOR_ARROW_CONVERSION", False),
    ("json", "interval year to month"): ("INTERVAL '3-4' YEAR TO MONTH", True),
    ("csv", "interval year to month"): ("INTERVAL '3-4' YEAR TO MONTH", True),
    ("parquet", "array"): ("[1, 2]", False),
    ("json", "array"): ("[1, 2]", False),
    ("parquet", "array with a null"): ("['a', None]", False),
    ("json", "array with a null"): ("['a', None]", False),
    ("parquet", "map"): ("[('k', 1)]", False),
    ("json", "map"): ("{'k': 1}", False),
    ("parquet", "struct"): ("{'a': 1, 'b': 'x'}", False),
    ("json", "struct"): ("{'a': 1, 'b': 'x'}", False),
    ("parquet", "struct in an array"): ("[{'a': 1}]", False),
    ("json", "struct in an array"): ("[{'a': 1}]", False),
    ("parquet", "variant"): (
        "{'value': b'\\x02\\x01\\x00\\x00\\x02\\x0c\\x01', 'metadata': b'\\x01\\x01\\x00\\x01a'}",
        True,
    ),
    ("json", "variant"): ("{'a': 1}", True),
}

# (format, case): what Spark 4.2 gives back, and whether Sail differs.
TO_PANDAS = {
    ("parquet", "boolean"): ("True", False),
    ("json", "boolean"): ("True", False),
    ("csv", "boolean"): ("true", False),
    ("parquet", "tinyint"): ("1", False),
    ("json", "tinyint"): ("1", False),
    ("csv", "tinyint"): ("1", False),
    ("parquet", "smallint"): ("1", False),
    ("json", "smallint"): ("1", False),
    ("csv", "smallint"): ("1", False),
    ("parquet", "int"): ("1", False),
    ("json", "int"): ("1", False),
    ("csv", "int"): ("1", False),
    ("parquet", "bigint"): ("1", False),
    ("json", "bigint"): ("1", False),
    ("csv", "bigint"): ("1", False),
    ("parquet", "float"): ("0.1", False),
    ("json", "float"): ("0.1", False),
    ("csv", "float"): ("0.1", False),
    ("parquet", "double"): ("0.1", False),
    ("json", "double"): ("0.1", False),
    ("csv", "double"): ("0.1", False),
    ("parquet", "string"): ("x", False),
    ("json", "string"): ("x", False),
    ("csv", "string"): ("x", False),
    ("parquet", "binary"): ("b'x'", False),
    ("json", "binary"): ("eA==", True),
    ("csv", "binary"): ("[78]", True),
    ("parquet", "decimal(10,2)"): ("1.50", False),
    ("json", "decimal(10,2)"): ("1.5", False),
    ("csv", "decimal(10,2)"): ("1.50", False),
    ("parquet", "decimal(38,0)"): ("1", False),
    ("json", "decimal(38,0)"): ("1", False),
    ("csv", "decimal(38,0)"): ("1", False),
    ("parquet", "decimal(38,38)"): ("0.10000000000000000000000000000000000000", False),
    ("json", "decimal(38,38)"): ("0.1", False),
    ("csv", "decimal(38,38)"): ("0.10000000000000000000000000000000000000", False),
    ("parquet", "decimal(1,1)"): ("0.1", False),
    ("json", "decimal(1,1)"): ("0.1", False),
    ("csv", "decimal(1,1)"): ("0.1", False),
    ("parquet", "date"): ("2024-03-05", False),
    ("json", "date"): ("2024-03-05", False),
    ("csv", "date"): ("2024-03-05", False),
    ("parquet", "timestamp"): ("2024-03-05 06:07:08.900000", False),
    ("json", "timestamp"): ("2024-03-05T06:07:08.900Z", False),
    ("csv", "timestamp"): ("2024-03-05T06:07:08.900Z", False),
    ("parquet", "timestamp_ntz"): ("2024-03-05 06:07:08.900000", False),
    ("json", "timestamp_ntz"): ("2024-03-05T06:07:08.900", False),
    ("csv", "timestamp_ntz"): ("2024-03-05T06:07:08.900", False),
    ("parquet", "time(0)"): ("06:07:08", False),
    ("json", "time(0)"): ("06:07:08", False),
    ("csv", "time(0)"): ("06:07:08", False),
    ("parquet", "time(3)"): ("06:07:08.123000", False),
    ("json", "time(3)"): ("06:07:08.123", False),
    ("csv", "time(3)"): ("06:07:08.123", False),
    ("parquet", "time(6)"): ("06:07:08.123456", False),
    ("json", "time(6)"): ("06:07:08.123456", False),
    ("csv", "time(6)"): ("06:07:08.123456", False),
    ("parquet", "interval day"): ("5 days 00:00:00", False),
    ("json", "interval day"): ("INTERVAL '5' DAY", True),
    ("csv", "interval day"): ("INTERVAL '5' DAY", True),
    ("parquet", "interval day to second"): ("1 days 02:03:04.500000", False),
    ("json", "interval day to second"): ("INTERVAL '1 02:03:04.5' DAY TO SECOND", True),
    ("csv", "interval day to second"): ("INTERVAL '1 02:03:04.5' DAY TO SECOND", True),
    ("parquet", "interval year to month"): ("!NOT_IMPLEMENTED", True),
    ("json", "interval year to month"): ("INTERVAL '3-4' YEAR TO MONTH", True),
    ("csv", "interval year to month"): ("INTERVAL '3-4' YEAR TO MONTH", True),
    ("parquet", "array"): ("[1 2]", False),
    ("json", "array"): ("[1 2]", False),
    ("parquet", "array with a null"): ("['a' None]", False),
    ("json", "array with a null"): ("['a' None]", False),
    ("parquet", "map"): ("{'k': 1}", False),
    ("json", "map"): ("{'k': 1}", False),
    ("parquet", "struct"): ("{'a': 1, 'b': 'x'}", False),
    ("json", "struct"): ("{'a': 1, 'b': 'x'}", False),
    ("parquet", "struct in an array"): ("[{'a': 1}]", False),
    ("json", "struct in an array"): ("[{'a': 1}]", False),
    ("parquet", "variant"): ('{"a":1}', False),
    ("json", "variant"): ("{'a': 1}", True),
}

# (format, case): the error class Spark raises instead of writing, and whether Sail differs.
REFUSED = {
    ("parquet", "interval calendar"): ("UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE", True),
    ("json", "interval calendar"): ("UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE", True),
    ("csv", "interval calendar"): ("UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE", True),
    ("csv", "array"): ("UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE", True),
    ("csv", "array with a null"): ("UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE", True),
    ("csv", "map"): ("UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE", True),
    ("csv", "struct"): ("UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE", True),
    ("csv", "struct in an array"): ("UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE", True),
    ("csv", "variant"): ("UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE", True),
}


@pytest.fixture(autouse=True)
def _utc_process_timezone():
    """Pin the PROCESS timezone, not only the session one.

    A TIMESTAMP comes back from Connect as a Python `datetime` that the CLIENT builds in its own
    local zone, so `str(value)` shifts with the machine: the same cast printed 07:07:08 here
    (Europe/Madrid) and 06:07:08 on the CI runner (UTC). Pinning `spark.sql.session.timeZone` does
    not cover it -- that is the server's zone. This is the same hazard PR #2644 fixes for the Arrow
    UDF tests, and the expectations in the tables below are measured with TZ=UTC.
    """
    previous = os.environ.get("TZ")
    os.environ["TZ"] = "UTC"
    time.tzset()
    yield
    if previous is None:
        os.environ.pop("TZ", None)
    else:
        os.environ["TZ"] = previous
    time.tzset()


def _cases(table):
    return [
        pytest.param(
            fmt,
            case,
            expected,
            id=f"{fmt}-{case.replace(' ', '-')}",
            marks=tuple(([_SAIL_BUG] if diverges else []) + _guards(case)),
        )
        for (fmt, case), (expected, diverges) in table.items()
    ]


def _guards(case):
    guards = []
    if "time(" in case:
        guards.append(_NEEDS_TIME)
    if "variant" in case:
        guards.append(_NEEDS_VARIANT)
    return guards


def _round_trip(spark, tmp_path, fmt, case):
    df = spark.sql(f"SELECT {EXPRESSION[case]} AS v")
    path = str(tmp_path / f"{fmt}-{case.replace(' ', '-')}")
    writer = df.write.mode("overwrite").format(fmt)
    reader = spark.read.format(fmt)
    if fmt == "csv":
        writer = writer.option("header", "true")
        reader = reader.option("header", "true")
    writer.save(path)
    return reader.load(path)


def _skip_unless_materialisable(expected, case, lens):
    """Skip the lenses the PySpark client itself cannot serve, whatever engine is behind it.

    This has to run BEFORE the value is asked for: materialising is what raises.
    """
    if expected.startswith("!"):
        pytest.skip(f"the PySpark client cannot materialise {case} through {lens}")


def _text(value):
    """`bytes` and `bytearray` print differently, and which one the client hands back changed in
    PySpark 4.1. Normalising keeps one expectation good for every client version."""
    if isinstance(value, bytearray):
        value = bytes(value)
    return str(value)


@pytest.mark.parametrize(("fmt", "case", "expected"), _cases(SCHEMA))
def test_round_trip_keeps_the_schema(spark, tmp_path, fmt, case, expected):
    assert _round_trip(spark, tmp_path, fmt, case).schema.simpleString() == expected


@pytest.mark.parametrize(("fmt", "case", "expected"), _cases(COLLECT))
def test_round_trip_collect(spark, tmp_path, fmt, case, expected):
    _skip_unless_materialisable(expected, case, "collect")
    back = _round_trip(spark, tmp_path, fmt, case)
    assert _text(back.collect()[0][0]) == expected


@pytest.mark.parametrize(("fmt", "case", "expected"), _cases(TO_ARROW))
@_NEEDS_TO_ARROW
def test_round_trip_to_arrow(spark, tmp_path, fmt, case, expected):
    _skip_unless_materialisable(expected, case, "toArrow")
    back = _round_trip(spark, tmp_path, fmt, case)
    assert _text(back.toArrow().column(0).to_pylist()[0]) == expected


@pytest.mark.parametrize(("fmt", "case", "expected"), _cases(TO_PANDAS))
def test_round_trip_to_pandas(spark, tmp_path, fmt, case, expected):
    _skip_unless_materialisable(expected, case, "toPandas")
    back = _round_trip(spark, tmp_path, fmt, case)
    assert _text(back.toPandas().iloc[0, 0]) == expected


@pytest.mark.parametrize(("fmt", "case", "expected"), _cases(REFUSED))
def test_the_format_refuses_the_type(spark, tmp_path, fmt, case, expected):
    df = spark.sql(f"SELECT {EXPRESSION[case]} AS v")
    writer = df.write.mode("overwrite").format(fmt)
    if fmt == "csv":
        writer = writer.option("header", "true")
    with pytest.raises(Exception, match=expected):
        writer.save(str(tmp_path / f"{fmt}-{case.replace(' ', '-')}"))


# A date before the Gregorian cutover means two different days depending on which calendar reads
# it, so Spark makes the writer declare its intent: `datetimeRebaseModeInWrite` is EXCEPTION by
# default for ambiguous values, and LEGACY or CORRECTED say which calendar to write. Arrow has no
# such notion, and Sail writes the value whatever the mode says.
_REBASE_DATE = "DATE '1500-01-01'"


@pytest.mark.parametrize("mode", ["LEGACY", "CORRECTED"])
def test_parquet_rebase_mode_writes_a_pre_gregorian_date(spark, tmp_path, mode):
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", mode)
    path = str(tmp_path / f"rebase-{mode}")
    spark.sql(f"SELECT {_REBASE_DATE} AS v").write.mode("overwrite").parquet(path)
    assert str(spark.read.parquet(path).collect()[0][0]) == "1500-01-01"


@_SAIL_BUG
def test_parquet_rebase_mode_exception_refuses_a_pre_gregorian_date(spark, tmp_path):
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "EXCEPTION")
    with pytest.raises(Exception, match="INCONSISTENT_BEHAVIOR_CROSS_VERSION"):
        spark.sql(f"SELECT {_REBASE_DATE} AS v").write.mode("overwrite").parquet(str(tmp_path / "rebase-exception"))
