"""Coverage for the Spark Connect table-valued-function relation, using `variant_explode`.

A table-valued function returns a relation rather than a value, so it is called in a `FROM`
clause instead of a projection. Spark reaches it three ways:

1. `FROM variant_explode(v)`                      -- SQL, plain table-valued function
2. `FROM t, LATERAL variant_explode(t.v) e`       -- SQL, correlated to a column of `t`
3. `spark.tvf.variant_explode(col)`               -- DataFrame API, `pyspark/sql/tvf.py`

Note `LATERAL VIEW variant_explode(v) ...` is *not* one of them: `LATERAL VIEW` resolves its
generator against the function registry, where table-valued functions are not registered, so
Spark answers `ROUTINE_NOT_FOUND`. Sail accepts that form too, which is why the scenarios
using it in `features/generator/variant_explode.feature` are tagged `@sail-only`.

Forms 1 and 2 are the ones exercised by that feature file, on both engines. This module covers
form 3 only, because Sail does not implement the Connect relation behind it and answers every
`spark.tvf.*` call -- `explode`, `range`, `stack`, `json_tuple`, `variant_explode`, ... -- with
`unresolved table valued function`. The gap is in the relation, not in any single function.
"""

import pytest
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

pytestmark = [
    pytest.mark.skipif(pyspark_version() < (4,), reason="VARIANT requires Spark 4+"),
    pytest.mark.xfail(not is_jvm_spark(), reason="Sail does not support the Connect TVF relation", strict=True),
]


def test_tvf_relation_returns_the_declared_schema(spark):
    df = spark.tvf.variant_explode(F.parse_json(F.lit("[1, 2]")))

    assert [(f.name, f.dataType.simpleString(), f.nullable) for f in df.schema.fields] == [
        ("pos", "int", False),
        ("key", "string", True),
        ("value", "variant", False),
    ]


def test_tvf_relation_produces_one_row_per_element(spark):
    df = spark.tvf.variant_explode(F.parse_json(F.lit('{"a": 1, "b": "x"}')))
    rows = df.select("pos", "key", F.to_json(F.col("value")).alias("value")).collect()

    assert [(r["pos"], r["key"], r["value"]) for r in rows] == [(0, "a", "1"), (1, "b", '"x"')]


def test_tvf_relation_composes_with_the_rest_of_the_dataframe_api(spark):
    df = spark.tvf.variant_explode(F.parse_json(F.lit("[10, 20, 30]")))

    rows = df.where(F.col("pos") > 0).select("pos", F.to_json(F.col("value")).alias("value")).collect()

    assert [(r["pos"], r["value"]) for r in rows] == [(1, "20"), (2, "30")]
