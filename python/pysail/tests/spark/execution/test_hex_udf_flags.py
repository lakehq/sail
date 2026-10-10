# ruff: noqa: S608
import re

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")

# `hex`, `unhex` and `to_binary` carry flags (`ansi_mode`, `fail_on_error`, `is_try`) that decide
# between an error and a NULL or a saturated value. The expression is serialized to the workers,
# so each query below spreads the rows over 8 partitions and puts the one row that depends on the
# flag in the middle of them.
#
# A failing task is retried on a local cluster and the retry reports "cannot add stream ... a
# different attempt has already produced job output", so the error text of the strict cases is
# lost: they assert that the query raises. The lenient cases over the same rows return a value,
# which is what tells a lost flag apart.
BAD_ROW = 17777
ROW_COUNT = 20000
ROWS = f"range(0, {ROW_COUNT}, 1, 8)"


def collect_one(spark, sql):
    return spark.sql(sql).collect()[0][0]


def test_unhex_is_lenient_on_workers(spark):
    sql = f"SELECT count(unhex(IF(id = {BAD_ROW}, 'ZZ', hex(id)))) FROM {ROWS}"
    assert collect_one(spark, sql) == ROW_COUNT - 1


def test_try_to_binary_is_lenient_on_workers(spark):
    sql = f"SELECT count(try_to_binary(IF(id = {BAD_ROW}, 'ZZ', hex(id)), 'hex')) FROM {ROWS}"
    assert collect_one(spark, sql) == ROW_COUNT - 1


@pytest.mark.parametrize("call", ["to_binary(v, 'hex')", "to_binary(v)"])
def test_to_binary_is_strict_on_workers(spark, call):
    # The same query over valid rows runs, so the expression itself decodes on the workers.
    valid = f"SELECT count({call}) FROM (SELECT hex(id) AS v FROM {ROWS})"
    assert collect_one(spark, valid) == ROW_COUNT
    sql = f"SELECT count({call}) FROM (SELECT IF(id = {BAD_ROW}, 'ZZ', hex(id)) AS v FROM {ROWS})"
    with pytest.raises(Exception):  # noqa: B017, PT011
        spark.sql(sql).collect()


def test_try_to_binary_base64_is_lenient_per_row_on_workers(spark):
    sql = f"SELECT count(try_to_binary(IF(id = {BAD_ROW}, 'a!', 'YQ=='), 'base64')) FROM {ROWS}"
    assert collect_one(spark, sql) == ROW_COUNT - 1


def test_to_binary_base64_is_strict_on_workers(spark):
    # `to_binary(.., 'base64')` is not nullable for a non-null input, so `count` of it would be
    # rewritten to `count(1)` without evaluating it: `sum(length(..))` forces the evaluation.
    valid = f"SELECT sum(length(to_binary(IF(id = {BAD_ROW}, 'YQ==', 'YQ=='), 'base64'))) FROM {ROWS}"
    assert collect_one(spark, valid) == ROW_COUNT
    sql = f"SELECT sum(length(to_binary(IF(id = {BAD_ROW}, 'a!', 'YQ=='), 'base64'))) FROM {ROWS}"
    with pytest.raises(Exception):  # noqa: B017, PT011
        spark.sql(sql).collect()


def test_to_binary_is_strict_on_workers_for_a_value_that_prints_as_text(spark):
    valid = f"SELECT count(to_binary(CAST(id AS STRING))) FROM {ROWS} WHERE id = 1"
    assert collect_one(spark, valid) == 1
    sql = f"SELECT count(to_binary(CAST(id AS DOUBLE) / 4)) FROM {ROWS} WHERE id = {BAD_ROW}"
    with pytest.raises(Exception):  # noqa: B017, PT011
        spark.sql(sql).collect()


def test_hex_overflow_follows_ansi_on_workers(spark):
    sql = f"SELECT hex(CAST(id AS DOUBLE) * 1.0E26) FROM {ROWS} WHERE id = {BAD_ROW}"
    ansi = spark.conf.get("spark.sql.ansi.enabled")
    try:
        spark.conf.set("spark.sql.ansi.enabled", "true")
        with pytest.raises(Exception, match=re.compile(r"CAST_OVERFLOW|overflow", re.IGNORECASE)):
            spark.sql(sql).collect()
        spark.conf.set("spark.sql.ansi.enabled", "false")
        assert collect_one(spark, sql) == "7FFFFFFFFFFFFFFF"
    finally:
        spark.conf.set("spark.sql.ansi.enabled", ansi)
