import re
from uuid import uuid4

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version
from pysail.testing.spark.utils.sql import escape_sql_string_literal

SCALAR_SUBQUERY_SLOTS = re.compile(r"ScalarSubqueryExec: subqueries=(\d+)")

pytestmark = [
    pytest.mark.skipif(is_jvm_spark(), reason="Sail physical plans"),
    pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.scalar requires PySpark 4+"),
]

# Each trial builds both reads anew. The server decodes the read options of each read into its
# own hash map, so a cache key that depends on the option order misses in most trials.
TRIALS = 10


def scalar_subquery_slots(df):
    return [int(n) for n in SCALAR_SUBQUERY_SLOTS.findall(df._explain_string())]  # noqa: SLF001


@pytest.fixture
def csv_table(spark, tmp_path):
    """A CSV table with the line `1;x,y`, so that `sep` decides the value of `a`."""
    name = f"read_options_reuse_{uuid4().hex[:8]}"
    location = tmp_path / name
    location.mkdir()
    (location / "data.csv").write_text("1;x,y\n")
    spark.sql(
        f"CREATE TABLE {name} (a STRING, b STRING) USING csv LOCATION '{escape_sql_string_literal(str(location))}'"
    )
    yield name
    spark.sql(f"DROP TABLE IF EXISTS {name}")


def lookup(spark, table, options):
    reader = spark.read
    for key, value in options:
        reader = reader.option(key, value)
    return reader.table(table).select("a")


def run(spark, table, left, right):
    df = spark.range(1).select(
        lookup(spark, table, left).scalar().alias("l"), lookup(spark, table, right).scalar().alias("r")
    )
    return tuple(df.collect()[0]), scalar_subquery_slots(df)


MANY = [("sep", ";"), ("quote", "'"), ("escape", "\\"), ("nullValue", "NA"), ("comment", "#"), ("header", "false")]


@pytest.mark.parametrize(
    ("left", "right"),
    [
        pytest.param(MANY, MANY, id="same-order"),
        pytest.param(MANY, MANY[::-1], id="reversed-order"),
        pytest.param([("sep", ";"), ("nullValue", "NA")], [("nullValue", "NA"), ("sep", ";")], id="two-keys"),
    ],
)
def test_reads_with_the_same_options_share_one_source(spark, csv_table, left, right):
    for _ in range(TRIALS):
        assert run(spark, csv_table, left, right) == (("1", "1"), [1])


@pytest.mark.parametrize(
    ("left", "right", "expected"),
    [
        pytest.param([("sep", ";")], [("sep", ",")], ("1", "1;x"), id="value"),
        pytest.param([("sep", ";")], [("delimiter", ",")], ("1", "1;x"), id="alias-value"),
        pytest.param([("sep", ";")], [("SEP", ",")], ("1", "1;x"), id="case-value"),
        pytest.param([("sep", ";")], [("sep", ";"), ("header", "true")], ("1", None), id="extra-key"),
        pytest.param([("sep", ";")], [], ("1", "1;x"), id="no-options"),
    ],
)
def test_reads_with_different_options_do_not_share_a_source(spark, csv_table, left, right, expected):
    for _ in range(TRIALS):
        assert run(spark, csv_table, left, right) == (expected, [2])


@pytest.mark.parametrize(
    ("left", "right"),
    [
        # These reads give the same keys different values.
        pytest.param([("sep", ";"), ("delimiter", ",")], [("sep", ","), ("delimiter", ";")], id="alias"),
        pytest.param([("sep", ";"), ("SEP", ",")], [("sep", ","), ("SEP", ";")], id="case"),
    ],
)
def test_reads_with_conflicting_option_keys_do_not_share_a_source(spark, csv_table, left, right):
    for _ in range(TRIALS):
        (l_value, r_value), slots = run(spark, csv_table, left, right)
        assert slots == [2]
        assert {l_value, r_value} <= {"1", "1;x"}
