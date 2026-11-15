"""Correlated EXISTS / NOT EXISTS / IN / scalar subqueries whose body has a LIMIT (with or
without ORDER BY and OFFSET) return the same rows as Spark, which keeps the top rows for every
outer row.

Expected values were verified on JVM Spark 4.2.0. The `t` fixture is a parquet table
(g INT, x INT, y STRING) with the rows (1, 10, 'a'), (1, 20, 'b'), (2, 5, 'c'), (2, NULL, 'd').
"""

import uuid

import pytest

from pysail.testing.spark.utils.sql import escape_sql_string_literal


@pytest.fixture(scope="module")
def t(spark, tmp_path_factory):
    name = f"corr_limit_{uuid.uuid4().hex[:8]}"
    location = escape_sql_string_literal(str(tmp_path_factory.mktemp("corr_limit") / name))
    spark.sql(f"CREATE TABLE {name} (g INT, x INT, y STRING) USING parquet LOCATION '{location}'")
    spark.sql(f"INSERT INTO {name} VALUES (1, 10, 'a'), (1, 20, 'b'), (2, 5, 'c'), (2, NULL, 'd')")  # noqa: S608
    yield name
    spark.sql(f"DROP TABLE IF EXISTS {name}")


ALL_ROWS = [(2, None), (2, 5), (1, 10), (1, 20)]


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        pytest.param(
            "SELECT g, y FROM {t} AS o WHERE EXISTS (SELECT i.x FROM {t} AS i WHERE i.g = o.g "
            "ORDER BY i.x DESC NULLS LAST LIMIT 1) ORDER BY y",
            [(1, "a"), (1, "b"), (2, "c"), (2, "d")],
            id="exists-sorted-limit",
        ),
        pytest.param(
            "SELECT g, y FROM {t} AS o WHERE NOT EXISTS (SELECT i.x FROM {t} AS i WHERE i.x > o.x "
            "ORDER BY i.x LIMIT 1) ORDER BY y",
            [(1, "b"), (2, "d")],
            id="not-exists-sorted-limit",
        ),
        pytest.param(
            "SELECT g, x FROM {t} AS o WHERE EXISTS (SELECT i.x FROM {t} AS i WHERE i.g = o.g "
            "ORDER BY i.x LIMIT 0) ORDER BY x NULLS FIRST",
            [],
            id="exists-sorted-limit-0",
        ),
        pytest.param(
            "SELECT g, x FROM {t} AS o WHERE NOT EXISTS (SELECT i.x FROM {t} AS i WHERE i.g = o.g "
            "ORDER BY i.x LIMIT 0) ORDER BY x NULLS FIRST",
            ALL_ROWS,
            id="not-exists-sorted-limit-0",
        ),
        pytest.param(
            "SELECT g, x FROM {t} AS o WHERE EXISTS (SELECT i.x FROM {t} AS i WHERE i.g = o.g "
            "ORDER BY i.x LIMIT 1 OFFSET 1) ORDER BY x NULLS FIRST",
            ALL_ROWS,
            id="exists-sorted-limit-offset-1",
        ),
        pytest.param(
            "SELECT g, x FROM {t} AS o WHERE EXISTS (SELECT 1 FROM {t} AS i WHERE i.g = o.g "
            "LIMIT 1 OFFSET 2) ORDER BY x NULLS FIRST",
            [],
            id="exists-limit-offset-2",
        ),
        pytest.param(
            "SELECT g, x FROM {t} AS o WHERE EXISTS (SELECT 1 FROM {t} AS i WHERE i.g = o.g "
            "AND i.x > 9 LIMIT 1) ORDER BY x NULLS FIRST",
            [(1, 10), (1, 20)],
            id="exists-limit",
        ),
        pytest.param(
            "SELECT g, x FROM {t} AS o WHERE EXISTS (SELECT 1 FROM {t} AS i LIMIT 1) ORDER BY x NULLS FIRST",
            ALL_ROWS,
            id="exists-limit-uncorrelated",
        ),
        pytest.param(
            "SELECT g, x FROM {t} AS o WHERE x IN (SELECT i.x FROM {t} AS i WHERE i.g = o.g "
            "ORDER BY i.x DESC NULLS LAST LIMIT 1) ORDER BY x",
            [(2, 5), (1, 20)],
            id="in-sorted-limit",
        ),
        pytest.param(
            "SELECT g, x FROM {t} AS o WHERE x IN (SELECT i.x FROM {t} AS i WHERE i.g = o.g "
            "ORDER BY i.x LIMIT 2) ORDER BY x",
            [(2, 5), (1, 10), (1, 20)],
            id="in-sorted-limit-2",
        ),
        pytest.param(
            "SELECT g, x FROM {t} AS o WHERE x IN (SELECT i.x FROM {t} AS i WHERE i.g = o.g "
            "ORDER BY i.x DESC NULLS FIRST LIMIT 1) ORDER BY x",
            [(1, 20)],
            id="in-sorted-limit-nulls-first",
        ),
        pytest.param(
            "SELECT g, x FROM {t} AS o WHERE x IN (SELECT i.x FROM {t} AS i WHERE i.g = o.g "
            "ORDER BY i.x DESC NULLS LAST LIMIT 1 OFFSET 1) ORDER BY x",
            [(1, 10)],
            id="in-sorted-limit-offset",
        ),
        pytest.param(
            "SELECT y, (SELECT i.x FROM {t} AS i WHERE i.g = o.g ORDER BY i.x DESC NULLS LAST LIMIT 1) AS top "
            "FROM {t} AS o ORDER BY y",
            [("a", 20), ("b", 20), ("c", 5), ("d", 5)],
            id="scalar-sorted-limit",
        ),
    ],
)
def test_correlated_subqueries_with_limit(spark, t, query, expected):
    actual = [tuple(row) for row in spark.sql(query.format(t=t)).collect()]
    assert actual == expected
