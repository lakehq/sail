"""Correlated EXISTS / NOT EXISTS / IN / NOT IN / scalar subqueries whose body has an ORDER BY
plan and return the same rows as without the ORDER BY.

Expected values were verified on JVM Spark 4.2.0 (test corpus suite scalar-subquery-reuse-extension
v9 (bug follow-ups B1-B3), cases b2-*). The `t` fixture is a parquet table (g INT, x INT, y STRING) with the rows
(1, 10, 'a'), (1, 20, 'b'), (2, 5, 'c'), (2, NULL, 'd').
"""

import uuid

import pytest

from pysail.testing.spark.utils.sql import escape_sql_string_literal


@pytest.fixture(scope="module")
def t(spark, tmp_path_factory):
    name = f"corr_order_{uuid.uuid4().hex[:8]}"
    location = escape_sql_string_literal(str(tmp_path_factory.mktemp("corr_order") / name))
    spark.sql(f"CREATE TABLE {name} (g INT, x INT, y STRING) USING parquet LOCATION '{location}'")
    spark.sql(f"INSERT INTO {name} VALUES (1, 10, 'a'), (1, 20, 'b'), (2, 5, 'c'), (2, NULL, 'd')")  # noqa: S608
    yield name
    spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        pytest.param(
            "SELECT g, x FROM {t} AS o WHERE EXISTS (SELECT 1 FROM {t} AS i WHERE i.g = o.g AND i.x > 9 ORDER BY i.x) ORDER BY x NULLS FIRST",
            [(1, 10), (1, 20)],
            id="exists-sorted",
        ),
        pytest.param(
            "SELECT g, x FROM {t} AS o WHERE EXISTS (SELECT i.x FROM {t} AS i WHERE i.g = o.g ORDER BY i.x DESC NULLS LAST) ORDER BY x NULLS FIRST",
            [(2, None), (2, 5), (1, 10), (1, 20)],
            id="exists-sorted-desc",
        ),
        pytest.param(
            "SELECT g, y FROM {t} AS o WHERE NOT EXISTS (SELECT 1 FROM {t} AS i WHERE i.x > o.x ORDER BY i.x DESC) ORDER BY y",
            [(1, "b"), (2, "d")],
            id="not-exists-sorted",
        ),
        pytest.param(
            "SELECT g, x FROM {t} AS o WHERE x IN (SELECT i.x FROM {t} AS i WHERE i.g = o.g ORDER BY i.x DESC) ORDER BY x",
            [(2, 5), (1, 10), (1, 20)],
            id="in-sorted-correlated",
        ),
        pytest.param(
            "SELECT g, x FROM {t} AS o WHERE EXISTS (SELECT i.x + 1 AS z FROM {t} AS i WHERE i.g = o.g ORDER BY z DESC) ORDER BY x NULLS FIRST",
            [(2, None), (2, 5), (1, 10), (1, 20)],
            id="exists-sorted-projection",
        ),
        pytest.param(
            "SELECT g, x FROM {t} AS o WHERE x NOT IN (SELECT i.x FROM {t} AS i WHERE i.g <> o.g AND i.x IS NOT NULL ORDER BY i.x DESC) ORDER BY x",
            [(2, 5), (1, 10), (1, 20)],
            id="not-in-sorted-correlated",
        ),
        pytest.param(
            "SELECT y, (SELECT max(i.x) FROM {t} AS i WHERE i.g = o.g ORDER BY 1) AS m FROM {t} AS o ORDER BY y",
            [("a", 20), ("b", 20), ("c", 5), ("d", 5)],
            id="scalar-sorted-ordinal",
        ),
        pytest.param(
            "SELECT y, (SELECT max(i.x) FROM {t} AS i WHERE i.g = o.g ORDER BY max(i.x) DESC) AS m FROM {t} AS o ORDER BY y",
            [("a", 20), ("b", 20), ("c", 5), ("d", 5)],
            id="scalar-sorted-agg",
        ),
        pytest.param(
            "SELECT y, (SELECT count(*) FROM {t} AS i WHERE i.x < o.x ORDER BY 1) AS c FROM {t} AS o ORDER BY y",
            [("a", 1), ("b", 2), ("c", 0), ("d", 0)],
            id="scalar-sorted-count",
        ),
        pytest.param(
            "SELECT y, (SELECT i.x FROM {t} AS i WHERE i.g = o.g ORDER BY i.x DESC NULLS LAST LIMIT 1) AS top FROM {t} AS o ORDER BY y",
            [("a", 20), ("b", 20), ("c", 5), ("d", 5)],
            id="scalar-sorted-limit",
        ),
        pytest.param(
            "SELECT y FROM {t} AS o WHERE x = (SELECT max(i.x) FROM {t} AS i WHERE i.g = o.g ORDER BY 1) ORDER BY y",
            [("b",), ("c",)],
            id="scalar-sorted-in-where",
        ),
        pytest.param(
            "WITH c AS (SELECT * FROM {t}) SELECT g, x FROM c AS o WHERE EXISTS (SELECT 1 FROM c AS i WHERE i.g = o.g AND i.x > 15 ORDER BY i.y) ORDER BY x NULLS FIRST",
            [(1, 10), (1, 20)],
            id="exists-sorted-cte",
        ),
        pytest.param(
            "SELECT g, x FROM {t} AS o WHERE EXISTS (SELECT 1 FROM {t} AS i WHERE i.g = o.g AND i.x > 15 ORDER BY i.x) OR o.x IS NULL ORDER BY x NULLS FIRST",
            [(2, None), (1, 10), (1, 20)],
            id="exists-sorted-or",
        ),
        pytest.param(
            "SELECT id FROM range(10) AS o WHERE EXISTS (SELECT 1 FROM range(10) AS i WHERE i.id = o.id * 2 ORDER BY i.id DESC) ORDER BY id",
            [(0,), (1,), (2,), (3,), (4,)],
            id="exists-sorted-range",
        ),
        pytest.param(
            "SELECT id, (SELECT max(i.id) FROM range(10) AS i WHERE i.id <= o.id ORDER BY 1 DESC) AS m FROM range(5) AS o ORDER BY id",
            [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)],
            id="scalar-sorted-range",
        ),
        pytest.param(
            "SELECT g, x FROM {t} AS o WHERE EXISTS (SELECT 1 FROM {t} AS i WHERE i.g = o.g AND i.x > 9) ORDER BY x NULLS FIRST",
            [(1, 10), (1, 20)],
            id="exists-control",
        ),
        pytest.param(
            "SELECT g, y FROM {t} AS o WHERE NOT EXISTS (SELECT 1 FROM {t} AS i WHERE i.x > o.x) ORDER BY y",
            [(1, "b"), (2, "d")],
            id="not-exists-control",
        ),
        pytest.param(
            "SELECT g, x FROM {t} AS o WHERE x IN (SELECT i.x FROM {t} AS i WHERE i.g = o.g) ORDER BY x",
            [(2, 5), (1, 10), (1, 20)],
            id="in-control",
        ),
        pytest.param(
            "SELECT g, x FROM {t} AS o WHERE x NOT IN (SELECT i.x FROM {t} AS i WHERE i.g <> o.g AND i.x IS NOT NULL) ORDER BY x",
            [(2, 5), (1, 10), (1, 20)],
            id="not-in-control",
        ),
        pytest.param(
            "SELECT y, (SELECT max(i.x) FROM {t} AS i WHERE i.g = o.g) AS m FROM {t} AS o ORDER BY y",
            [("a", 20), ("b", 20), ("c", 5), ("d", 5)],
            id="scalar-control",
        ),
        pytest.param(
            "SELECT y, (SELECT count(*) FROM {t} AS i WHERE i.x < o.x) AS c FROM {t} AS o ORDER BY y",
            [("a", 1), ("b", 2), ("c", 0), ("d", 0)],
            id="scalar-count-control",
        ),
    ],
)
def test_correlated_subqueries_with_order_by(spark, t, query, expected):
    actual = [tuple(row) for row in spark.sql(query.format(t=t)).collect()]
    assert actual == expected


def test_correlated_scalar_subquery_with_order_by_and_many_rows_fails(spark, t):
    query = f"SELECT y, (SELECT i.x FROM {t} AS i WHERE i.g = o.g ORDER BY i.x) AS v FROM {t} AS o"  # noqa: S608
    with pytest.raises(Exception, match=r"(?i)(SCALAR_SUBQUERY_TOO_MANY_ROWS|more than one row)"):
        spark.sql(query).collect()
