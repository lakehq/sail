"""Parity of `JOIN` with every join type, criteria and key shape.

The join type, the criteria (`ON` / `USING` / `NATURAL`) and the shape of the key are independent
axes, and Spark resolves the key name with the analyzer resolver, so every case runs under both
values of `spark.sql.caseSensitive`. The analyzer itself is pinned for the same reason it is pinned
in `test_dataframe_columns.py`: a default that moves would quietly change what these assert.

Every expectation was measured against Spark before it was written down.
"""

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark

_SAIL_BUG = pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)

_ANALYZER = {"spark.sql.analyzer.singlePassResolver.enabled": "false"}

QUERIES = {
    "on/INNER": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l INNER JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r ON l.k = r.k",
    "using/INNER": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l INNER JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r USING (k)",
    "natural/INNER": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l NATURAL INNER JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r",
    "on/LEFT OUTER": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l LEFT OUTER JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r ON l.k = r.k",
    "using/LEFT OUTER": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l LEFT OUTER JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r USING (k)",
    "natural/LEFT OUTER": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l NATURAL LEFT OUTER JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r",
    "on/RIGHT OUTER": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l RIGHT OUTER JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r ON l.k = r.k",
    "using/RIGHT OUTER": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l RIGHT OUTER JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r USING (k)",
    "natural/RIGHT OUTER": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l NATURAL RIGHT OUTER JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r",
    "on/FULL OUTER": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l FULL OUTER JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r ON l.k = r.k",
    "using/FULL OUTER": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l FULL OUTER JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r USING (k)",
    "natural/FULL OUTER": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l NATURAL FULL OUTER JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r",
    "on/LEFT SEMI": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l LEFT SEMI JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r ON l.k = r.k",
    "using/LEFT SEMI": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l LEFT SEMI JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r USING (k)",
    "natural/LEFT SEMI": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l NATURAL LEFT SEMI JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r",
    "on/LEFT ANTI": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l LEFT ANTI JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r ON l.k = r.k",
    "using/LEFT ANTI": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l LEFT ANTI JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r USING (k)",
    "natural/LEFT ANTI": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l NATURAL LEFT ANTI JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r",
    "on/CROSS": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l CROSS JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r ON l.k = r.k",
    "using/CROSS": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l CROSS JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r USING (k)",
    "natural/CROSS": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l NATURAL CROSS JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r",
    "key/two keys": "SELECT * FROM (SELECT 1 AS a, 2 AS b, 3 AS c) AS l JOIN (SELECT 1 AS a, 2 AS b, 4 AS d) AS r USING (a, b)",
    "key/key repeated in the clause": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r USING (k, k)",
    "key/key differing in case": "SELECT * FROM (SELECT 1 AS a, 'p' AS b) AS l JOIN (SELECT 1 AS A, 'q' AS c) AS r USING (A)",
    "key/key duplicated on the left": "SELECT * FROM (SELECT 1 AS a, 1 AS a) AS l JOIN (SELECT 1 AS a) AS r USING (a)",
    "key/key duplicated on the right": "SELECT * FROM (SELECT 1 AS a) AS l JOIN (SELECT 1 AS a, 1 AS a) AS r USING (a)",
    "key/key missing on the right": "SELECT * FROM (SELECT 1 AS a) AS l JOIN (SELECT 1 AS b) AS r USING (a)",
    "key/key missing on both": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r USING (nope)",
    "key/widening key types": "SELECT * FROM (SELECT CAST(1 AS INT) AS k) AS l JOIN (SELECT CAST(1 AS BIGINT) AS k) AS r USING (k)",
    "key/incompatible key types": "SELECT * FROM (SELECT 1 AS k) AS l JOIN (SELECT 'x' AS k) AS r USING (k)",
    "key/array key": "SELECT * FROM (SELECT array(1,2) AS k) AS l JOIN (SELECT array(1,2) AS k) AS r USING (k)",
    "key/struct key": "SELECT * FROM (SELECT named_struct('a',1) AS k) AS l JOIN (SELECT named_struct('a',1) AS k) AS r USING (k)",
    "key/map key": "SELECT * FROM (SELECT map('a',1) AS k) AS l JOIN (SELECT map('a',1) AS k) AS r USING (k)",
    "key/self join": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l JOIN (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS r USING (k)",
    "key/natural self join": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l NATURAL JOIN (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS r",
    "key/no common name": "SELECT * FROM (SELECT 1 AS a) AS l NATURAL JOIN (SELECT 2 AS b) AS r",
    "key/every name common": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l NATURAL JOIN (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS r",
    "key/qualified key on the left": "SELECT l.k FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r USING (k)",
    "key/qualified key on the right": "SELECT r.k FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r USING (k)",
    "key/star then qualified key": "SELECT *, l.k FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r USING (k)",
    "key/key in a where clause": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r USING (k) WHERE k = 1",
    "key/key in a group by": "SELECT k, count(*) AS n FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r USING (k) GROUP BY k",
    "key/key in an order by": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS l JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS r USING (k) ORDER BY k",
    "key/three way using": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS a JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS b USING (k) JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS c USING (k)",
    "key/three way natural": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS a NATURAL JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS b NATURAL JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS c",
    "key/using then natural": "SELECT * FROM (SELECT * FROM VALUES (1,'x'),(2,'y'),(NULL,'z') AS t(k, lv)) AS a JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS b USING (k) NATURAL JOIN (SELECT * FROM VALUES (1,'p'),(3,'q'),(NULL,'r') AS t(k, rv)) AS c",
}


# (case, caseSensitive, columns, schema, rows)
RESULTS = [
    (
        "on/INNER",
        "false",
        ["k", "lv", "k", "rv"],
        "struct<k:int,lv:string,k:int,rv:string>",
        ["{'k#1': 1, 'lv': 'x', 'k#2': 1, 'rv': 'p'}"],
    ),
    (
        "on/INNER",
        "true",
        ["k", "lv", "k", "rv"],
        "struct<k:int,lv:string,k:int,rv:string>",
        ["{'k#1': 1, 'lv': 'x', 'k#2': 1, 'rv': 'p'}"],
    ),
    (
        "using/INNER",
        "false",
        ["k", "lv", "rv"],
        "struct<k:int,lv:string,rv:string>",
        ["{'k': 1, 'lv': 'x', 'rv': 'p'}"],
    ),
    ("using/INNER", "true", ["k", "lv", "rv"], "struct<k:int,lv:string,rv:string>", ["{'k': 1, 'lv': 'x', 'rv': 'p'}"]),
    (
        "natural/INNER",
        "false",
        ["k", "lv", "rv"],
        "struct<k:int,lv:string,rv:string>",
        ["{'k': 1, 'lv': 'x', 'rv': 'p'}"],
    ),
    (
        "natural/INNER",
        "true",
        ["k", "lv", "rv"],
        "struct<k:int,lv:string,rv:string>",
        ["{'k': 1, 'lv': 'x', 'rv': 'p'}"],
    ),
    (
        "on/LEFT OUTER",
        "false",
        ["k", "lv", "k", "rv"],
        "struct<k:int,lv:string,k:int,rv:string>",
        [
            "{'k#1': 1, 'lv': 'x', 'k#2': 1, 'rv': 'p'}",
            "{'k#1': 2, 'lv': 'y', 'k#2': None, 'rv': None}",
            "{'k#1': None, 'lv': 'z', 'k#2': None, 'rv': None}",
        ],
    ),
    (
        "on/LEFT OUTER",
        "true",
        ["k", "lv", "k", "rv"],
        "struct<k:int,lv:string,k:int,rv:string>",
        [
            "{'k#1': 1, 'lv': 'x', 'k#2': 1, 'rv': 'p'}",
            "{'k#1': 2, 'lv': 'y', 'k#2': None, 'rv': None}",
            "{'k#1': None, 'lv': 'z', 'k#2': None, 'rv': None}",
        ],
    ),
    (
        "using/LEFT OUTER",
        "false",
        ["k", "lv", "rv"],
        "struct<k:int,lv:string,rv:string>",
        ["{'k': 1, 'lv': 'x', 'rv': 'p'}", "{'k': 2, 'lv': 'y', 'rv': None}", "{'k': None, 'lv': 'z', 'rv': None}"],
    ),
    (
        "using/LEFT OUTER",
        "true",
        ["k", "lv", "rv"],
        "struct<k:int,lv:string,rv:string>",
        ["{'k': 1, 'lv': 'x', 'rv': 'p'}", "{'k': 2, 'lv': 'y', 'rv': None}", "{'k': None, 'lv': 'z', 'rv': None}"],
    ),
    (
        "natural/LEFT OUTER",
        "false",
        ["k", "lv", "rv"],
        "struct<k:int,lv:string,rv:string>",
        ["{'k': 1, 'lv': 'x', 'rv': 'p'}", "{'k': 2, 'lv': 'y', 'rv': None}", "{'k': None, 'lv': 'z', 'rv': None}"],
    ),
    (
        "natural/LEFT OUTER",
        "true",
        ["k", "lv", "rv"],
        "struct<k:int,lv:string,rv:string>",
        ["{'k': 1, 'lv': 'x', 'rv': 'p'}", "{'k': 2, 'lv': 'y', 'rv': None}", "{'k': None, 'lv': 'z', 'rv': None}"],
    ),
    (
        "on/RIGHT OUTER",
        "false",
        ["k", "lv", "k", "rv"],
        "struct<k:int,lv:string,k:int,rv:string>",
        [
            "{'k#1': 1, 'lv': 'x', 'k#2': 1, 'rv': 'p'}",
            "{'k#1': None, 'lv': None, 'k#2': 3, 'rv': 'q'}",
            "{'k#1': None, 'lv': None, 'k#2': None, 'rv': 'r'}",
        ],
    ),
    (
        "on/RIGHT OUTER",
        "true",
        ["k", "lv", "k", "rv"],
        "struct<k:int,lv:string,k:int,rv:string>",
        [
            "{'k#1': 1, 'lv': 'x', 'k#2': 1, 'rv': 'p'}",
            "{'k#1': None, 'lv': None, 'k#2': 3, 'rv': 'q'}",
            "{'k#1': None, 'lv': None, 'k#2': None, 'rv': 'r'}",
        ],
    ),
    (
        "using/RIGHT OUTER",
        "false",
        ["k", "lv", "rv"],
        "struct<k:int,lv:string,rv:string>",
        ["{'k': 1, 'lv': 'x', 'rv': 'p'}", "{'k': 3, 'lv': None, 'rv': 'q'}", "{'k': None, 'lv': None, 'rv': 'r'}"],
    ),
    (
        "using/RIGHT OUTER",
        "true",
        ["k", "lv", "rv"],
        "struct<k:int,lv:string,rv:string>",
        ["{'k': 1, 'lv': 'x', 'rv': 'p'}", "{'k': 3, 'lv': None, 'rv': 'q'}", "{'k': None, 'lv': None, 'rv': 'r'}"],
    ),
    (
        "natural/RIGHT OUTER",
        "false",
        ["k", "lv", "rv"],
        "struct<k:int,lv:string,rv:string>",
        ["{'k': 1, 'lv': 'x', 'rv': 'p'}", "{'k': 3, 'lv': None, 'rv': 'q'}", "{'k': None, 'lv': None, 'rv': 'r'}"],
    ),
    (
        "natural/RIGHT OUTER",
        "true",
        ["k", "lv", "rv"],
        "struct<k:int,lv:string,rv:string>",
        ["{'k': 1, 'lv': 'x', 'rv': 'p'}", "{'k': 3, 'lv': None, 'rv': 'q'}", "{'k': None, 'lv': None, 'rv': 'r'}"],
    ),
    (
        "on/FULL OUTER",
        "false",
        ["k", "lv", "k", "rv"],
        "struct<k:int,lv:string,k:int,rv:string>",
        [
            "{'k#1': 1, 'lv': 'x', 'k#2': 1, 'rv': 'p'}",
            "{'k#1': 2, 'lv': 'y', 'k#2': None, 'rv': None}",
            "{'k#1': None, 'lv': 'z', 'k#2': None, 'rv': None}",
            "{'k#1': None, 'lv': None, 'k#2': 3, 'rv': 'q'}",
            "{'k#1': None, 'lv': None, 'k#2': None, 'rv': 'r'}",
        ],
    ),
    (
        "on/FULL OUTER",
        "true",
        ["k", "lv", "k", "rv"],
        "struct<k:int,lv:string,k:int,rv:string>",
        [
            "{'k#1': 1, 'lv': 'x', 'k#2': 1, 'rv': 'p'}",
            "{'k#1': 2, 'lv': 'y', 'k#2': None, 'rv': None}",
            "{'k#1': None, 'lv': 'z', 'k#2': None, 'rv': None}",
            "{'k#1': None, 'lv': None, 'k#2': 3, 'rv': 'q'}",
            "{'k#1': None, 'lv': None, 'k#2': None, 'rv': 'r'}",
        ],
    ),
    (
        "using/FULL OUTER",
        "false",
        ["k", "lv", "rv"],
        "struct<k:int,lv:string,rv:string>",
        [
            "{'k': 1, 'lv': 'x', 'rv': 'p'}",
            "{'k': 2, 'lv': 'y', 'rv': None}",
            "{'k': 3, 'lv': None, 'rv': 'q'}",
            "{'k': None, 'lv': 'z', 'rv': None}",
            "{'k': None, 'lv': None, 'rv': 'r'}",
        ],
    ),
    (
        "using/FULL OUTER",
        "true",
        ["k", "lv", "rv"],
        "struct<k:int,lv:string,rv:string>",
        [
            "{'k': 1, 'lv': 'x', 'rv': 'p'}",
            "{'k': 2, 'lv': 'y', 'rv': None}",
            "{'k': 3, 'lv': None, 'rv': 'q'}",
            "{'k': None, 'lv': 'z', 'rv': None}",
            "{'k': None, 'lv': None, 'rv': 'r'}",
        ],
    ),
    (
        "natural/FULL OUTER",
        "false",
        ["k", "lv", "rv"],
        "struct<k:int,lv:string,rv:string>",
        [
            "{'k': 1, 'lv': 'x', 'rv': 'p'}",
            "{'k': 2, 'lv': 'y', 'rv': None}",
            "{'k': 3, 'lv': None, 'rv': 'q'}",
            "{'k': None, 'lv': 'z', 'rv': None}",
            "{'k': None, 'lv': None, 'rv': 'r'}",
        ],
    ),
    (
        "natural/FULL OUTER",
        "true",
        ["k", "lv", "rv"],
        "struct<k:int,lv:string,rv:string>",
        [
            "{'k': 1, 'lv': 'x', 'rv': 'p'}",
            "{'k': 2, 'lv': 'y', 'rv': None}",
            "{'k': 3, 'lv': None, 'rv': 'q'}",
            "{'k': None, 'lv': 'z', 'rv': None}",
            "{'k': None, 'lv': None, 'rv': 'r'}",
        ],
    ),
    ("on/LEFT SEMI", "false", ["k", "lv"], "struct<k:int,lv:string>", ["{'k': 1, 'lv': 'x'}"]),
    ("on/LEFT SEMI", "true", ["k", "lv"], "struct<k:int,lv:string>", ["{'k': 1, 'lv': 'x'}"]),
    ("using/LEFT SEMI", "false", ["k", "lv"], "struct<k:int,lv:string>", ["{'k': 1, 'lv': 'x'}"]),
    ("using/LEFT SEMI", "true", ["k", "lv"], "struct<k:int,lv:string>", ["{'k': 1, 'lv': 'x'}"]),
    (
        "on/LEFT ANTI",
        "false",
        ["k", "lv"],
        "struct<k:int,lv:string>",
        ["{'k': 2, 'lv': 'y'}", "{'k': None, 'lv': 'z'}"],
    ),
    ("on/LEFT ANTI", "true", ["k", "lv"], "struct<k:int,lv:string>", ["{'k': 2, 'lv': 'y'}", "{'k': None, 'lv': 'z'}"]),
    (
        "using/LEFT ANTI",
        "false",
        ["k", "lv"],
        "struct<k:int,lv:string>",
        ["{'k': 2, 'lv': 'y'}", "{'k': None, 'lv': 'z'}"],
    ),
    (
        "using/LEFT ANTI",
        "true",
        ["k", "lv"],
        "struct<k:int,lv:string>",
        ["{'k': 2, 'lv': 'y'}", "{'k': None, 'lv': 'z'}"],
    ),
    pytest.param(
        *(
            "on/CROSS",
            "false",
            ["k", "lv", "k", "rv"],
            "struct<k:int,lv:string,k:int,rv:string>",
            ["{'k#1': 1, 'lv': 'x', 'k#2': 1, 'rv': 'p'}"],
        ),
        marks=_SAIL_BUG,
    ),
    pytest.param(
        *(
            "on/CROSS",
            "true",
            ["k", "lv", "k", "rv"],
            "struct<k:int,lv:string,k:int,rv:string>",
            ["{'k#1': 1, 'lv': 'x', 'k#2': 1, 'rv': 'p'}"],
        ),
        marks=_SAIL_BUG,
    ),
    pytest.param(
        *(
            "using/CROSS",
            "false",
            ["k", "lv", "rv"],
            "struct<k:int,lv:string,rv:string>",
            ["{'k': 1, 'lv': 'x', 'rv': 'p'}"],
        ),
        marks=_SAIL_BUG,
    ),
    pytest.param(
        *(
            "using/CROSS",
            "true",
            ["k", "lv", "rv"],
            "struct<k:int,lv:string,rv:string>",
            ["{'k': 1, 'lv': 'x', 'rv': 'p'}"],
        ),
        marks=_SAIL_BUG,
    ),
    (
        "key/two keys",
        "false",
        ["a", "b", "c", "d"],
        "struct<a:int,b:int,c:int,d:int>",
        ["{'a': 1, 'b': 2, 'c': 3, 'd': 4}"],
    ),
    (
        "key/two keys",
        "true",
        ["a", "b", "c", "d"],
        "struct<a:int,b:int,c:int,d:int>",
        ["{'a': 1, 'b': 2, 'c': 3, 'd': 4}"],
    ),
    (
        "key/key repeated in the clause",
        "false",
        ["k", "k", "lv", "rv"],
        "struct<k:int,k:int,lv:string,rv:string>",
        ["{'k#1': 1, 'k#2': 1, 'lv': 'x', 'rv': 'p'}"],
    ),
    (
        "key/key repeated in the clause",
        "true",
        ["k", "k", "lv", "rv"],
        "struct<k:int,k:int,lv:string,rv:string>",
        ["{'k#1': 1, 'k#2': 1, 'lv': 'x', 'rv': 'p'}"],
    ),
    (
        "key/key differing in case",
        "false",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:string>",
        ["{'a': 1, 'b': 'p', 'c': 'q'}"],
    ),
    ("key/key duplicated on the left", "false", ["a", "a"], "struct<a:int,a:int>", ["{'a#1': 1, 'a#2': 1}"]),
    ("key/key duplicated on the left", "true", ["a", "a"], "struct<a:int,a:int>", ["{'a#1': 1, 'a#2': 1}"]),
    ("key/key duplicated on the right", "false", ["a", "a"], "struct<a:int,a:int>", ["{'a#1': 1, 'a#2': 1}"]),
    ("key/key duplicated on the right", "true", ["a", "a"], "struct<a:int,a:int>", ["{'a#1': 1, 'a#2': 1}"]),
    ("key/widening key types", "false", ["k"], "struct<k:int>", ["{'k': 1}"]),
    ("key/widening key types", "true", ["k"], "struct<k:int>", ["{'k': 1}"]),
    ("key/array key", "false", ["k"], "struct<k:array<int>>", ["{'k': [1, 2]}"]),
    ("key/array key", "true", ["k"], "struct<k:array<int>>", ["{'k': [1, 2]}"]),
    ("key/struct key", "false", ["k"], "struct<k:struct<a:int>>", ["{'k': Row(a=1)}"]),
    ("key/struct key", "true", ["k"], "struct<k:struct<a:int>>", ["{'k': Row(a=1)}"]),
    (
        "key/self join",
        "false",
        ["k", "lv", "lv"],
        "struct<k:int,lv:string,lv:string>",
        ["{'k': 1, 'lv#1': 'x', 'lv#2': 'x'}", "{'k': 2, 'lv#1': 'y', 'lv#2': 'y'}"],
    ),
    (
        "key/self join",
        "true",
        ["k", "lv", "lv"],
        "struct<k:int,lv:string,lv:string>",
        ["{'k': 1, 'lv#1': 'x', 'lv#2': 'x'}", "{'k': 2, 'lv#1': 'y', 'lv#2': 'y'}"],
    ),
    (
        "key/natural self join",
        "false",
        ["k", "lv"],
        "struct<k:int,lv:string>",
        ["{'k': 1, 'lv': 'x'}", "{'k': 2, 'lv': 'y'}"],
    ),
    (
        "key/natural self join",
        "true",
        ["k", "lv"],
        "struct<k:int,lv:string>",
        ["{'k': 1, 'lv': 'x'}", "{'k': 2, 'lv': 'y'}"],
    ),
    ("key/no common name", "false", ["a", "b"], "struct<a:int,b:int>", ["{'a': 1, 'b': 2}"]),
    ("key/no common name", "true", ["a", "b"], "struct<a:int,b:int>", ["{'a': 1, 'b': 2}"]),
    (
        "key/every name common",
        "false",
        ["k", "lv"],
        "struct<k:int,lv:string>",
        ["{'k': 1, 'lv': 'x'}", "{'k': 2, 'lv': 'y'}"],
    ),
    (
        "key/every name common",
        "true",
        ["k", "lv"],
        "struct<k:int,lv:string>",
        ["{'k': 1, 'lv': 'x'}", "{'k': 2, 'lv': 'y'}"],
    ),
    pytest.param(*("key/qualified key on the left", "false", ["k"], "struct<k:int>", ["{'k': 1}"]), marks=_SAIL_BUG),
    pytest.param(*("key/qualified key on the left", "true", ["k"], "struct<k:int>", ["{'k': 1}"]), marks=_SAIL_BUG),
    pytest.param(*("key/qualified key on the right", "false", ["k"], "struct<k:int>", ["{'k': 1}"]), marks=_SAIL_BUG),
    pytest.param(*("key/qualified key on the right", "true", ["k"], "struct<k:int>", ["{'k': 1}"]), marks=_SAIL_BUG),
    pytest.param(
        *(
            "key/star then qualified key",
            "false",
            ["k", "lv", "rv", "k"],
            "struct<k:int,lv:string,rv:string,k:int>",
            ["{'k#1': 1, 'lv': 'x', 'rv': 'p', 'k#2': 1}"],
        ),
        marks=_SAIL_BUG,
    ),
    pytest.param(
        *(
            "key/star then qualified key",
            "true",
            ["k", "lv", "rv", "k"],
            "struct<k:int,lv:string,rv:string,k:int>",
            ["{'k#1': 1, 'lv': 'x', 'rv': 'p', 'k#2': 1}"],
        ),
        marks=_SAIL_BUG,
    ),
    (
        "key/key in a where clause",
        "false",
        ["k", "lv", "rv"],
        "struct<k:int,lv:string,rv:string>",
        ["{'k': 1, 'lv': 'x', 'rv': 'p'}"],
    ),
    (
        "key/key in a where clause",
        "true",
        ["k", "lv", "rv"],
        "struct<k:int,lv:string,rv:string>",
        ["{'k': 1, 'lv': 'x', 'rv': 'p'}"],
    ),
    ("key/key in a group by", "false", ["k", "n"], "struct<k:int,n:bigint>", ["{'k': 1, 'n': 1}"]),
    ("key/key in a group by", "true", ["k", "n"], "struct<k:int,n:bigint>", ["{'k': 1, 'n': 1}"]),
    (
        "key/key in an order by",
        "false",
        ["k", "lv", "rv"],
        "struct<k:int,lv:string,rv:string>",
        ["{'k': 1, 'lv': 'x', 'rv': 'p'}"],
    ),
    (
        "key/key in an order by",
        "true",
        ["k", "lv", "rv"],
        "struct<k:int,lv:string,rv:string>",
        ["{'k': 1, 'lv': 'x', 'rv': 'p'}"],
    ),
    (
        "key/three way using",
        "false",
        ["k", "lv", "rv", "rv"],
        "struct<k:int,lv:string,rv:string,rv:string>",
        ["{'k': 1, 'lv': 'x', 'rv#1': 'p', 'rv#2': 'p'}"],
    ),
    (
        "key/three way using",
        "true",
        ["k", "lv", "rv", "rv"],
        "struct<k:int,lv:string,rv:string,rv:string>",
        ["{'k': 1, 'lv': 'x', 'rv#1': 'p', 'rv#2': 'p'}"],
    ),
    (
        "key/three way natural",
        "false",
        ["k", "rv", "lv"],
        "struct<k:int,rv:string,lv:string>",
        ["{'k': 1, 'rv': 'p', 'lv': 'x'}"],
    ),
    (
        "key/three way natural",
        "true",
        ["k", "rv", "lv"],
        "struct<k:int,rv:string,lv:string>",
        ["{'k': 1, 'rv': 'p', 'lv': 'x'}"],
    ),
    (
        "key/using then natural",
        "false",
        ["k", "rv", "lv"],
        "struct<k:int,rv:string,lv:string>",
        ["{'k': 1, 'rv': 'p', 'lv': 'x'}"],
    ),
    (
        "key/using then natural",
        "true",
        ["k", "rv", "lv"],
        "struct<k:int,rv:string,lv:string>",
        ["{'k': 1, 'rv': 'p', 'lv': 'x'}"],
    ),
]

# (case, caseSensitive, error condition)
ERRORS = [
    pytest.param(
        *("natural/LEFT SEMI", "false", r"Unsupported natural join type LeftSemi"),
        marks=_SAIL_BUG,
    ),
    pytest.param(
        *("natural/LEFT SEMI", "true", r"Unsupported natural join type LeftSemi"),
        marks=_SAIL_BUG,
    ),
    pytest.param(
        *("natural/LEFT ANTI", "false", r"Unsupported natural join type LeftAnti"),
        marks=_SAIL_BUG,
    ),
    pytest.param(
        *("natural/LEFT ANTI", "true", r"Unsupported natural join type LeftAnti"),
        marks=_SAIL_BUG,
    ),
    pytest.param(*("natural/CROSS", "false", "INCOMPATIBLE_JOIN_TYPES"), marks=_SAIL_BUG),
    pytest.param(*("natural/CROSS", "true", "INCOMPATIBLE_JOIN_TYPES"), marks=_SAIL_BUG),
    ("key/key differing in case", "true", "UNRESOLVED_USING_COLUMN_FOR_JOIN"),
    ("key/key missing on the right", "false", "UNRESOLVED_USING_COLUMN_FOR_JOIN"),
    ("key/key missing on the right", "true", "UNRESOLVED_USING_COLUMN_FOR_JOIN"),
    ("key/key missing on both", "false", "UNRESOLVED_USING_COLUMN_FOR_JOIN"),
    ("key/key missing on both", "true", "UNRESOLVED_USING_COLUMN_FOR_JOIN"),
    pytest.param(*("key/incompatible key types", "false", "CAST_INVALID_INPUT"), marks=_SAIL_BUG),
    pytest.param(*("key/incompatible key types", "true", "CAST_INVALID_INPUT"), marks=_SAIL_BUG),
    pytest.param(*("key/map key", "false", "DATATYPE_MISMATCH.INVALID_ORDERING_TYPE"), marks=_SAIL_BUG),
    pytest.param(*("key/map key", "true", "DATATYPE_MISMATCH.INVALID_ORDERING_TYPE"), marks=_SAIL_BUG),
]


def _configure(spark, case_sensitive):
    for key, value in {**_ANALYZER, "spark.sql.caseSensitive": case_sensitive}.items():
        spark.conf.set(key, value)


def _unconfigure(spark):
    for key in [*_ANALYZER, "spark.sql.caseSensitive"]:
        spark.conf.unset(key)


def _row_keys(names):
    """Disambiguates repeated column names so a row keeps every column it has.

    `Row.asDict` keeps only one of a pair of columns that share a name, which is exactly the
    column a case about duplicate names is asserting, so the repeated ones are numbered by
    position instead.
    """
    repeated = {name for name in names if names.count(name) > 1}
    seen = {}
    keys = []
    for name in names:
        if name in repeated:
            seen[name] = seen.get(name, 0) + 1
            keys.append(f"{name}#{seen[name]}")
        else:
            keys.append(name)
    return keys


@pytest.mark.parametrize(("case", "case_sensitive", "columns", "schema", "rows"), RESULTS)
def test_join_result(spark, case, case_sensitive, columns, schema, rows):
    _configure(spark, case_sensitive)
    try:
        df = spark.sql(QUERIES[case])
        assert df.columns == columns
        assert df.schema.simpleString() == schema
        keys = _row_keys(df.columns)
        assert sorted(str(dict(zip(keys, list(row), strict=True))) for row in df.collect()) == rows
    finally:
        _unconfigure(spark)


@pytest.mark.parametrize(("case", "case_sensitive", "condition"), ERRORS)
def test_join_error(spark, case, case_sensitive, condition):
    _configure(spark, case_sensitive)
    try:
        with pytest.raises(Exception, match=condition):
            _ = spark.sql(QUERIES[case]).collect()
    finally:
        _unconfigure(spark)
