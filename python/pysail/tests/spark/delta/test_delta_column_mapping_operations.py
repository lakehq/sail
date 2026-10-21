# ruff: noqa: DTZ001
"""Fixed Spark expectations for mapped Delta reads and DataFrame mutation checks.

SQL mutations are covered by features/column_mapping_operations.feature.
"""

from __future__ import annotations

import datetime
import json
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pysail.testing.spark.utils.sql import escape_sql_string_literal

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


pytestmark = pytest.mark.integration


_INNER = StructType([StructField("c", IntegerType()), StructField("d", StringType())])
_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("grp", StringType()),
        StructField("big", LongType()),
        StructField("dbl", DoubleType()),
        StructField("dec", DecimalType(10, 2)),
        StructField("flag", BooleanType()),
        StructField("name", StringType()),
        StructField("dt", DateType()),
        StructField("ts", TimestampType()),
        StructField("bin", BinaryType()),
        StructField("spaced col", IntegerType()),
        StructField("dotted.col", StringType()),
        StructField(
            "s",
            StructType(
                [
                    StructField("a", IntegerType()),
                    StructField("b", StringType()),
                    StructField("inner", _INNER),
                ]
            ),
        ),
        StructField("arr", ArrayType(IntegerType())),
        StructField(
            "arr_s",
            ArrayType(StructType([StructField("x", IntegerType()), StructField("y", StringType())])),
        ),
        StructField("m", MapType(StringType(), IntegerType())),
        StructField(
            "m_s",
            MapType(StringType(), StructType([StructField("p", IntegerType()), StructField("q", StringType())])),
        ),
        StructField("m_arr", MapType(StringType(), ArrayType(IntegerType()))),
        StructField(
            "nested",
            StructType(
                [
                    StructField("inner_map", MapType(StringType(), StringType())),
                    StructField(
                        "inner_arr",
                        ArrayType(StructType([StructField("z", IntegerType())])),
                    ),
                ]
            ),
        ),
    ]
)


def _row(i: int, grp: str | None, *, a: int, b: str, c: int, **overrides) -> tuple:
    values = {
        "id": i,
        "grp": grp,
        "big": i * 100,
        "dbl": i + 0.5,
        "dec": Decimal(f"{i}.25"),
        "flag": i % 2 == 0,
        "name": f"n{i}",
        "dt": datetime.date(2024, 1, i),
        "ts": datetime.datetime(2024, 1, i, 12, 0, i),
        "bin": bytes([i]),
        "spaced col": i * 10,
        "dotted.col": f"d{i}",
        "s": Row(a=a, b=b, inner=Row(c=c, d=f"inner{i}")),
        "arr": [i, i + 1],
        "arr_s": [Row(x=i, y=f"y{i}"), Row(x=i + 10, y=f"y{i + 10}")],
        "m": {"k1": i, "k2": i * 2},
        "m_s": {"k1": Row(p=i, q=f"q{i}")},
        "m_arr": {"k1": [i, i]},
        "nested": Row(inner_map={"a": f"A{i}"}, inner_arr=[Row(z=i)]),
    }
    values.update(overrides)
    return tuple(values[field.name] for field in _SCHEMA.fields)


# Rows 1 and 5 share s.a, s.b, and s.inner.c but differ in s.inner.d.
# Whole-struct grouping and equality must include that nested field.
_ROWS_FIRST = [
    _row(1, "g1", a=1, b="x", c=10),
    _row(2, "g1", a=2, b="y", c=20),
    _row(3, "g2", a=3, b="z", c=30, arr=[], arr_s=[], m={}, m_s={}),
    _row(4, None, a=4, b="w", c=40),
]
_ROWS_SECOND = [
    _row(5, "g2", a=1, b="x", c=10),
    _row(6, "g1", a=6, b="v", c=60, name=None, flag=None, dbl=None),
    _row(7, "g2", a=7, b="u", c=70, s=None, arr=None, arr_s=None, m=None, m_s=None, m_arr=None, nested=None),
    _row(8, None, a=8, b="t", c=80, arr=[8, None], m={"k1": None}),
]

_TABLE_VARIANTS = [
    pytest.param(("name", False), id="name-unpartitioned"),
    pytest.param(("name", True), id="name-partitioned"),
    pytest.param(("id", False), id="id-unpartitioned"),
    pytest.param(("id", True), id="id-partitioned"),
]

# Expected rows verified with JVM Spark 4.1.3 / Delta 4.3.1, UTC, ANSI enabled.
# Each query reads `{t}`. Rows and order-sensitive aggregates have a fixed order.
_QUERIES = {
    # projections
    "select_scalars": (
        "SELECT id, grp, big, dbl, dec, flag, name, dt, ts, bin FROM {t} ORDER BY id",
        [
            (
                1,
                "g1",
                100,
                1.5,
                Decimal("1.25"),
                False,
                "n1",
                datetime.date(2024, 1, 1),
                datetime.datetime(2024, 1, 1, 12, 0, 1),
                b"\x01",
            ),
            (
                2,
                "g1",
                200,
                2.5,
                Decimal("2.25"),
                True,
                "n2",
                datetime.date(2024, 1, 2),
                datetime.datetime(2024, 1, 2, 12, 0, 2),
                b"\x02",
            ),
            (
                3,
                "g2",
                300,
                3.5,
                Decimal("3.25"),
                False,
                "n3",
                datetime.date(2024, 1, 3),
                datetime.datetime(2024, 1, 3, 12, 0, 3),
                b"\x03",
            ),
            (
                4,
                None,
                400,
                4.5,
                Decimal("4.25"),
                True,
                "n4",
                datetime.date(2024, 1, 4),
                datetime.datetime(2024, 1, 4, 12, 0, 4),
                b"\x04",
            ),
            (
                5,
                "g2",
                500,
                5.5,
                Decimal("5.25"),
                False,
                "n5",
                datetime.date(2024, 1, 5),
                datetime.datetime(2024, 1, 5, 12, 0, 5),
                b"\x05",
            ),
            (
                6,
                "g1",
                600,
                None,
                Decimal("6.25"),
                None,
                None,
                datetime.date(2024, 1, 6),
                datetime.datetime(2024, 1, 6, 12, 0, 6),
                b"\x06",
            ),
            (
                7,
                "g2",
                700,
                7.5,
                Decimal("7.25"),
                False,
                "n7",
                datetime.date(2024, 1, 7),
                datetime.datetime(2024, 1, 7, 12, 0, 7),
                b"\x07",
            ),
            (
                8,
                None,
                800,
                8.5,
                Decimal("8.25"),
                True,
                "n8",
                datetime.date(2024, 1, 8),
                datetime.datetime(2024, 1, 8, 12, 0, 8),
                b"\x08",
            ),
        ],
    ),
    "select_special_names": (
        "SELECT id, `spaced col`, `dotted.col` FROM {t} ORDER BY id",
        [
            (1, 10, "d1"),
            (2, 20, "d2"),
            (3, 30, "d3"),
            (4, 40, "d4"),
            (5, 50, "d5"),
            (6, 60, "d6"),
            (7, 70, "d7"),
            (8, 80, "d8"),
        ],
    ),
    "select_struct": (
        "SELECT id, s FROM {t} ORDER BY id",
        [
            (1, Row(a=1, b="x", inner=Row(c=10, d="inner1"))),
            (2, Row(a=2, b="y", inner=Row(c=20, d="inner2"))),
            (3, Row(a=3, b="z", inner=Row(c=30, d="inner3"))),
            (4, Row(a=4, b="w", inner=Row(c=40, d="inner4"))),
            (5, Row(a=1, b="x", inner=Row(c=10, d="inner5"))),
            (6, Row(a=6, b="v", inner=Row(c=60, d="inner6"))),
            (7, None),
            (8, Row(a=8, b="t", inner=Row(c=80, d="inner8"))),
        ],
    ),
    "select_struct_fields": (
        "SELECT id, s.a, s.b, s.inner, s.inner.c, s.inner.d FROM {t} ORDER BY id",
        [
            (1, 1, "x", Row(c=10, d="inner1"), 10, "inner1"),
            (2, 2, "y", Row(c=20, d="inner2"), 20, "inner2"),
            (3, 3, "z", Row(c=30, d="inner3"), 30, "inner3"),
            (4, 4, "w", Row(c=40, d="inner4"), 40, "inner4"),
            (5, 1, "x", Row(c=10, d="inner5"), 10, "inner5"),
            (6, 6, "v", Row(c=60, d="inner6"), 60, "inner6"),
            (7, None, None, None, None, None),
            (8, 8, "t", Row(c=80, d="inner8"), 80, "inner8"),
        ],
    ),
    "select_struct_star": (
        "SELECT id, s.* FROM {t} ORDER BY id",
        [
            (1, 1, "x", Row(c=10, d="inner1")),
            (2, 2, "y", Row(c=20, d="inner2")),
            (3, 3, "z", Row(c=30, d="inner3")),
            (4, 4, "w", Row(c=40, d="inner4")),
            (5, 1, "x", Row(c=10, d="inner5")),
            (6, 6, "v", Row(c=60, d="inner6")),
            (7, None, None, None),
            (8, 8, "t", Row(c=80, d="inner8")),
        ],
    ),
    "select_arrays": (
        "SELECT id, arr, arr_s FROM {t} ORDER BY id",
        [
            (1, [1, 2], [Row(x=1, y="y1"), Row(x=11, y="y11")]),
            (2, [2, 3], [Row(x=2, y="y2"), Row(x=12, y="y12")]),
            (3, [], []),
            (4, [4, 5], [Row(x=4, y="y4"), Row(x=14, y="y14")]),
            (5, [5, 6], [Row(x=5, y="y5"), Row(x=15, y="y15")]),
            (6, [6, 7], [Row(x=6, y="y6"), Row(x=16, y="y16")]),
            (7, None, None),
            (8, [8, None], [Row(x=8, y="y8"), Row(x=18, y="y18")]),
        ],
    ),
    "select_array_elements": (
        "SELECT id, get(arr, 0) AS a0, get(arr_s, 0) AS e0, get(arr_s, 0).x AS x0, arr_s.y AS ys FROM {t} ORDER BY id",
        [
            (1, 1, Row(x=1, y="y1"), 1, ["y1", "y11"]),
            (2, 2, Row(x=2, y="y2"), 2, ["y2", "y12"]),
            (3, None, None, None, []),
            (4, 4, Row(x=4, y="y4"), 4, ["y4", "y14"]),
            (5, 5, Row(x=5, y="y5"), 5, ["y5", "y15"]),
            (6, 6, Row(x=6, y="y6"), 6, ["y6", "y16"]),
            (7, None, None, None, None),
            (8, 8, Row(x=8, y="y8"), 8, ["y8", "y18"]),
        ],
    ),
    "select_element_at": (
        "SELECT id, try_element_at(arr, 1) AS a1, try_element_at(arr_s, -1).y AS last_y FROM {t} ORDER BY id",
        [
            (1, 1, "y11"),
            (2, 2, "y12"),
            (3, None, None),
            (4, 4, "y14"),
            (5, 5, "y15"),
            (6, 6, "y16"),
            (7, None, None),
            (8, 8, "y18"),
        ],
    ),
    "select_maps": (
        "SELECT id, m, m_s, m_arr FROM {t} ORDER BY id",
        [
            (1, {"k1": 1, "k2": 2}, {"k1": Row(p=1, q="q1")}, {"k1": [1, 1]}),
            (2, {"k1": 2, "k2": 4}, {"k1": Row(p=2, q="q2")}, {"k1": [2, 2]}),
            (3, {}, {}, {"k1": [3, 3]}),
            (4, {"k1": 4, "k2": 8}, {"k1": Row(p=4, q="q4")}, {"k1": [4, 4]}),
            (5, {"k1": 5, "k2": 10}, {"k1": Row(p=5, q="q5")}, {"k1": [5, 5]}),
            (6, {"k1": 6, "k2": 12}, {"k1": Row(p=6, q="q6")}, {"k1": [6, 6]}),
            (7, None, None, None),
            (8, {"k1": None}, {"k1": Row(p=8, q="q8")}, {"k1": [8, 8]}),
        ],
    ),
    "select_map_values": (
        "SELECT id, m['k1'] AS k1, m_s['k1'] AS s1, m_s['k1'].q AS q1, m_arr['k1'] AS a1 FROM {t} ORDER BY id",
        [
            (1, 1, Row(p=1, q="q1"), "q1", [1, 1]),
            (2, 2, Row(p=2, q="q2"), "q2", [2, 2]),
            (3, None, None, None, [3, 3]),
            (4, 4, Row(p=4, q="q4"), "q4", [4, 4]),
            (5, 5, Row(p=5, q="q5"), "q5", [5, 5]),
            (6, 6, Row(p=6, q="q6"), "q6", [6, 6]),
            (7, None, None, None, None),
            (8, None, Row(p=8, q="q8"), "q8", [8, 8]),
        ],
    ),
    "select_map_functions": (
        "SELECT id, map_keys(m) AS ks, map_values(m_s) AS vs, map_entries(m_arr) AS es FROM {t} ORDER BY id",
        [
            (1, ["k1", "k2"], [Row(p=1, q="q1")], [Row(key="k1", value=[1, 1])]),
            (2, ["k1", "k2"], [Row(p=2, q="q2")], [Row(key="k1", value=[2, 2])]),
            (3, [], [], [Row(key="k1", value=[3, 3])]),
            (4, ["k1", "k2"], [Row(p=4, q="q4")], [Row(key="k1", value=[4, 4])]),
            (5, ["k1", "k2"], [Row(p=5, q="q5")], [Row(key="k1", value=[5, 5])]),
            (6, ["k1", "k2"], [Row(p=6, q="q6")], [Row(key="k1", value=[6, 6])]),
            (7, None, None, None),
            (8, ["k1"], [Row(p=8, q="q8")], [Row(key="k1", value=[8, 8])]),
        ],
    ),
    "select_nested": (
        "SELECT id, nested, nested.inner_map['a'] AS a, nested.inner_arr[0].z AS z FROM {t} ORDER BY id",
        [
            (1, Row(inner_map={"a": "A1"}, inner_arr=[Row(z=1)]), "A1", 1),
            (2, Row(inner_map={"a": "A2"}, inner_arr=[Row(z=2)]), "A2", 2),
            (3, Row(inner_map={"a": "A3"}, inner_arr=[Row(z=3)]), "A3", 3),
            (4, Row(inner_map={"a": "A4"}, inner_arr=[Row(z=4)]), "A4", 4),
            (5, Row(inner_map={"a": "A5"}, inner_arr=[Row(z=5)]), "A5", 5),
            (6, Row(inner_map={"a": "A6"}, inner_arr=[Row(z=6)]), "A6", 6),
            (7, None, None, None),
            (8, Row(inner_map={"a": "A8"}, inner_arr=[Row(z=8)]), "A8", 8),
        ],
    ),
    "select_expressions": (
        "SELECT id, big + id AS x, upper(name) AS u, s.a * 2 AS a2, concat(s.b, s.inner.d) AS bd FROM {t} ORDER BY id",
        [
            (1, 101, "N1", 2, "xinner1"),
            (2, 202, "N2", 4, "yinner2"),
            (3, 303, "N3", 6, "zinner3"),
            (4, 404, "N4", 8, "winner4"),
            (5, 505, "N5", 2, "xinner5"),
            (6, 606, None, 12, "vinner6"),
            (7, 707, "N7", None, None),
            (8, 808, "N8", 16, "tinner8"),
        ],
    ),
    "select_casts": (
        "SELECT id, CAST(s AS STRING) AS s1, CAST(arr AS STRING) AS a1, CAST(m AS STRING) AS m1,"
        " to_json(s) AS j1, to_json(arr_s) AS j2 FROM {t} ORDER BY id",
        [
            (
                1,
                "{1, x, {10, inner1}}",
                "[1, 2]",
                "{k1 -> 1, k2 -> 2}",
                '{"a":1,"b":"x","inner":{"c":10,"d":"inner1"}}',
                '[{"x":1,"y":"y1"},{"x":11,"y":"y11"}]',
            ),
            (
                2,
                "{2, y, {20, inner2}}",
                "[2, 3]",
                "{k1 -> 2, k2 -> 4}",
                '{"a":2,"b":"y","inner":{"c":20,"d":"inner2"}}',
                '[{"x":2,"y":"y2"},{"x":12,"y":"y12"}]',
            ),
            (3, "{3, z, {30, inner3}}", "[]", "{}", '{"a":3,"b":"z","inner":{"c":30,"d":"inner3"}}', "[]"),
            (
                4,
                "{4, w, {40, inner4}}",
                "[4, 5]",
                "{k1 -> 4, k2 -> 8}",
                '{"a":4,"b":"w","inner":{"c":40,"d":"inner4"}}',
                '[{"x":4,"y":"y4"},{"x":14,"y":"y14"}]',
            ),
            (
                5,
                "{1, x, {10, inner5}}",
                "[5, 6]",
                "{k1 -> 5, k2 -> 10}",
                '{"a":1,"b":"x","inner":{"c":10,"d":"inner5"}}',
                '[{"x":5,"y":"y5"},{"x":15,"y":"y15"}]',
            ),
            (
                6,
                "{6, v, {60, inner6}}",
                "[6, 7]",
                "{k1 -> 6, k2 -> 12}",
                '{"a":6,"b":"v","inner":{"c":60,"d":"inner6"}}',
                '[{"x":6,"y":"y6"},{"x":16,"y":"y16"}]',
            ),
            (7, None, None, None, None, None),
            (
                8,
                "{8, t, {80, inner8}}",
                "[8, null]",
                "{k1 -> null}",
                '{"a":8,"b":"t","inner":{"c":80,"d":"inner8"}}',
                '[{"x":8,"y":"y8"},{"x":18,"y":"y18"}]',
            ),
        ],
    ),
    "select_struct_construction": (
        "SELECT id, named_struct('k', s.a, 'v', arr_s) AS n, struct(s.b, s.inner) AS st FROM {t} ORDER BY id",
        [
            (1, Row(k=1, v=[Row(x=1, y="y1"), Row(x=11, y="y11")]), Row(b="x", inner=Row(c=10, d="inner1"))),
            (2, Row(k=2, v=[Row(x=2, y="y2"), Row(x=12, y="y12")]), Row(b="y", inner=Row(c=20, d="inner2"))),
            (3, Row(k=3, v=[]), Row(b="z", inner=Row(c=30, d="inner3"))),
            (4, Row(k=4, v=[Row(x=4, y="y4"), Row(x=14, y="y14")]), Row(b="w", inner=Row(c=40, d="inner4"))),
            (5, Row(k=1, v=[Row(x=5, y="y5"), Row(x=15, y="y15")]), Row(b="x", inner=Row(c=10, d="inner5"))),
            (6, Row(k=6, v=[Row(x=6, y="y6"), Row(x=16, y="y16")]), Row(b="v", inner=Row(c=60, d="inner6"))),
            (7, Row(k=None, v=None), Row(b=None, inner=None)),
            (8, Row(k=8, v=[Row(x=8, y="y8"), Row(x=18, y="y18")]), Row(b="t", inner=Row(c=80, d="inner8"))),
        ],
    ),
    # conditional expressions on nested values
    "case_struct": (
        "SELECT id, CASE WHEN s.a > 2 THEN s END AS s2 FROM {t} ORDER BY id",
        [
            (1, None),
            (2, None),
            (3, Row(a=3, b="z", inner=Row(c=30, d="inner3"))),
            (4, Row(a=4, b="w", inner=Row(c=40, d="inner4"))),
            (5, None),
            (6, Row(a=6, b="v", inner=Row(c=60, d="inner6"))),
            (7, None),
            (8, Row(a=8, b="t", inner=Row(c=80, d="inner8"))),
        ],
    ),
    "case_struct_literal": (
        "SELECT id, CASE WHEN s.a > 2 THEN s.inner"
        " ELSE named_struct('c', 0, 'd', 'none') END AS i2 FROM {t} ORDER BY id",
        [
            (1, Row(c=0, d="none")),
            (2, Row(c=0, d="none")),
            (3, Row(c=30, d="inner3")),
            (4, Row(c=40, d="inner4")),
            (5, Row(c=0, d="none")),
            (6, Row(c=60, d="inner6")),
            (7, Row(c=0, d="none")),
            (8, Row(c=80, d="inner8")),
        ],
    ),
    "coalesce_struct": (
        "SELECT id, coalesce(s.inner, named_struct('c', -1, 'd', 'missing')) AS i2 FROM {t} ORDER BY id",
        [
            (1, Row(c=10, d="inner1")),
            (2, Row(c=20, d="inner2")),
            (3, Row(c=30, d="inner3")),
            (4, Row(c=40, d="inner4")),
            (5, Row(c=10, d="inner5")),
            (6, Row(c=60, d="inner6")),
            (7, Row(c=-1, d="missing")),
            (8, Row(c=80, d="inner8")),
        ],
    ),
    "coalesce_whole_struct": (
        "SELECT id, coalesce(s, named_struct('a', 0, 'b', '',"
        " 'inner', named_struct('c', 0, 'd', ''))) AS s2 FROM {t} ORDER BY id",
        [
            (1, Row(a=1, b="x", inner=Row(c=10, d="inner1"))),
            (2, Row(a=2, b="y", inner=Row(c=20, d="inner2"))),
            (3, Row(a=3, b="z", inner=Row(c=30, d="inner3"))),
            (4, Row(a=4, b="w", inner=Row(c=40, d="inner4"))),
            (5, Row(a=1, b="x", inner=Row(c=10, d="inner5"))),
            (6, Row(a=6, b="v", inner=Row(c=60, d="inner6"))),
            (7, Row(a=0, b="", inner=Row(c=0, d=""))),
            (8, Row(a=8, b="t", inner=Row(c=80, d="inner8"))),
        ],
    ),
    "if_struct": (
        "SELECT id, if(flag, s.inner, NULL) AS i2 FROM {t} ORDER BY id",
        [
            (1, None),
            (2, Row(c=20, d="inner2")),
            (3, None),
            (4, Row(c=40, d="inner4")),
            (5, None),
            (6, None),
            (7, None),
            (8, Row(c=80, d="inner8")),
        ],
    ),
    "nvl_array_struct": (
        "SELECT id, nvl(arr_s, array(named_struct('x', 0, 'y', ''))) AS a2 FROM {t} ORDER BY id",
        [
            (1, [Row(x=1, y="y1"), Row(x=11, y="y11")]),
            (2, [Row(x=2, y="y2"), Row(x=12, y="y12")]),
            (3, []),
            (4, [Row(x=4, y="y4"), Row(x=14, y="y14")]),
            (5, [Row(x=5, y="y5"), Row(x=15, y="y15")]),
            (6, [Row(x=6, y="y6"), Row(x=16, y="y16")]),
            (7, [Row(x=0, y="")]),
            (8, [Row(x=8, y="y8"), Row(x=18, y="y18")]),
        ],
    ),
    "coalesce_map": (
        "SELECT id, coalesce(m_s, map('none', named_struct('p', 0, 'q', ''))) AS m2 FROM {t} ORDER BY id",
        [
            (1, {"k1": Row(p=1, q="q1")}),
            (2, {"k1": Row(p=2, q="q2")}),
            (3, {}),
            (4, {"k1": Row(p=4, q="q4")}),
            (5, {"k1": Row(p=5, q="q5")}),
            (6, {"k1": Row(p=6, q="q6")}),
            (7, {"none": Row(p=0, q="")}),
            (8, {"k1": Row(p=8, q="q8")}),
        ],
    ),
    # filters
    "filter_scalar": (
        "SELECT id FROM {t} WHERE big > 200 AND dbl < 6 ORDER BY id",
        [(3,), (4,), (5,)],
    ),
    "filter_partition_column": (
        "SELECT id FROM {t} WHERE grp = 'g2' ORDER BY id",
        [(3,), (5,), (7,)],
    ),
    "filter_partition_null": (
        "SELECT id FROM {t} WHERE grp IS NULL ORDER BY id",
        [(4,), (8,)],
    ),
    "filter_in": (
        "SELECT id FROM {t} WHERE name IN ('n1', 'n3', 'n8') ORDER BY id",
        [(1,), (3,), (8,)],
    ),
    "filter_between_date": (
        "SELECT id FROM {t} WHERE dt BETWEEN DATE '2024-01-02' AND DATE '2024-01-05' ORDER BY id",
        [(2,), (3,), (4,), (5,)],
    ),
    "filter_like": (
        "SELECT id FROM {t} WHERE `dotted.col` LIKE 'd%' AND `spaced col` >= 30 ORDER BY id",
        [(3,), (4,), (5,), (6,), (7,), (8,)],
    ),
    "filter_struct_field": (
        "SELECT id FROM {t} WHERE s.a = 1 ORDER BY id",
        [(1,), (5,)],
    ),
    "filter_deep_struct_field": (
        "SELECT id FROM {t} WHERE s.inner.c > 20 AND s.inner.d <> 'inner7' ORDER BY id",
        [(3,), (4,), (6,), (8,)],
    ),
    "filter_struct_equality": (
        "SELECT id FROM {t} WHERE s = named_struct('a', 1, 'b', 'x',"
        " 'inner', named_struct('c', 10, 'd', 'inner1')) ORDER BY id",
        [(1,)],
    ),
    "filter_inner_struct_equality": (
        "SELECT id FROM {t} WHERE s.inner = named_struct('c', 20, 'd', 'inner2') ORDER BY id",
        [(2,)],
    ),
    "filter_struct_in": (
        "SELECT id FROM {t} WHERE s.inner IN (named_struct('c', 10, 'd', 'inner1'),"
        " named_struct('c', 30, 'd', 'inner3')) ORDER BY id",
        [(1,), (3,)],
    ),
    "filter_struct_is_null": (
        "SELECT id FROM {t} WHERE s IS NULL OR s.inner IS NULL ORDER BY id",
        [(7,)],
    ),
    "filter_array_struct_is_null": (
        "SELECT id FROM {t} WHERE arr_s IS NULL ORDER BY id",
        [(7,)],
    ),
    "filter_map_struct_is_not_null": (
        "SELECT id FROM {t} WHERE m_s IS NOT NULL ORDER BY id",
        [(1,), (2,), (3,), (4,), (5,), (6,), (8,)],
    ),
    "filter_array": (
        "SELECT id FROM {t} WHERE array_contains(arr, 2) OR size(arr_s) = 0 ORDER BY id",
        [(1,), (2,), (3,)],
    ),
    "filter_array_struct": (
        "SELECT id FROM {t} WHERE exists(arr_s, e -> e.x > 12) ORDER BY id",
        [(4,), (5,), (6,), (8,)],
    ),
    "filter_map": (
        "SELECT id FROM {t} WHERE m['k2'] >= 8 OR m_s['k1'].p = 1 ORDER BY id",
        [(1,), (4,), (5,), (6,)],
    ),
    "filter_nested_map": (
        "SELECT id FROM {t} WHERE nested.inner_map['a'] = 'A3' ORDER BY id",
        [(3,)],
    ),
    "filter_or_across_columns": (
        "SELECT id FROM {t} WHERE grp = 'g1' OR s.b = 'u' OR flag ORDER BY id",
        [(1,), (2,), (4,), (6,), (8,)],
    ),
    # aggregation
    "count": (
        "SELECT count(*) AS n, count(s) AS ns, count(s.a) AS na, count(arr) AS narr FROM {t}",
        [(8, 7, 7, 7)],
    ),
    "aggregate_scalars": (
        "SELECT sum(big) AS sb, avg(dbl) AS ad, sum(dec) AS sd, min(dt) AS mn, max(ts) AS mx FROM {t}",
        [
            (
                3600,
                4.785714285714286,
                Decimal("38.00"),
                datetime.date(2024, 1, 1),
                datetime.datetime(2024, 1, 8, 12, 0, 8),
            )
        ],
    ),
    "group_by_partition": (
        "SELECT grp, count(*) AS n, sum(s.a) AS sa, max(s.inner.c) AS mc FROM {t} GROUP BY grp ORDER BY grp",
        [(None, 2, 12, 80), ("g1", 3, 9, 60), ("g2", 3, 4, 30)],
    ),
    "group_by_struct_field": (
        "SELECT s.b, count(*) AS n FROM {t} GROUP BY s.b ORDER BY s.b",
        [(None, 1), ("t", 1), ("v", 1), ("w", 1), ("x", 2), ("y", 1), ("z", 1)],
    ),
    "group_by_struct": (
        "SELECT s, count(*) AS n FROM {t} GROUP BY s ORDER BY s.a, s.inner.d",
        [
            (None, 1),
            (Row(a=1, b="x", inner=Row(c=10, d="inner1")), 1),
            (Row(a=1, b="x", inner=Row(c=10, d="inner5")), 1),
            (Row(a=2, b="y", inner=Row(c=20, d="inner2")), 1),
            (Row(a=3, b="z", inner=Row(c=30, d="inner3")), 1),
            (Row(a=4, b="w", inner=Row(c=40, d="inner4")), 1),
            (Row(a=6, b="v", inner=Row(c=60, d="inner6")), 1),
            (Row(a=8, b="t", inner=Row(c=80, d="inner8")), 1),
        ],
    ),
    "group_by_having": (
        "SELECT grp, count(*) AS n FROM {t} GROUP BY grp HAVING count(*) > 2 ORDER BY grp",
        [("g1", 3), ("g2", 3)],
    ),
    "distinct_scalar": (
        "SELECT DISTINCT grp FROM {t} ORDER BY grp",
        [(None,), ("g1",), ("g2",)],
    ),
    "distinct_struct": (
        "SELECT DISTINCT s.a, s.b FROM {t} ORDER BY a, b",
        [(None, None), (1, "x"), (2, "y"), (3, "z"), (4, "w"), (6, "v"), (8, "t")],
    ),
    "distinct_whole_struct": (
        "SELECT DISTINCT s FROM {t} ORDER BY s.a, s.inner.d",
        [
            (None,),
            (Row(a=1, b="x", inner=Row(c=10, d="inner1")),),
            (Row(a=1, b="x", inner=Row(c=10, d="inner5")),),
            (Row(a=2, b="y", inner=Row(c=20, d="inner2")),),
            (Row(a=3, b="z", inner=Row(c=30, d="inner3")),),
            (Row(a=4, b="w", inner=Row(c=40, d="inner4")),),
            (Row(a=6, b="v", inner=Row(c=60, d="inner6")),),
            (Row(a=8, b="t", inner=Row(c=80, d="inner8")),),
        ],
    ),
    "count_distinct_struct": (
        "SELECT count(DISTINCT s) AS n, count(DISTINCT s.b) AS nb FROM {t}",
        [(7, 6)],
    ),
    "min_max_struct": (
        "SELECT min(s) AS mn, max(s) AS mx, min(s.inner) AS mi FROM {t}",
        [
            (
                Row(a=1, b="x", inner=Row(c=10, d="inner1")),
                Row(a=8, b="t", inner=Row(c=80, d="inner8")),
                Row(c=10, d="inner1"),
            )
        ],
    ),
    "collect_list": (
        "SELECT grp, sort_array(collect_list(s.a)) AS l, sort_array(collect_set(s.b)) AS st"
        " FROM {t} GROUP BY grp ORDER BY grp",
        [(None, [4, 8], ["t", "w"]), ("g1", [1, 2, 6], ["v", "x", "y"]), ("g2", [1, 3], ["x", "z"])],
    ),
    "first_struct": (
        "SELECT first(s) FILTER (WHERE id = 1) AS f, first(arr_s) FILTER (WHERE id = 1) AS fa FROM {t}",
        [(Row(a=1, b="x", inner=Row(c=10, d="inner1")), [Row(x=1, y="y1"), Row(x=11, y="y11")])],
    ),
    # sorting and limits
    "order_by_struct_field": (
        "SELECT id FROM {t} ORDER BY s.inner.c DESC NULLS LAST, id",
        [(8,), (6,), (4,), (3,), (2,), (1,), (5,), (7,)],
    ),
    "order_by_struct": (
        "SELECT id FROM {t} ORDER BY s, id",
        [(7,), (1,), (5,), (2,), (3,), (4,), (6,), (8,)],
    ),
    "order_by_limit": (
        "SELECT id, s, m FROM {t} ORDER BY dbl DESC NULLS LAST, id LIMIT 3",
        [
            (8, Row(a=8, b="t", inner=Row(c=80, d="inner8")), {"k1": None}),
            (7, None, None),
            (5, Row(a=1, b="x", inner=Row(c=10, d="inner5")), {"k1": 5, "k2": 10}),
        ],
    ),
    "limit_offset": (
        "SELECT id FROM {t} ORDER BY id LIMIT 3 OFFSET 2",
        [(3,), (4,), (5,)],
    ),
    # windows
    "window_row_number": (
        "SELECT id, row_number() OVER (PARTITION BY grp ORDER BY s.a, id) AS rn FROM {t} ORDER BY id",
        [(1, 1), (2, 2), (3, 3), (4, 1), (5, 2), (6, 3), (7, 1), (8, 2)],
    ),
    "window_struct": (
        "SELECT id, max(s) OVER (PARTITION BY grp) AS mx, lag(s.inner) OVER (ORDER BY id) AS prev FROM {t} ORDER BY id",
        [
            (1, Row(a=6, b="v", inner=Row(c=60, d="inner6")), None),
            (2, Row(a=6, b="v", inner=Row(c=60, d="inner6")), Row(c=10, d="inner1")),
            (3, Row(a=3, b="z", inner=Row(c=30, d="inner3")), Row(c=20, d="inner2")),
            (4, Row(a=8, b="t", inner=Row(c=80, d="inner8")), Row(c=30, d="inner3")),
            (5, Row(a=3, b="z", inner=Row(c=30, d="inner3")), Row(c=40, d="inner4")),
            (6, Row(a=6, b="v", inner=Row(c=60, d="inner6")), Row(c=10, d="inner5")),
            (7, Row(a=3, b="z", inner=Row(c=30, d="inner3")), Row(c=60, d="inner6")),
            (8, Row(a=8, b="t", inner=Row(c=80, d="inner8")), None),
        ],
    ),
    "window_running_sum": (
        "SELECT id, sum(s.a) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
        " AS total FROM {t} ORDER BY id",
        [(1, 1), (2, 3), (3, 6), (4, 10), (5, 11), (6, 17), (7, 17), (8, 25)],
    ),
    # generators and higher-order functions
    "explode_array": (
        "SELECT id, v FROM {t} LATERAL VIEW explode(arr) AS v ORDER BY id, v",
        [
            (1, 1),
            (1, 2),
            (2, 2),
            (2, 3),
            (4, 4),
            (4, 5),
            (5, 5),
            (5, 6),
            (6, 6),
            (6, 7),
            (8, None),
            (8, 8),
        ],
    ),
    "explode_array_struct": (
        "SELECT id, e.x, e.y FROM {t} LATERAL VIEW explode(arr_s) AS e ORDER BY id, e.x",
        [
            (1, 1, "y1"),
            (1, 11, "y11"),
            (2, 2, "y2"),
            (2, 12, "y12"),
            (4, 4, "y4"),
            (4, 14, "y14"),
            (5, 5, "y5"),
            (5, 15, "y15"),
            (6, 6, "y6"),
            (6, 16, "y16"),
            (8, 8, "y8"),
            (8, 18, "y18"),
        ],
    ),
    "posexplode": (
        "SELECT id, pos, e FROM {t} LATERAL VIEW posexplode(arr_s) AS pos, e ORDER BY id, pos",
        [
            (1, 0, Row(x=1, y="y1")),
            (1, 1, Row(x=11, y="y11")),
            (2, 0, Row(x=2, y="y2")),
            (2, 1, Row(x=12, y="y12")),
            (4, 0, Row(x=4, y="y4")),
            (4, 1, Row(x=14, y="y14")),
            (5, 0, Row(x=5, y="y5")),
            (5, 1, Row(x=15, y="y15")),
            (6, 0, Row(x=6, y="y6")),
            (6, 1, Row(x=16, y="y16")),
            (8, 0, Row(x=8, y="y8")),
            (8, 1, Row(x=18, y="y18")),
        ],
    ),
    "inline": (
        "SELECT id, inline(arr_s) FROM {t} ORDER BY id, x",
        [
            (1, 1, "y1"),
            (1, 11, "y11"),
            (2, 2, "y2"),
            (2, 12, "y12"),
            (4, 4, "y4"),
            (4, 14, "y14"),
            (5, 5, "y5"),
            (5, 15, "y15"),
            (6, 6, "y6"),
            (6, 16, "y16"),
            (8, 8, "y8"),
            (8, 18, "y18"),
        ],
    ),
    "explode_map": (
        "SELECT id, k, v FROM {t} LATERAL VIEW explode(m_s) AS k, v ORDER BY id, k",
        [
            (1, "k1", Row(p=1, q="q1")),
            (2, "k1", Row(p=2, q="q2")),
            (4, "k1", Row(p=4, q="q4")),
            (5, "k1", Row(p=5, q="q5")),
            (6, "k1", Row(p=6, q="q6")),
            (8, "k1", Row(p=8, q="q8")),
        ],
    ),
    "explode_outer": (
        "SELECT id, e FROM {t} LATERAL VIEW OUTER explode(arr_s) AS e ORDER BY id, e.x",
        [
            (1, Row(x=1, y="y1")),
            (1, Row(x=11, y="y11")),
            (2, Row(x=2, y="y2")),
            (2, Row(x=12, y="y12")),
            (3, None),
            (4, Row(x=4, y="y4")),
            (4, Row(x=14, y="y14")),
            (5, Row(x=5, y="y5")),
            (5, Row(x=15, y="y15")),
            (6, Row(x=6, y="y6")),
            (6, Row(x=16, y="y16")),
            (7, None),
            (8, Row(x=8, y="y8")),
            (8, Row(x=18, y="y18")),
        ],
    ),
    "higher_order": (
        "SELECT id, transform(arr_s, e -> e.x + 1) AS xs, filter(arr_s, e -> e.x < 10) AS f,"
        " aggregate(arr, 0, (acc, v) -> acc + coalesce(v, 0)) AS total FROM {t} ORDER BY id",
        [
            (1, [2, 12], [Row(x=1, y="y1")], 3),
            (2, [3, 13], [Row(x=2, y="y2")], 5),
            (3, [], [], 0),
            (4, [5, 15], [Row(x=4, y="y4")], 9),
            (5, [6, 16], [Row(x=5, y="y5")], 11),
            (6, [7, 17], [Row(x=6, y="y6")], 13),
            (7, None, None, None),
            (8, [9, 19], [Row(x=8, y="y8")], 8),
        ],
    ),
    "sort_array_struct": (
        "SELECT id, sort_array(arr_s, false) AS sorted FROM {t} ORDER BY id",
        [
            (1, [Row(x=11, y="y11"), Row(x=1, y="y1")]),
            (2, [Row(x=12, y="y12"), Row(x=2, y="y2")]),
            (3, []),
            (4, [Row(x=14, y="y14"), Row(x=4, y="y4")]),
            (5, [Row(x=15, y="y15"), Row(x=5, y="y5")]),
            (6, [Row(x=16, y="y16"), Row(x=6, y="y6")]),
            (7, None),
            (8, [Row(x=18, y="y18"), Row(x=8, y="y8")]),
        ],
    ),
    # joins and set operations
    "self_join_scalar": (
        "SELECT a.id, b.name FROM {t} a JOIN {t} b ON a.id = b.id ORDER BY a.id",
        [(1, "n1"), (2, "n2"), (3, "n3"), (4, "n4"), (5, "n5"), (6, None), (7, "n7"), (8, "n8")],
    ),
    "self_join_struct": (
        "SELECT a.id AS l, b.id AS r FROM {t} a JOIN {t} b ON a.s = b.s ORDER BY l, r",
        [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (8, 8)],
    ),
    "self_join_struct_field": (
        "SELECT a.id AS l, b.id AS r FROM {t} a JOIN {t} b ON a.s.a = b.s.a AND a.id < b.id ORDER BY l, r",
        [(1, 5)],
    ),
    "left_join_nested": (
        "SELECT a.id, b.arr_s FROM {t} a LEFT JOIN {t} b ON a.id = b.id + 1 ORDER BY a.id",
        [
            (1, None),
            (2, [Row(x=1, y="y1"), Row(x=11, y="y11")]),
            (3, [Row(x=2, y="y2"), Row(x=12, y="y12")]),
            (4, []),
            (5, [Row(x=4, y="y4"), Row(x=14, y="y14")]),
            (6, [Row(x=5, y="y5"), Row(x=15, y="y15")]),
            (7, [Row(x=6, y="y6"), Row(x=16, y="y16")]),
            (8, None),
        ],
    ),
    "semi_join": (
        "SELECT id FROM {t} a WHERE EXISTS (SELECT 1 FROM {t} b WHERE b.s.a = a.s.a AND b.id <> a.id) ORDER BY id",
        [(1,), (5,)],
    ),
    "in_subquery": (
        "SELECT id FROM {t} WHERE s.a IN (SELECT s.a FROM {t} WHERE grp = 'g2') ORDER BY id",
        [(1,), (3,), (5,)],
    ),
    "scalar_subquery": (
        "SELECT id, (SELECT max(s.inner.c) FROM {t}) AS mx FROM {t} ORDER BY id",
        [(1, 80), (2, 80), (3, 80), (4, 80), (5, 80), (6, 80), (7, 80), (8, 80)],
    ),
    "union_all": (
        "SELECT id, s, arr_s, m_s FROM {t} UNION ALL SELECT id, s, arr_s, m_s FROM {t} ORDER BY id",
        [
            (
                1,
                Row(a=1, b="x", inner=Row(c=10, d="inner1")),
                [Row(x=1, y="y1"), Row(x=11, y="y11")],
                {"k1": Row(p=1, q="q1")},
            ),
            (
                1,
                Row(a=1, b="x", inner=Row(c=10, d="inner1")),
                [Row(x=1, y="y1"), Row(x=11, y="y11")],
                {"k1": Row(p=1, q="q1")},
            ),
            (
                2,
                Row(a=2, b="y", inner=Row(c=20, d="inner2")),
                [Row(x=2, y="y2"), Row(x=12, y="y12")],
                {"k1": Row(p=2, q="q2")},
            ),
            (
                2,
                Row(a=2, b="y", inner=Row(c=20, d="inner2")),
                [Row(x=2, y="y2"), Row(x=12, y="y12")],
                {"k1": Row(p=2, q="q2")},
            ),
            (3, Row(a=3, b="z", inner=Row(c=30, d="inner3")), [], {}),
            (3, Row(a=3, b="z", inner=Row(c=30, d="inner3")), [], {}),
            (
                4,
                Row(a=4, b="w", inner=Row(c=40, d="inner4")),
                [Row(x=4, y="y4"), Row(x=14, y="y14")],
                {"k1": Row(p=4, q="q4")},
            ),
            (
                4,
                Row(a=4, b="w", inner=Row(c=40, d="inner4")),
                [Row(x=4, y="y4"), Row(x=14, y="y14")],
                {"k1": Row(p=4, q="q4")},
            ),
            (
                5,
                Row(a=1, b="x", inner=Row(c=10, d="inner5")),
                [Row(x=5, y="y5"), Row(x=15, y="y15")],
                {"k1": Row(p=5, q="q5")},
            ),
            (
                5,
                Row(a=1, b="x", inner=Row(c=10, d="inner5")),
                [Row(x=5, y="y5"), Row(x=15, y="y15")],
                {"k1": Row(p=5, q="q5")},
            ),
            (
                6,
                Row(a=6, b="v", inner=Row(c=60, d="inner6")),
                [Row(x=6, y="y6"), Row(x=16, y="y16")],
                {"k1": Row(p=6, q="q6")},
            ),
            (
                6,
                Row(a=6, b="v", inner=Row(c=60, d="inner6")),
                [Row(x=6, y="y6"), Row(x=16, y="y16")],
                {"k1": Row(p=6, q="q6")},
            ),
            (7, None, None, None),
            (7, None, None, None),
            (
                8,
                Row(a=8, b="t", inner=Row(c=80, d="inner8")),
                [Row(x=8, y="y8"), Row(x=18, y="y18")],
                {"k1": Row(p=8, q="q8")},
            ),
            (
                8,
                Row(a=8, b="t", inner=Row(c=80, d="inner8")),
                [Row(x=8, y="y8"), Row(x=18, y="y18")],
                {"k1": Row(p=8, q="q8")},
            ),
        ],
    ),
    "union": (
        "SELECT id, s, arr_s FROM {t} UNION SELECT id, s, arr_s FROM {t} ORDER BY id",
        [
            (1, Row(a=1, b="x", inner=Row(c=10, d="inner1")), [Row(x=1, y="y1"), Row(x=11, y="y11")]),
            (2, Row(a=2, b="y", inner=Row(c=20, d="inner2")), [Row(x=2, y="y2"), Row(x=12, y="y12")]),
            (3, Row(a=3, b="z", inner=Row(c=30, d="inner3")), []),
            (4, Row(a=4, b="w", inner=Row(c=40, d="inner4")), [Row(x=4, y="y4"), Row(x=14, y="y14")]),
            (5, Row(a=1, b="x", inner=Row(c=10, d="inner5")), [Row(x=5, y="y5"), Row(x=15, y="y15")]),
            (6, Row(a=6, b="v", inner=Row(c=60, d="inner6")), [Row(x=6, y="y6"), Row(x=16, y="y16")]),
            (7, None, None),
            (8, Row(a=8, b="t", inner=Row(c=80, d="inner8")), [Row(x=8, y="y8"), Row(x=18, y="y18")]),
        ],
    ),
    "intersect": (
        "SELECT id, s, arr_s FROM {t} INTERSECT SELECT id, s, arr_s FROM {t} WHERE id > 2 ORDER BY id",
        [
            (3, Row(a=3, b="z", inner=Row(c=30, d="inner3")), []),
            (4, Row(a=4, b="w", inner=Row(c=40, d="inner4")), [Row(x=4, y="y4"), Row(x=14, y="y14")]),
            (5, Row(a=1, b="x", inner=Row(c=10, d="inner5")), [Row(x=5, y="y5"), Row(x=15, y="y15")]),
            (6, Row(a=6, b="v", inner=Row(c=60, d="inner6")), [Row(x=6, y="y6"), Row(x=16, y="y16")]),
            (7, None, None),
            (8, Row(a=8, b="t", inner=Row(c=80, d="inner8")), [Row(x=8, y="y8"), Row(x=18, y="y18")]),
        ],
    ),
    "except": (
        "SELECT id, s FROM {t} EXCEPT SELECT id, s FROM {t} WHERE grp = 'g1' ORDER BY id",
        [
            (3, Row(a=3, b="z", inner=Row(c=30, d="inner3"))),
            (4, Row(a=4, b="w", inner=Row(c=40, d="inner4"))),
            (5, Row(a=1, b="x", inner=Row(c=10, d="inner5"))),
            (7, None),
            (8, Row(a=8, b="t", inner=Row(c=80, d="inner8"))),
        ],
    ),
    "cte": (
        "WITH c AS (SELECT id, s.inner AS i, grp FROM {t} WHERE s IS NOT NULL)"
        " SELECT grp, max(i) AS mi FROM c GROUP BY grp ORDER BY grp",
        [(None, Row(c=80, d="inner8")), ("g1", Row(c=60, d="inner6")), ("g2", Row(c=30, d="inner3"))],
    ),
}

# Queries that fail for reasons unrelated to column mapping: each of them also fails
# on the Delta table without column mapping. The value is the reason, and whether
# the failure only happens on partitioned tables.
_KNOWN_QUERY_FAILURES = {
    "filter_array_struct": (
        "a higher-order function over an array of structs in a filter cannot resolve the column",
        False,
    ),
    "semi_join": ("a correlated reference to a nested field cannot be resolved", False),
    "intersect": ("INTERSECT on array of struct columns fails to compare list values", False),
    "select_casts": ("casting arrays and maps with null elements to STRING prints NULL instead of null", False),
}


def _load_rows(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    return spark.createDataFrame(rows, schema=_SCHEMA)


def _write_delta(df: DataFrame, path: Path, *, mode: str, column_mapping_mode: str, partitioned: bool) -> None:
    writer = df.write.format("delta").mode(mode)
    if column_mapping_mode != "none":
        writer = writer.option("delta.columnMapping.mode", column_mapping_mode)
    if partitioned:
        writer = writer.partitionBy("grp")
    writer.save(str(path))


def _sorted_rows(df: DataFrame) -> list[Row]:
    return df.orderBy("id").collect()


@pytest.fixture(scope="module", params=_TABLE_VARIANTS)
def column_mapped_views(request, spark: SparkSession, tmp_path_factory) -> dict:
    """Write two commits and expose both path and catalog access to the mapped table."""
    column_mapping_mode, partitioned = request.param
    suffix = f"{column_mapping_mode}_{'part' if partitioned else 'flat'}"
    path = tmp_path_factory.mktemp(f"delta_cm_{suffix}") / "table"
    _write_delta(
        _load_rows(spark, _ROWS_FIRST),
        path,
        mode="overwrite",
        column_mapping_mode=column_mapping_mode,
        partitioned=partitioned,
    )
    _write_delta(
        _load_rows(spark, _ROWS_SECOND),
        path,
        mode="append",
        column_mapping_mode=column_mapping_mode,
        partitioned=partitioned,
    )

    path_view = f"cm_path_{suffix}"
    table = f"cm_table_{suffix}"
    spark.read.format("delta").load(str(path)).createOrReplaceTempView(path_view)
    spark.sql(f"CREATE TABLE {table} USING delta LOCATION '{escape_sql_string_literal(str(path))}'")
    yield {
        "path": path_view,
        "table": table,
        "location": str(path),
        "partitioned": partitioned,
    }
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.catalog.dropTempView(path_view)


def test_column_mapped_table_is_column_mapped(column_mapped_views: dict) -> None:
    commit = Path(column_mapped_views["location"]) / "_delta_log" / "00000000000000000000.json"
    actions = [json.loads(line) for line in commit.read_text().splitlines()]
    metadata = next(action["metaData"] for action in actions if "metaData" in action)
    assert metadata["configuration"]["delta.columnMapping.mode"] in {"name", "id"}


@pytest.mark.parametrize("access", ["path", "table"])
def test_column_mapped_roundtrip(spark: SparkSession, column_mapped_views: dict, access: str) -> None:
    df = spark.table(column_mapped_views[access])
    fields = list(_SCHEMA.fields)
    if column_mapped_views["partitioned"]:
        # Sail's DataFrame writer stores partition columns after the data columns.
        fields = [f for f in fields if f.name != "grp"] + [_SCHEMA["grp"]]
    assert [(f.name, f.dataType) for f in df.schema.fields] == [(f.name, f.dataType) for f in fields]
    rows = df.orderBy("id").collect()
    assert [tuple(row[name] for name in _SCHEMA.names) for row in rows] == _ROWS_FIRST + _ROWS_SECOND


@pytest.mark.parametrize("access", ["path", "table"])
@pytest.mark.parametrize("name", list(_QUERIES))
def test_column_mapped_query(request, spark: SparkSession, column_mapped_views: dict, access: str, name: str) -> None:
    if name in _KNOWN_QUERY_FAILURES:
        reason, partitioned_only = _KNOWN_QUERY_FAILURES[name]
        if column_mapped_views["partitioned"] or not partitioned_only:
            request.applymarker(pytest.mark.xfail(reason=reason, strict=True))
    query, expected = _QUERIES[name]
    actual = spark.sql(query.format(t=column_mapped_views[access])).collect()
    assert [tuple(row) for row in actual] == expected


_DATAFRAME_OPERATIONS = {
    "drop_duplicates_scalar": (
        lambda df: df.dropDuplicates(["grp"]).select("grp").orderBy("grp"),
        [(None,), ("g1",), ("g2",)],
    ),
    "drop_duplicates_struct": (
        lambda df: df.select("s").dropDuplicates().orderBy("s.a", "s.inner.d"),
        [
            (None,),
            (Row(a=1, b="x", inner=Row(c=10, d="inner1")),),
            (Row(a=1, b="x", inner=Row(c=10, d="inner5")),),
            (Row(a=2, b="y", inner=Row(c=20, d="inner2")),),
            (Row(a=3, b="z", inner=Row(c=30, d="inner3")),),
            (Row(a=4, b="w", inner=Row(c=40, d="inner4")),),
            (Row(a=6, b="v", inner=Row(c=60, d="inner6")),),
            (Row(a=8, b="t", inner=Row(c=80, d="inner8")),),
        ],
    ),
    "select_nested_columns": (
        lambda df: df.select("id", "s.inner.c", F.get("arr_s", 0)["y"].alias("y")).orderBy("id"),
        [
            (1, 10, "y1"),
            (2, 20, "y2"),
            (3, 30, None),
            (4, 40, "y4"),
            (5, 10, "y5"),
            (6, 60, "y6"),
            (7, None, None),
            (8, 80, "y8"),
        ],
    ),
    "filter_nested_column": (
        lambda df: df.filter(F.col("s.inner.c") >= F.lit(30)).select("id").orderBy("id"),
        [(3,), (4,), (6,), (8,)],
    ),
    "with_field": (
        lambda df: df.select("id", F.col("s").withField("a", F.lit(0)).alias("s")).orderBy("id"),
        [
            (1, Row(a=0, b="x", inner=Row(c=10, d="inner1"))),
            (2, Row(a=0, b="y", inner=Row(c=20, d="inner2"))),
            (3, Row(a=0, b="z", inner=Row(c=30, d="inner3"))),
            (4, Row(a=0, b="w", inner=Row(c=40, d="inner4"))),
            (5, Row(a=0, b="x", inner=Row(c=10, d="inner5"))),
            (6, Row(a=0, b="v", inner=Row(c=60, d="inner6"))),
            (7, None),
            (8, Row(a=0, b="t", inner=Row(c=80, d="inner8"))),
        ],
    ),
    "drop_fields": (
        lambda df: df.select("id", F.col("s").dropFields("inner").alias("s")).orderBy("id"),
        [
            (1, Row(a=1, b="x")),
            (2, Row(a=2, b="y")),
            (3, Row(a=3, b="z")),
            (4, Row(a=4, b="w")),
            (5, Row(a=1, b="x")),
            (6, Row(a=6, b="v")),
            (7, None),
            (8, Row(a=8, b="t")),
        ],
    ),
    "with_column": (
        lambda df: df.withColumn("a2", F.col("s.a") + 1).select("id", "a2", "s").orderBy("id"),
        [
            (1, 2, Row(a=1, b="x", inner=Row(c=10, d="inner1"))),
            (2, 3, Row(a=2, b="y", inner=Row(c=20, d="inner2"))),
            (3, 4, Row(a=3, b="z", inner=Row(c=30, d="inner3"))),
            (4, 5, Row(a=4, b="w", inner=Row(c=40, d="inner4"))),
            (5, 2, Row(a=1, b="x", inner=Row(c=10, d="inner5"))),
            (6, 7, Row(a=6, b="v", inner=Row(c=60, d="inner6"))),
            (7, None, None),
            (8, 9, Row(a=8, b="t", inner=Row(c=80, d="inner8"))),
        ],
    ),
    "group_by_agg": (
        lambda df: (
            df.groupBy("grp")
            .agg(F.count("*").alias("n"), F.max("s.inner").alias("mi"), F.collect_set("s.b").alias("bs"))
            .select("grp", "n", "mi", F.array_sort("bs").alias("bs"))
            .orderBy("grp")
        ),
        [
            (None, 2, Row(c=80, d="inner8"), ["t", "w"]),
            ("g1", 3, Row(c=60, d="inner6"), ["v", "x", "y"]),
            ("g2", 3, Row(c=30, d="inner3"), ["x", "z"]),
        ],
    ),
    "union_by_name": (
        lambda df: (
            df.select("id", "s", "m").unionByName(df.select("m", "s", "id")).orderBy("id").select("id", "s", "m")
        ),
        [
            (1, Row(a=1, b="x", inner=Row(c=10, d="inner1")), {"k1": 1, "k2": 2}),
            (1, Row(a=1, b="x", inner=Row(c=10, d="inner1")), {"k1": 1, "k2": 2}),
            (2, Row(a=2, b="y", inner=Row(c=20, d="inner2")), {"k1": 2, "k2": 4}),
            (2, Row(a=2, b="y", inner=Row(c=20, d="inner2")), {"k1": 2, "k2": 4}),
            (3, Row(a=3, b="z", inner=Row(c=30, d="inner3")), {}),
            (3, Row(a=3, b="z", inner=Row(c=30, d="inner3")), {}),
            (4, Row(a=4, b="w", inner=Row(c=40, d="inner4")), {"k1": 4, "k2": 8}),
            (4, Row(a=4, b="w", inner=Row(c=40, d="inner4")), {"k1": 4, "k2": 8}),
            (5, Row(a=1, b="x", inner=Row(c=10, d="inner5")), {"k1": 5, "k2": 10}),
            (5, Row(a=1, b="x", inner=Row(c=10, d="inner5")), {"k1": 5, "k2": 10}),
            (6, Row(a=6, b="v", inner=Row(c=60, d="inner6")), {"k1": 6, "k2": 12}),
            (6, Row(a=6, b="v", inner=Row(c=60, d="inner6")), {"k1": 6, "k2": 12}),
            (7, None, None),
            (7, None, None),
            (8, Row(a=8, b="t", inner=Row(c=80, d="inner8")), {"k1": None}),
            (8, Row(a=8, b="t", inner=Row(c=80, d="inner8")), {"k1": None}),
        ],
    ),
    "fill_na": (
        lambda df: df.select("id", "name", "dbl").na.fill({"name": "none", "dbl": 0.0}).orderBy("id"),
        [
            (1, "n1", 1.5),
            (2, "n2", 2.5),
            (3, "n3", 3.5),
            (4, "n4", 4.5),
            (5, "n5", 5.5),
            (6, "none", 0.0),
            (7, "n7", 7.5),
            (8, "n8", 8.5),
        ],
    ),
    "drop_na": (
        lambda df: df.select("id", "s", "arr").na.drop().orderBy("id"),
        [
            (1, Row(a=1, b="x", inner=Row(c=10, d="inner1")), [1, 2]),
            (2, Row(a=2, b="y", inner=Row(c=20, d="inner2")), [2, 3]),
            (3, Row(a=3, b="z", inner=Row(c=30, d="inner3")), []),
            (4, Row(a=4, b="w", inner=Row(c=40, d="inner4")), [4, 5]),
            (5, Row(a=1, b="x", inner=Row(c=10, d="inner5")), [5, 6]),
            (6, Row(a=6, b="v", inner=Row(c=60, d="inner6")), [6, 7]),
            (8, Row(a=8, b="t", inner=Row(c=80, d="inner8")), [8, None]),
        ],
    ),
    "explode": (
        lambda df: df.select("id", F.explode("arr_s").alias("e")).select("id", "e.x", "e.y").orderBy("id", "x"),
        [
            (1, 1, "y1"),
            (1, 11, "y11"),
            (2, 2, "y2"),
            (2, 12, "y12"),
            (4, 4, "y4"),
            (4, 14, "y14"),
            (5, 5, "y5"),
            (5, 15, "y15"),
            (6, 6, "y6"),
            (6, 16, "y16"),
            (8, 8, "y8"),
            (8, 18, "y18"),
        ],
    ),
    "sort_desc": (
        lambda df: df.select("id", "s").orderBy(F.col("s.inner.c").desc_nulls_last(), "id"),
        [
            (8, Row(a=8, b="t", inner=Row(c=80, d="inner8"))),
            (6, Row(a=6, b="v", inner=Row(c=60, d="inner6"))),
            (4, Row(a=4, b="w", inner=Row(c=40, d="inner4"))),
            (3, Row(a=3, b="z", inner=Row(c=30, d="inner3"))),
            (2, Row(a=2, b="y", inner=Row(c=20, d="inner2"))),
            (1, Row(a=1, b="x", inner=Row(c=10, d="inner1"))),
            (5, Row(a=1, b="x", inner=Row(c=10, d="inner5"))),
            (7, None),
        ],
    ),
    "join_on_struct_field": (
        lambda df: (
            df.select(F.col("id").alias("l"), F.col("s.a").alias("k"))
            .join(df.select(F.col("id").alias("r"), F.col("s.a").alias("k")), "k")
            .select("l", "r")
            .orderBy("l", "r")
        ),
        [(1, 1), (1, 5), (2, 2), (3, 3), (4, 4), (5, 1), (5, 5), (6, 6), (8, 8)],
    ),
    "distinct": (
        lambda df: df.select("grp", "flag").distinct().orderBy("grp", "flag"),
        [(None, True), ("g1", None), ("g1", False), ("g1", True), ("g2", False)],
    ),
}


@pytest.mark.parametrize("access", ["path", "table"])
@pytest.mark.parametrize("name", list(_DATAFRAME_OPERATIONS))
def test_column_mapped_dataframe_operation(
    spark: SparkSession, column_mapped_views: dict, access: str, name: str
) -> None:
    operation, expected = _DATAFRAME_OPERATIONS[name]
    actual = operation(spark.table(column_mapped_views[access])).collect()
    assert [tuple(row) for row in actual] == expected


class _TablePair:
    """A column-mapped Delta table and a Delta table without column mapping holding the same rows."""

    def __init__(self, spark: SparkSession, base: Path, column_mapping_mode: str, *, partitioned: bool) -> None:
        self.spark = spark
        self.partitioned = partitioned
        self.paths = {"mapped": base / "mapped", "plain": base / "plain"}
        self.modes = {"mapped": column_mapping_mode, "plain": "none"}
        for key, path in self.paths.items():
            _write_delta(
                _load_rows(spark, _ROWS_FIRST + _ROWS_SECOND),
                path,
                mode="overwrite",
                column_mapping_mode=self.modes[key],
                partitioned=partitioned,
            )

    def table(self, key: str) -> str:
        return f"delta.`{self.paths[key]}`"

    def sql(self, statement: str) -> None:
        for key in self.paths:
            self.spark.sql(statement.format(t=self.table(key))).collect()

    def write(self, df: DataFrame, *, mode: str, **options: str) -> None:
        for path in self.paths.values():
            writer = df.write.format("delta").mode(mode)
            for name, value in options.items():
                writer = writer.option(name, value)
            writer.save(str(path))

    def read(self, key: str, **options: str) -> DataFrame:
        reader = self.spark.read.format("delta")
        for name, value in options.items():
            reader = reader.option(name, value)
        return reader.load(str(self.paths[key]))

    def assert_same(self, **options: str) -> None:
        mapped = self.read("mapped", **options)
        plain = self.read("plain", **options)
        assert mapped.schema == plain.schema
        assert _sorted_rows(mapped) == _sorted_rows(plain)


@pytest.fixture(params=_TABLE_VARIANTS)
def table_pair(request, spark: SparkSession, tmp_path: Path) -> _TablePair:
    column_mapping_mode, partitioned = request.param
    return _TablePair(spark, tmp_path, column_mapping_mode, partitioned=partitioned)


def test_column_mapped_append_and_overwrite(spark: SparkSession, table_pair: _TablePair) -> None:
    extra = [_row(10, "g3", a=10, b="appended", c=100)]
    options = {"partitionBy": "grp"} if table_pair.partitioned else {}
    table_pair.write(_load_rows(spark, extra), mode="append", **options)
    table_pair.assert_same()
    table_pair.write(_load_rows(spark, _ROWS_SECOND), mode="overwrite", **options)
    table_pair.assert_same()


@pytest.mark.xfail(
    reason="replaceWhere fails with mismatched nested types in the union, also without column mapping", strict=True
)
def test_column_mapped_replace_where(spark: SparkSession, table_pair: _TablePair) -> None:
    replacement = [_row(11, "g1", a=11, b="replaced", c=110)]
    options = {"partitionBy": "grp"} if table_pair.partitioned else {}
    table_pair.write(_load_rows(spark, replacement), mode="overwrite", replaceWhere="grp = 'g1'", **options)
    table_pair.assert_same()


def test_column_mapped_schema_evolution(spark: SparkSession, table_pair: _TablePair) -> None:
    evolved = (
        _load_rows(spark, [_row(12, "g1", a=12, b="evolved", c=120)])
        .withColumn("extra", F.when(F.col("id").isNotNull(), F.lit("e")))
        .withColumn("s", F.col("s").withField("added", F.when(F.col("id").isNotNull(), F.lit(1))))
    )
    options = {"partitionBy": "grp"} if table_pair.partitioned else {}
    table_pair.write(evolved, mode="append", mergeSchema="true", **options)
    table_pair.assert_same()
    for key in table_pair.paths:
        df = table_pair.read(key)
        assert df.schema["extra"].dataType == StringType()
        assert df.schema["s"].dataType["added"].dataType == IntegerType()


@pytest.mark.xfail(
    reason="reading old files after a mergeSchema append of a non-nullable column fails, also without column mapping",
    strict=True,
)
def test_column_mapped_schema_evolution_non_nullable_column(spark: SparkSession, table_pair: _TablePair) -> None:
    evolved = _load_rows(spark, [_row(12, "g1", a=12, b="evolved", c=120)]).withColumn("extra", F.lit("e"))
    options = {"partitionBy": "grp"} if table_pair.partitioned else {}
    table_pair.write(evolved, mode="append", mergeSchema="true", **options)
    table_pair.assert_same()


@pytest.mark.skip(
    reason="reading old files after a mergeSchema append of a non-nullable nested field panics the server,"
    " also without column mapping"
)
def test_column_mapped_schema_evolution_non_nullable_nested_field(spark: SparkSession, table_pair: _TablePair) -> None:
    evolved = _load_rows(spark, [_row(12, "g1", a=12, b="evolved", c=120)]).withColumn(
        "s", F.col("s").withField("added", F.lit(1))
    )
    options = {"partitionBy": "grp"} if table_pair.partitioned else {}
    table_pair.write(evolved, mode="append", mergeSchema="true", **options)
    table_pair.assert_same()


@pytest.mark.xfail(
    reason="setting a struct column to NULL fails a type check, also without column mapping", strict=True
)
def test_column_mapped_update_struct_to_null(table_pair: _TablePair) -> None:
    table_pair.sql("UPDATE {t} SET s = NULL WHERE id = 1")
    table_pair.assert_same()
