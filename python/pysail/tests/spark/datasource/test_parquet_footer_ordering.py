"""Parquet footer `sorting_columns` and how they affect reads by path.

Spark ignores `sorting_columns`, so every query here must return rows in the
order its ORDER BY asks for, whatever the footer says. Sail can also use a
footer that matches the query to skip the sort; the plan checks cover that.

The cases are generated from a few dimensions (column type, null placement,
declared order, query order, file layout, and footer shape) so that the matrix
covers several thousand footer and query combinations. Cases that Sail does
not support are marked `xfail`. The findings are summarized in
`docs/development/parquet-footer-ordering.md`.
"""

from __future__ import annotations

import datetime
import decimal
import itertools
import math
from dataclasses import dataclass, field

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pysail.testing.spark.utils.common import is_jvm_spark

# A sort key is (column, descending, nulls_first).
SortKey = tuple[str, bool, bool]

DIRECTIONS = [(False, True), (False, False), (True, True), (True, False)]


@dataclass(frozen=True)
class ColumnType:
    name: str
    arrow_type: pa.DataType
    values: tuple


def _dates():
    base = datetime.date(2024, 1, 1)
    return tuple(base + datetime.timedelta(days=d) for d in (-400, -3, 0, 1, 31, 365))


def _timestamps():
    base = datetime.datetime(2024, 1, 1)
    return tuple(base + datetime.timedelta(seconds=s) for s in (-86400, -1, 0, 1, 3600, 86400 * 40))


COLUMN_TYPES = [
    ColumnType("int8", pa.int8(), (-128, -7, 0, 1, 42, 127)),
    ColumnType("int16", pa.int16(), (-32768, -7, 0, 1, 42, 32767)),
    ColumnType("int32", pa.int32(), (-(2**31), -7, 0, 1, 42, 2**31 - 1)),
    ColumnType("int64", pa.int64(), (-(2**63), -7, 0, 1, 42, 2**63 - 1)),
    ColumnType("float32", pa.float32(), (-math.inf, -1.5, 0.0, 0.5, 1024.0, math.inf)),
    ColumnType("float64", pa.float64(), (-math.inf, -1.5, -0.0, 0.5, 1e10, math.inf)),
    ColumnType("float64_nan", pa.float64(), (-1.5, 0.0, 2.5, 1e10, math.inf, math.nan)),
    ColumnType("string", pa.string(), ("", "A", "a", "ab", "b", "é")),
    ColumnType("large_string", pa.large_string(), ("", "A", "a", "ab", "b", "\u00e9")),
    ColumnType("dictionary", pa.dictionary(pa.int32(), pa.string()), ("", "A", "a", "ab", "b", "\u00e9")),
    ColumnType("binary", pa.binary(), (b"", b"\x00", b"\x01", b"a", b"ab", b"\xff")),
    ColumnType("boolean", pa.bool_(), (False, True)),
    ColumnType("date", pa.date32(), _dates()),
    ColumnType("timestamp_us", pa.timestamp("us"), _timestamps()),
    ColumnType("timestamp_ms", pa.timestamp("ms"), _timestamps()),
    ColumnType(
        "decimal",
        pa.decimal128(10, 2),
        tuple(decimal.Decimal(v) for v in ("-99999999.99", "-1.00", "0.00", "0.01", "12.34", "99999999.99")),
    ),
]


def _value_key(value, descending: bool, nulls_first: bool):
    """Sort key that follows Spark ordering, including NaN as the largest value."""
    if value is None:
        return (0 if nulls_first else 2,)
    if isinstance(value, float) and math.isnan(value):
        rank = (1, 0)
    else:
        rank = (0, value)
    if descending:
        return (1, _Reversed(rank))
    return (1, rank)


class _Reversed:
    __slots__ = ("value",)

    def __init__(self, value):
        self.value = value

    def __lt__(self, other):
        return other.value < self.value

    def __eq__(self, other):
        return self.value == other.value


def _row_key(row: dict, keys: list[SortKey]):
    return tuple(_value_key(row[c], d, n) for c, d, n in keys)


def _normalize(value):
    if isinstance(value, float) and math.isnan(value):
        return "NaN"
    if isinstance(value, bytearray):
        return bytes(value)
    return value


def _sql_column(column: str) -> str:
    return ".".join(f"`{part}`" for part in column.split("."))


def _sql_order(keys: list[SortKey]) -> str:
    return ", ".join(f"{_sql_column(c)} {'DESC' if d else 'ASC'} NULLS {'FIRST' if n else 'LAST'}" for c, d, n in keys)


def _key_id(keys: list[SortKey]) -> str:
    if not keys:
        return "none"
    return "+".join(f"{c}-{'desc' if d else 'asc'}-{'nf' if n else 'nl'}" for c, d, n in keys)


def _satisfies(declared: list[SortKey], query: list[SortKey]) -> bool:
    """Whether data sorted by `declared` is already in `query` order."""
    return len(query) <= len(declared) and declared[: len(query)] == query


@dataclass
class Case:
    id: str
    columns: dict[str, pa.DataType]
    # Each file is a list of rows (dicts), written in the given order.
    files: list[list[dict]]
    # Sorting columns written in each file footer.
    footers: list[list[SortKey]]
    query: list[SortKey]
    # Whether the footer order of every file matches the actual data order.
    truthful: bool = True
    row_group_size: int | None = None
    select: list[str] | None = None
    # Per-file column order (for files whose physical column order differs).
    file_columns: list[list[str]] | None = None
    expect_no_sort: bool = False
    # Hive partition value for each file, stored under `k=<value>`.
    partitions: list[int] | None = None
    # Sorting columns written as raw leaf indexes, overriding `footers`.
    raw_sorting: list[tuple[int, bool, bool]] | None = None
    where: tuple[str, object] | None = None
    limit: int | None = None
    marks: list = field(default_factory=list)


def _single_column_rows(ctype: ColumnType, *, nulls: bool) -> list[dict]:
    values = [v for v in ctype.values for _ in range(2)]
    if nulls:
        values += [None, None, None]
    return [{"v": v} for v in values]


def _sorted_rows(rows: list[dict], keys: list[SortKey]) -> list[dict]:
    return sorted(rows, key=lambda r: _row_key(r, keys))


LAYOUTS = ["one_row_group", "many_row_groups", "files_disjoint", "files_overlapping"]


def _layout(rows: list[dict], keys: list[SortKey], layout: str) -> tuple[list[list[dict]], int | None]:
    ordered = _sorted_rows(rows, keys)
    if layout == "one_row_group":
        return [ordered], None
    if layout == "many_row_groups":
        return [ordered], 4
    if layout == "files_disjoint":
        n = len(ordered)
        bounds = [0, n // 3, 2 * n // 3, n]
        return [ordered[bounds[i] : bounds[i + 1]] for i in range(3)], None
    if layout == "files_overlapping":
        return [_sorted_rows(ordered[i::3], keys) for i in range(3)], None
    raise ValueError(layout)


def _single_column_cases() -> list[Case]:
    cases = []
    for ctype, nulls, (fd, fn), (qd, qn), layout in itertools.product(
        COLUMN_TYPES, [False, True], DIRECTIONS, DIRECTIONS, LAYOUTS
    ):
        declared = [("v", fd, fn)]
        query = [("v", qd, qn)]
        files, rgs = _layout(_single_column_rows(ctype, nulls=nulls), declared, layout)
        cases.append(
            Case(
                id=f"single-{ctype.name}-{'nulls' if nulls else 'nonulls'}-{layout}"
                f"-footer_{_key_id(declared)}-query_{_key_id(query)}",
                columns={"v": ctype.arrow_type},
                files=files,
                footers=[declared] * len(files),
                query=query,
                row_group_size=rgs,
                expect_no_sort=_satisfies(declared, query),
            )
        )
    return cases


_PAIR_ROWS = [{"a": a, "b": b} for a, b in itertools.product([None, -3, 0, 5], [None, "", "m", "z"])]


def _all_keys(columns: list[str]) -> list[list[SortKey]]:
    keys = []
    for n in (1, 2):
        for cols in itertools.permutations(columns, n):
            for dirs in itertools.product(DIRECTIONS, repeat=n):
                keys.append([(c, d, nf) for c, (d, nf) in zip(cols, dirs)])
    return keys


def _multi_column_cases() -> list[Case]:
    cases = []
    footers = _all_keys(["a", "b"])
    # Queries: all one-key orders, plus two-key orders whose keys share a direction,
    # which keeps the matrix to a few thousand cases while still covering prefixes.
    queries = [q for q in _all_keys(["a", "b"]) if len(q) == 1 or q[0][1:] == q[1][1:]]
    for declared, query, layout in itertools.product(footers, queries, ["one_row_group", "many_row_groups"]):
        files, rgs = _layout(list(_PAIR_ROWS), declared, layout)
        cases.append(
            Case(
                id=f"multi-{layout}-footer_{_key_id(declared)}-query_{_key_id(query)}",
                columns={"a": pa.int64(), "b": pa.string()},
                files=files,
                footers=[declared] * len(files),
                query=query,
                row_group_size=rgs,
                expect_no_sort=_satisfies(declared, query),
            )
        )
    return cases


def _footer_shape_cases() -> list[Case]:
    """Footers that disagree with the data or with each other."""
    cases = []
    rows = [{"a": a, "b": f"s{a:03d}"} for a in range(30)]
    asc = [("a", False, False)]
    desc = [("a", True, False)]
    columns = {"a": pa.int64(), "b": pa.string()}

    # The footer claims an order the data does not have.
    for name, data in [
        ("reversed", list(reversed(rows))),
        ("shuffled", [rows[(i * 7) % 30] for i in range(30)]),
        ("one_swap", rows[:10] + [rows[11], rows[10]] + rows[12:]),
    ]:
        for rgs in (None, 5):
            for query in (asc, desc):
                cases.append(
                    Case(
                        id=f"shape-untruthful_{name}-rg{rgs or 'all'}-query_{_key_id(query)}",
                        columns=columns,
                        files=[data],
                        footers=[asc],
                        query=query,
                        truthful=False,
                        row_group_size=rgs,
                    )
                )

    # Files with different footers.
    halves = [rows[:15], rows[15:]]
    for name, footers in [
        ("one_without_footer", [asc, []]),
        ("asc_and_desc", [asc, desc]),
        ("prefix_only", [asc + [("b", False, False)], asc]),
        ("different_second_key", [asc + [("b", False, False)], asc + [("b", True, False)]]),
        ("other_column", [asc, [("b", False, False)]]),
    ]:
        files = [_sorted_rows(h, f or asc) for h, f in zip(halves, footers)]
        for query in (asc, desc, asc + [("b", False, False)]):
            common = footers[0]
            for f in footers[1:]:
                n = 0
                while n < min(len(common), len(f)) and common[n] == f[n]:
                    n += 1
                common = common[:n]
            cases.append(
                Case(
                    id=f"shape-mixed_{name}-query_{_key_id(query)}",
                    columns=columns,
                    files=files,
                    footers=footers,
                    query=query,
                    expect_no_sort=_satisfies(common, query),
                )
            )

    # The sorted column is at a different position in each file.
    files = [_sorted_rows(rows[:15], asc), _sorted_rows(rows[15:], asc)]
    for query in (asc, desc):
        cases.append(
            Case(
                id=f"shape-column_position_differs-query_{_key_id(query)}",
                columns=columns,
                files=files,
                footers=[asc, asc],
                file_columns=[["a", "b"], ["b", "a"]],
                query=query,
                expect_no_sort=_satisfies(asc, query),
            )
        )

    # Projections and empty data.
    for select in (["a"], ["b"], ["b", "a"]):
        for query in (asc, desc):
            cases.append(
                Case(
                    id=f"shape-select_{'_'.join(select)}-query_{_key_id(query)}",
                    columns=columns,
                    files=[_sorted_rows(rows, asc)],
                    footers=[asc],
                    query=query,
                    select=select,
                    expect_no_sort=_satisfies(asc, query),
                )
            )
    # Footer indexes count leaf columns, so a struct shifts the columns after it.
    nested_columns = {"s": pa.struct([("x", pa.int64()), ("y", pa.int64())]), **columns}
    nested_rows = [{**r, "s": {"x": 100 - r["a"], "y": r["a"]}, "s.x": 100 - r["a"]} for r in rows]
    for name, key in [("after_struct", "a"), ("struct_leaf", "s.x")]:
        declared = [(key, False, False)]
        for query in (declared, [(key, True, False)]):
            cases.append(
                Case(
                    id=f"shape-nested_{name}-query_{_key_id(query)}",
                    columns=nested_columns,
                    files=[_sorted_rows(nested_rows, declared)],
                    footers=[declared],
                    query=query,
                    select=["a", "b"],
                    expect_no_sort=key == "a" and _satisfies(declared, query),
                )
            )

    # Partition directories, each holding one sorted file.
    keyed = [{**r, "k": i % 3} for i, r in enumerate(rows)]
    parts = [_sorted_rows([r for r in keyed if r["k"] == k], asc) for k in range(3)]
    partition_key = [("k", False, False)]
    for query in (asc, desc, partition_key + asc):
        cases.append(
            Case(
                id=f"shape-partitioned-query_{_key_id(query)}",
                columns=columns,
                files=parts,
                footers=[asc] * 3,
                partitions=[0, 1, 2],
                query=query,
                select=["a", "b"],
                expect_no_sort=_satisfies(asc, query),
            )
        )

    # Filters and limits on top of a footer order.
    ordered = _sorted_rows(rows, asc)
    for query in (asc, desc):
        for name, where, limit in [
            ("filter", ("`a` >= 10 AND `a` < 20", lambda r: 10 <= r["a"] < 20), None),
            ("limit", None, 5),
            ("filter_limit", ("`a` >= 10", lambda r: r["a"] >= 10), 5),
        ]:
            cases.append(
                Case(
                    id=f"shape-{name}-query_{_key_id(query)}",
                    columns=columns,
                    files=[ordered],
                    footers=[asc],
                    query=query,
                    row_group_size=5,
                    where=where,
                    limit=limit,
                    expect_no_sort=_satisfies(asc, query),
                )
            )

    # Footers that are unusual but carry no valid promise about the data.
    for name, raw in [
        ("index_out_of_range", [(99, False, False)]),
        ("repeated_column", [(0, False, False), (0, True, False)]),
    ]:
        for query in (asc, desc):
            cases.append(
                Case(
                    id=f"shape-{name}-query_{_key_id(query)}",
                    columns=columns,
                    files=[ordered],
                    footers=[asc],
                    raw_sorting=raw,
                    query=query,
                )
            )
    all_null = [{"a": None, "b": f"s{i}"} for i in range(5)]
    for query in (asc, [("a", False, True)]):
        cases.append(
            Case(
                id=f"shape-all_null_column-query_{_key_id(query)}",
                columns=columns,
                files=[all_null],
                footers=[asc],
                query=query,
                expect_no_sort=_satisfies(asc, query),
            )
        )

    for query in (asc, desc):
        cases.append(
            Case(
                id=f"shape-empty_file-query_{_key_id(query)}",
                columns=columns,
                files=[[]],
                footers=[asc],
                query=query,
            )
        )
    return cases


def _leaf_names(schema: pa.Schema) -> list[str]:
    leaves = []
    for f in schema:
        if pa.types.is_struct(f.type):
            leaves.extend(f"{f.name}.{child.name}" for child in f.type)
        else:
            leaves.append(f.name)
    return leaves


def _xfail_reason(case: Case) -> str | None:
    if any(pa.types.is_dictionary(t) for t in case.columns.values()):
        return "Sail cannot read a column stored with an Arrow dictionary type"
    if case.raw_sorting is not None and any(
        c >= len(_leaf_names(pa.schema(list(case.columns.items())))) for c, _, _ in case.raw_sorting
    ):
        return "Sail panics on a sorting column index outside the file schema"
    if not case.truthful and _satisfies(case.footers[0], case.query):
        return "Sail trusts footer sorting_columns and skips the sort even when the data is not in that order"
    return None


def _params():
    params = []
    for case in _single_column_cases() + _multi_column_cases() + _footer_shape_cases():
        marks = list(case.marks)
        reason = _xfail_reason(case)
        if reason is not None:
            marks.append(pytest.mark.xfail(reason=reason, strict=True))
        params.append(pytest.param(case, id=case.id, marks=marks))
    return params


def _write(case: Case, root) -> str:
    path = root / "data"
    path.mkdir()
    for i, (rows, footer) in enumerate(zip(case.files, case.footers)):
        names = case.file_columns[i] if case.file_columns else list(case.columns)
        schema = pa.schema([(n, case.columns[n]) for n in names])
        table = pa.Table.from_pylist(rows, schema=schema)
        leaves = _leaf_names(schema)
        if case.raw_sorting is not None:
            sorting = [pq.SortingColumn(c, descending=d, nulls_first=n) for c, d, n in case.raw_sorting]
        else:
            sorting = [pq.SortingColumn(leaves.index(c), descending=d, nulls_first=n) for c, d, n in footer]
        directory = path
        if case.partitions is not None:
            directory = path / f"k={case.partitions[i]}"
            directory.mkdir()
        pq.write_table(
            table,
            directory / f"part-{i:03d}.parquet",
            sorting_columns=sorting or None,
            row_group_size=case.row_group_size,
        )
    return str(path)


@pytest.mark.parametrize("case", _params())
def test_parquet_footer_ordering(spark, tmp_path, case: Case):
    path = _write(case, tmp_path)
    select = case.select or list(case.columns)
    columns = ", ".join(_sql_column(c) for c in select)
    where = f"WHERE {case.where[0]}" if case.where else ""
    limit = f"LIMIT {case.limit}" if case.limit is not None else ""
    query = f"SELECT {columns} FROM parquet.`{path}` {where} ORDER BY {_sql_order(case.query)} {limit}"

    rows = [row for f in case.files for row in f]
    if case.where:
        rows = [r for r in rows if case.where[1](r)]
    actual = [tuple(_normalize(v) for v in row) for row in spark.sql(query).collect()]

    # Rows that tie on every sort key may come back in any order, so compare
    # each run of tied rows as a multiset.
    ordered = _sorted_rows(rows, case.query)
    if case.limit is not None:
        ordered = ordered[: case.limit]
    assert len(actual) == len(ordered)
    start = 0
    for _, group in itertools.groupby(ordered, key=lambda r: tuple(_normalize(r[c]) for c, _, _ in case.query)):
        expected = [tuple(_normalize(r[c]) for c in select) for r in group]
        chunk = actual[start : start + len(expected)]
        assert sorted(chunk, key=repr) == sorted(expected, key=repr), f"rows {start}..{start + len(expected)}"
        start += len(expected)

    if case.expect_no_sort and not is_jvm_spark():
        plan = spark.sql(f"EXPLAIN {query}").first()[0]
        assert "SortExec" not in plan
