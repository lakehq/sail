---
title: Parquet Footer Ordering
rank: 90
---

# Parquet Footer Ordering

A Parquet file can record its sort order in the `sorting_columns` field of each row group.
Spark ignores this field, and so does Sail by default.

Set the configuration option `parquet.trust_sorting_columns` to `true`, or the read option
`trustSortingColumns` for a single read, to make Sail use the field. When no sort order is configured for
the table, Sail then removes a `SortExec` from the plan if the footer satisfies the query. Sail does not
check the data against the footer, so turn this on only for files whose footers you trust.

This report describes what the test matrix in
`python/pysail/tests/spark/datasource/test_parquet_footer_ordering.py` covers and what it found.

## How the Tests Work

Every case writes Parquet files with PyArrow, runs `SELECT ... ORDER BY ...`, and compares the rows with the
order computed in Python using Spark ordering rules. Rows that tie on all sort keys can appear in any order.

`test_parquet_footer_ordering_trusted` runs the whole matrix with `trustSortingColumns` set. When a footer
promises an order that satisfies the query, it also checks that `EXPLAIN` has no `SortExec`.

`test_parquet_footer_ordering_ignored_by_default` runs the footer shape cases with the default configuration.
For the same promises, it checks that `EXPLAIN` still has a `SortExec`.

The plan checks are skipped on JVM Spark.

## Coverage

| Group         | Cases | Dimensions                                                                                                                                                    |
| ------------- | ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Single column | 2,048 | 16 column types, with and without nulls, 4 declared orders, 4 query orders, and 4 layouts (one row group, many row groups, disjoint files, overlapping files) |
| Two columns   | 1,280 | Every one-key and two-key declared order, one-key queries, two-key queries with matching directions, and 2 row group layouts                                  |
| Footer shapes | 56    | Footers that do not match the data, files with different footers, moved and nested columns, partitions, filters, limits, and empty or all-null data           |

Of the 3,384 cases, 620 include the plan check.

## Findings

With the option on, 3,248 of the 3,384 cases pass and 136 are marked `xfail`.
With the option off, all 56 footer shape cases pass.

| Behavior                                                                       | Option off    | Option on (cases) |
| ------------------------------------------------------------------------------ | ------------- | ----------------- |
| Rows are ordered correctly for the other column types and layouts              | Supported     | Supported (3,248) |
| `SortExec` is removed when the declared order satisfies the query              | Never removed | Supported (588)   |
| Footer declares an order that the data does not have, and the query matches it | Correct order | Wrong results (6) |
| Column stored with an Arrow dictionary type                                    | Query fails   | Query fails (128) |
| Sorting column index outside the file schema                                   | Correct order | Query fails (2)   |

The 588 plan checks are part of the 3,248 passing cases.

### Footers That Do Not Match the Data

With the option on, Sail trusts the footer. If a file claims ascending order on `a` but is not sorted,
`ORDER BY a ASC` returns the rows in file order. A query that the footer does not satisfy, such as
`ORDER BY a DESC` for the same file, keeps its `SortExec` and is correct.

With the option off, these queries sort and return the correct order.

### Arrow Dictionary Columns

Reading a column that PyArrow stored with a dictionary type fails with
`cast Dictionary(Int32, Utf8) to Spark data type`.
This is not related to the footer. It affects every query on such a column.

### Sorting Column Index Out of Range

With the option on, a footer whose sorting column index is outside the file schema makes the Parquet crate
panic while reading the metadata, and the client sees the stream cancelled. The server keeps running.

With the option off, Sail does not read the field, and the file reads normally.

## Behavior That Works

With the option on, these cases all pass:

- NaN, infinities, the extremes of each integer type, and non-ASCII strings.
- Multiple files, including files with overlapping value ranges.
- Files with different footers. Only the common prefix of the orders is used.
- Sorted columns at different positions in each file, and columns after a struct, where the footer index counts leaf columns.
- Sorting on a struct leaf.
- Hive partition directories, filters, and limits.
