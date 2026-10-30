---
title: Parquet Footer Ordering
rank: 90
---

# Parquet Footer Ordering

A Parquet file can record its sort order in the `sorting_columns` field of each row group.
Spark ignores this field. Sail reads it when no sort order is configured for the table, and uses it to
remove a `SortExec` from the plan.

This report describes what the test matrix in
`python/pysail/tests/spark/datasource/test_parquet_footer_ordering.py` covers and what it found.

## How the Tests Work

Every case writes Parquet files with PyArrow, runs `SELECT ... ORDER BY ...` against the path, and compares
the rows with the order computed in Python using Spark ordering rules. Rows that tie on all sort keys can
appear in any order. When a footer promises an order that satisfies the query, the case also checks
that `EXPLAIN` has no `SortExec`. The plan check is skipped on JVM Spark.

## Coverage

| Group         | Cases | Dimensions                                                                                                                                                    |
| ------------- | ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Single column | 2,048 | 16 column types, with and without nulls, 4 declared orders, 4 query orders, and 4 layouts (one row group, many row groups, disjoint files, overlapping files) |
| Two columns   | 1,280 | Every one-key and two-key declared order, one-key queries, two-key queries with matching directions, and 2 row group layouts                                  |
| Footer shapes | 56    | Footers that do not match the data, files with different footers, moved and nested columns, partitions, filters, limits, and empty or all-null data           |

Of the 3,384 cases, 620 include the plan check.

## Findings

3,248 cases pass. The other 136 are marked `xfail`. The 588 plan checks in the table are part of the passing cases.

| Behavior                                                                       | Cases | Status        |
| ------------------------------------------------------------------------------ | ----- | ------------- |
| Rows are ordered correctly for the other column types and layouts              | 3,248 | Supported     |
| `SortExec` is removed when the declared order satisfies the query              | 588   | Supported     |
| Footer declares an order that the data does not have, and the query matches it | 6     | Wrong results |
| Column stored with an Arrow dictionary type                                    | 128   | Query fails   |
| Sorting column index outside the file schema                                   | 2     | Query fails   |

### Footers That Do Not Match the Data

Sail trusts the footer. If a file claims ascending order on `a` but is not sorted, `ORDER BY a ASC` returns
the rows in file order. Spark returns sorted rows for the same file, because it ignores the footer.
A query that the footer does not satisfy, such as `ORDER BY a DESC` for the same file, keeps its `SortExec` and is correct.
Before this change, Sail did not use the footer order for reads by path, so these files sorted correctly.

### Arrow Dictionary Columns

Reading a column that PyArrow stored with a dictionary type fails with
`cast Dictionary(Int32, Utf8) to Spark data type`.
This is not related to the footer. It affects every query on such a column.

### Sorting Column Index Out of Range

A footer whose sorting column index is outside the file schema makes the Parquet crate panic while reading the
metadata, and the client sees the stream cancelled. The server keeps running. Spark ignores the field and reads
the file.

## Behavior That Works

These cases all pass:

- NaN, infinities, the extremes of each integer type, and non-ASCII strings.
- Multiple files, including files with overlapping value ranges.
- Files with different footers. Only the common prefix of the orders is used.
- Sorted columns at different positions in each file, and columns after a struct, where the footer index counts leaf columns.
- Sorting on a struct leaf.
- Hive partition directories, filters, and limits.
