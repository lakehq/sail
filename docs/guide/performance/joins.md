---
title: Join Strategies
rank: 3
---

# Join Strategies

<!--@include: ./_common/options.md-->

Sail chooses an algorithm for each join when it plans the query.
The choice depends on the join condition, the statistics that Sail has for each input, and the options on this page.

## Join Algorithms

For a join on equality conditions, Sail uses one of the following algorithms.

A **hash join** builds a hash table from one input, called the build side.
It looks up the rows of the other input in that table.
Sail repartitions both inputs by the join keys and joins each partition separately.
The hash table cannot spill.
The build side of each partition must fit in memory.

A **broadcast join** is a hash join that collects the build side into one partition.
It joins this partition with every partition of the other input, without repartitioning that input.
Sail plans a broadcast join when the estimated size of one input is less than 1 MiB.
If the size of the input is unknown, Sail plans a broadcast join when its estimated row count is less than 131,072.
No Sail or Spark option changes these thresholds, including `spark.sql.autoBroadcastJoinThreshold`.
Sail does not plan a broadcast join for an input without statistics.
Sail plans a broadcast join regardless of these thresholds and statistics in two cases:

- a `NOT IN` subquery on nullable columns
- any join on equality conditions when the default parallelism is 1

A **sort-merge join** sorts both inputs by the join keys and merges them.
Its buffered input can spill to disk.
It can therefore complete joins whose inputs do not fit in memory.
Hash joins are generally faster, but can require substantially more memory.

A join without equality conditions runs as a nested loop join, or as a cross join if it is an inner join with no condition at all.

## Options

| Option                          | Default |
| ------------------------------- | ------- |
| `optimizer.prefer_hash_join`    | `true`  |
| `optimizer.enable_join_swap`    | `true`  |
| `optimizer.enable_join_reorder` | `false` |
| `execution.collect_statistics`  | `true`  |

When you disable `optimizer.prefer_hash_join`, Sail plans sort-merge joins instead of hash joins and broadcast joins, except in the two cases above.

`optimizer.enable_join_swap` lets Sail swap the inputs of a join to make the smaller input the build side.
When you disable it, hash joins keep the input order of the query.
A small right input then no longer becomes the build side of a broadcast join.

`optimizer.enable_join_reorder` enables a cost-based algorithm that can reorder a series of inner joins based on their estimated costs.
This is different from `optimizer.enable_join_swap`, which only swaps the two inputs of a single join.
The algorithm reorders only hash joins.
It has no effect when you disable `optimizer.prefer_hash_join`, unless the default parallelism is 1.

`execution.collect_statistics` makes Sail read row counts and column statistics from the files when it plans a query that reads a path, such as `spark.read.parquet(path)`.
The `parquet.file_statistics_cache` keeps these statistics.
Later queries reuse them until the cache entry expires or the file changes.
Delta Lake and Iceberg tables take their statistics from the table metadata, whatever the value of this option.
Join swapping, broadcast joins, and join reordering all depend on these statistics.
Without them, Sail cannot plan a broadcast join for the files or swap join inputs based on their size, except in the two cases above.

## Join Hints

Sail ignores join hints, such as `BROADCAST`, `MERGE`, and `SHUFFLE_HASH`, and the `broadcast()` function, which adds a `broadcast` hint.
The server logs a warning for each ignored hint, and the join runs with the algorithm that Sail chooses.
Use the options on this page to influence the choice instead.

## Cluster Mode

In local mode, a hash join passes a filter built from its build side to the other input while the query runs.
The filter can skip rows that cannot match.
Cluster mode disables these join filters.
As a result, the same query can run differently in local mode and in cluster mode.
Cluster mode keeps the filters from `ORDER BY` with `LIMIT` and from `MIN` and `MAX` aggregations.

In cluster mode, Sail uses a broadcast join only when the result does not depend on which rows of the broadcast input found a match.
Each partition of the join can only see its own matches.
Sail uses a hash join instead for the following joins.

- an outer join that keeps unmatched rows of the broadcast input
- a semi or anti join that returns rows of the broadcast input

Sail repartitions both inputs of such a join into as many partitions as the other input has.
A nested loop join of these kinds has no join keys to repartition by.
In cluster mode, Sail joins all rows of its other input in a single task.

In cluster mode, each task of a broadcast join builds its own hash table from the broadcast input.
A worker can therefore hold one copy of that table for each task slot.

A `NOT IN` subquery on nullable columns needs all rows of both inputs in one place to handle `NULL` values correctly.
In cluster mode, Sail runs this join in a single task.
A `NOT EXISTS` subquery does not have this restriction.
It treats `NULL` values differently from `NOT IN`.
Use it only when that difference does not affect your results.
