---
title: Managing Memory
rank: 2
---

# Managing Memory

<!--@include: ./_common/options.md-->

Query operators reserve the memory they need from a **memory pool**.
When the pool refuses a reservation, some operators **spill**, which means they write intermediate data to temporary files and continue.
Other operators fail the query.

## Memory Pool

`runtime.memory_pool.type` controls how the memory pool limits reservations.

| Type        | Behavior                                                                                                                                                    |
| ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `unbounded` | Tracks reservations but never refuses one. This is the default.                                                                                             |
| `greedy`    | Grants reservations first come, first served, up to `runtime.memory_pool.greedy.max_size`.                                                                  |
| `fair`      | Divides `runtime.memory_pool.fair.max_size`, minus the memory held by operators that cannot spill, evenly among the partitions of operators that can spill. |

Since operators spill only when the pool refuses a reservation, no operator spills under the default `unbounded` pool.
A query that needs more memory than is available can then cause the operating system to stop the Sail process, instead of failing with an error.
If your workload can exceed the available memory, choose a bounded pool and set its maximum size below the memory available to Sail.
Both bounded pools default to a maximum size of 64 GiB.

The `greedy` pool works well when a query has at most one operator that spills.
The `fair` pool suits queries with several operators that spill.
It can make an operator spill while memory reserved for other operators is unused.
The `fair` pool gives each partition of an operator its own share.
For example, the 16 partitions of a sort split the memory 16 ways, along with the partitions of any other operators that can spill.

Sail creates a separate memory pool for each session.
The maximum size of the pool and the limit on [temporary files](#temporary-files) apply to each session on its own.
Concurrent sessions can together use more memory than the maximum size.
In cluster mode, tasks reserve memory from the pool of the worker process that runs them.
In `local-cluster` mode, all workers of a session share one pool and one temporary-file limit.

```bash
export SAIL_RUNTIME__MEMORY_POOL__TYPE=fair
export SAIL_RUNTIME__MEMORY_POOL__FAIR__MAX_SIZE=17179869184
```

## Spilling

The following table shows what each operator does when the pool refuses a reservation.

| Operator                       | When a reservation is refused                                                                                                            |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------- |
| Sort                           | Spills                                                                                                                                   |
| Grouped aggregation            | Partial aggregation sends results downstream early, and final aggregation spills unless its input is already sorted by the grouping keys |
| Aggregation without `GROUP BY` | Fails                                                                                                                                    |
| Repartitioning in local mode   | Spills buffered batches                                                                                                                  |
| Nested loop join               | Spills, except a full outer join whose other input has more than one partition                                                           |
| Sort-merge join                | Spills its buffered input, but not its streamed input                                                                                    |
| Hash join                      | Fails, because the hash table built from one input cannot spill                                                                          |
| Cross join                     | Fails                                                                                                                                    |
| `ORDER BY` with `LIMIT`        | Fails                                                                                                                                    |
| Window top-k                   | Fails                                                                                                                                    |

In cluster mode, repartitioning moves data between tasks through the shuffle, which does not reserve memory from the pool.
`df.repartition(n)` buffers a limited number of batches and does not reserve memory from the pool either.

Spilling still needs some memory to make progress.
Each partition of a sort reserves 10 MiB to merge its spilled data, starting from its first batch.
Merging sorted partitions also reserves memory that cannot spill.
A pool that is too small for the number of partitions running at once can therefore fail even for operators that spill.

If a hash join fails because the pool refuses a reservation, you can disable `optimizer.prefer_hash_join`.
Sail then plans sort-merge joins, which can spill.
Refer to the [Join Strategies](./joins.md#options) page for the exceptions and the trade-off.

Window top-k is an optimization that `optimizer.enable_window_topn` enables.
For a `ROW_NUMBER` or `RANK` window with `PARTITION BY` and an upper-bound filter, such as `rn <= 10`, it replaces the full sort with a per-partition top-k operator.
An equality filter such as `rn = 1` does not qualify.
Write `rn <= 1` instead.
This can improve performance when the window partition keys have low cardinality.
The option is disabled by default because the top-k operator can increase memory usage and runtime when the partition keys have high cardinality.
Large limits and large groups of tied ranks have the same effect.
The top-k operator cannot spill.

## Temporary Files

Operators write spill data to temporary files.
`runtime.temporary_files.paths` lists the local directories for these files.
If the list is empty, which is the default, Sail uses the operating system temporary directory.

`runtime.temporary_files.max_size` limits the total size of temporary files, and defaults to 64 GiB.
Setting it to `0` disables temporary files.
Operators that would spill then fail instead.

`runtime.temporary_files.compression` controls the compression of temporary files, and defaults to `uncompressed`.
`lz4_frame` compresses and decompresses faster, while `zstd` produces smaller files.

```bash
export SAIL_RUNTIME__TEMPORARY_FILES__PATHS='["/mnt/sail-spill"]'
export SAIL_RUNTIME__TEMPORARY_FILES__MAX_SIZE=107374182400
```

## Memory Outside the Pool

The memory pool covers the memory that operators reserve.
Sail also uses memory that the pool does not track, including memory for the following.

- window functions
- data buffered between tasks
- query planning
- network buffers
- Python UDFs
- the [Parquet metadata cache](./data-layout.md#read-caches)

Leave headroom between the maximum size of the pool and the memory available to Sail.

`runtime.stack_size` sets the stack size of the threads that Sail uses to plan and run queries, and defaults to 8 MiB.
If a query with deeply nested expressions or plans fails with a stack overflow during planning, increase this value.

## Monitoring

When [metrics export](/guide/observability/#telemetry) is enabled, `execution.spill_count` and `execution.spill_size` show whether each operator spilled, and how much.
Some operators also report their peak memory.
`execution.join.build_side_peak_memory_used` covers the build side of hash, nested loop, and cross joins.
`execution.join.peak_memory_used` covers sort-merge joins.
`execution.aggregate.peak_memory_used` covers aggregations with grouping sets, such as `ROLLUP` and `CUBE`.
