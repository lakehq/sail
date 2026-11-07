---
title: Query Plans
rank: 5
---

# Query Plans

The query plan shows how Sail runs a query, including the join algorithms it chose and where it repartitions data.
Refer to the [Query Planning](/concepts/query-planning/) page for how Sail creates the plan.

## Explain Modes

You can show the plan with the `DataFrame.explain()` method or the `EXPLAIN` SQL statement.
By default, both show the physical plan.

```python
df.explain()
df.explain(mode="extended")
```

The following table lists the explain modes and the plans each mode shows.

| Mode        | DataFrame method               | SQL statement       | Output                                                                     |
| ----------- | ------------------------------ | ------------------- | -------------------------------------------------------------------------- |
| `simple`    | `df.explain()`                 | `EXPLAIN`           | Physical plan                                                              |
| `extended`  | `df.explain(mode="extended")`  | `EXPLAIN EXTENDED`  | Parsed, analyzed, and optimized logical plans, and the physical plan       |
| `cost`      | `df.explain(mode="cost")`      | `EXPLAIN COST`      | Optimized logical plan, and the physical plan with statistics              |
| `formatted` | `df.explain(mode="formatted")` | `EXPLAIN FORMATTED` | Physical plan with statistics and schema                                   |
| `codegen`   | `df.explain(mode="codegen")`   | `EXPLAIN CODEGEN`   | Plan after each planning step, the physical plan, and the distributed plan |
| `verbose`   | Not available                  | `EXPLAIN VERBOSE`   | Physical plan, followed by the same plan with statistics and with schema   |
| `analyze`   | Not available                  | `EXPLAIN ANALYZE`   | Physical plan with statistics, schema, and metrics from running the query  |

Sail does not generate code for queries.
Instead, the `codegen` mode shows the plan after each planning step and the distributed plan.

## Reading the Physical Plan

The physical plan is a tree of operators.
Each line shows one operator.
The operators that provide its input appear below it with deeper indentation.
The following operators show planning decisions that affect performance.

| Operator in the plan                            | Meaning                                                                                             |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `RepartitionExec: partitioning=Hash`            | Rows are redistributed by the listed expressions                                                    |
| `RepartitionExec: partitioning=RoundRobinBatch` | Batches are spread evenly across partitions                                                         |
| `ExplicitRepartitionExec`                       | Round-robin repartitioning requested by `df.repartition(n)` or a `REPARTITION` hint without columns |
| `HashJoinExec: mode=CollectLeft`                | Broadcast join. In cluster mode, Sail can change it to a hash join on repartitioned inputs          |
| `HashJoinExec: mode=Partitioned`                | Hash join with both inputs repartitioned by the join keys                                           |
| `SortMergeJoinExec`                             | Sort-merge join                                                                                     |
| `DataSourceExec: file_groups`                   | File scan, where the number of file groups is the number of partitions                              |

Refer to the [Join Strategies](./joins.md) and [Parallelism and Batching](./parallelism.md) pages for how Sail makes these decisions.

## Runtime Metrics

`EXPLAIN ANALYZE` runs the query and shows the physical plan with metrics for each operator, such as the number of output rows and the elapsed time.
Since it runs the query, `EXPLAIN ANALYZE` also performs any writes in the query.

In cluster mode, `EXPLAIN ANALYZE` runs the whole query in the server process instead of on the workers.
Its metrics therefore do not reflect distributed execution.
The server also needs enough memory to run the query.
To measure a query on the cluster, export metrics.
See [Telemetry](/guide/observability/#telemetry).

## Distributed Plan

In cluster mode, Sail divides the physical plan into stages that exchange data through shuffles.
The `codegen` mode shows this division as the distributed plan.
In local mode, the distributed plan shows how Sail would divide this physical plan into stages in cluster mode with the Arrow Flight shuffle backend.

The `system.execution.stages` and `system.execution.tasks` tables in the [system catalog](/guide/observability/#system-catalog) show the stages and task attempts of jobs that run in cluster mode.

Plans for the same query can differ between local mode and cluster mode.
See [Join Strategies](./joins.md#cluster-mode).
