---
title: Web UI
rank: 11
---

# Web UI

The Sail Web UI provides a Spark-history-style view for inspecting applications
served by a Sail Spark Connect server. It is useful when you want to understand
which jobs, stages, tasks, workers, and physical operators were involved in a
query execution.

## Starting the Web UI

Start the Spark Connect server with a UI port.

```bash
sail spark server --ip 127.0.0.1 --port 50051 --ui-port 4040
```

Then open the UI in a browser.

```text
http://127.0.0.1:4040/
```

The server log also prints the bound Web UI address. The UI is served by the
same Sail process and reads the in-memory execution state from the active
session manager.

## Pages

The Web UI exposes a few Spark-compatible history routes. These routes all load
the same single-page application and select the corresponding view.

| Route | Description |
| --- | --- |
| `/` | Application overview |
| `/history/{app_id}/` | Application overview with an application ID in the header |
| `/history/{app_id}/jobs/` | Jobs view |
| `/history/{app_id}/stages/` | Stages view |
| `/history/{app_id}/tasks/` | Tasks view |
| `/history/{app_id}/executors/` | Executors or workers view |

The application ID in the path is used for display and route compatibility. The
current implementation still reads the jobs, stages, tasks, and workers that are
available in the running Sail server.

## Jobs, Stages, Tasks, and Workers

The main views summarize execution state collected by Sail:

- Jobs show submitted work and link to their related stages and tasks.
- Stages are grouped into active, completed, and failed sections.
- Tasks show per-task status and execution metrics.
- Executors show the Sail workers known to the session manager.

The UI polls the server APIs and updates the view as execution state changes.

## Physical Operator DAG

Each stage can show a physical operator DAG. The DAG is built from the recorded
physical plan and enriched with execution metrics when task or plan metrics are
available.

Operator IDs are displayed in Spark-style form, for example
`DataSourceExec (1)`. The numbering follows the physical data flow from upstream
operators to downstream operators. This means source-side operators such as
`DataSourceExec` receive smaller IDs, and downstream adapter operators such as
`ShowStringExec` receive larger IDs.

Some Sail execution operators are implemented as tracing wrappers around an
inner DataFusion plan node. The Web UI collapses duplicate wrapper/inner display
lines so that the same logical operator is not shown twice in the DAG or in the
physical plan text.

## Physical Plan Text

The physical plan text uses the same logical operator IDs as the DAG. It keeps
the original indentation and operator details from the plan, but removes
duplicate wrapper lines when a wrapper only delegates its display string to its
inner child.

For example, a join may be shown as:

```text
HashJoinExec (12)
mode=CollectLeft, join_type=Inner,
on=[(#8@0, #13@0)], projection=[#8@0, #9@1, #10@2]
```

Metrics are displayed only when the operator has metrics. The UI does not render
`metrics: none` for operators without recorded metrics.

## Metrics

Operator metrics come from Sail execution records. When task-level operator
metrics are available, the UI prefers them because they represent actual task
execution. If task metrics are not available, the UI falls back to the physical
plan metrics snapshot.

Metrics are attached to the matching logical operator by operator name and plan
position. The UI merges repeated task metrics for the same operator and sorts
common metrics such as output rows, bytes scanned, elapsed compute time, memory
usage, and join build time near the top.

Wrapper or adapter nodes such as `ShowStringExec`, `MapPartitionsExec`, and
`SchemaPivotExec` report their own baseline metrics when Sail records them.
These nodes do not inherit metrics from their child subtrees in the UI, because
subtree inheritance would make the operator-level metrics misleading.

Data source operators can show native DataFusion metrics such as scanned rows,
bytes, and elapsed compute time when those metrics are reported by the source.
If a specific source or execution path does not emit a metric, the UI leaves
that metric absent instead of fabricating a value.

## Known Limitations

The Web UI is currently focused on local inspection of a running Sail server. It
does not persist application history after the server process exits, and it does
not yet provide authentication, authorization, or multi-application filtering.
When exposing the UI outside localhost, place it behind the same network and
access controls that protect the Spark Connect server.
