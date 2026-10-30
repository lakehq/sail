---
title: OpenLineage
rank: 6
---

# OpenLineage

Sail can emit [OpenLineage](https://openlineage.io/) run events for the queries it executes,
so that you can track data lineage in an OpenLineage-compatible backend such as
[Marquez](https://marquezproject.ai/).

::: info
This feature is experimental. The event content may change in future releases.
:::

## Enabling OpenLineage

OpenLineage is disabled by default. You can enable it via environment variables when starting the Sail server.

```bash
export SAIL_LINEAGE__ENABLED=true
export SAIL_LINEAGE__TRANSPORT=http
export SAIL_LINEAGE__HTTP__URL=http://localhost:5000

sail spark server
```

The following transports are supported.

| Transport      | Description                                                                                    |
| -------------- | ---------------------------------------------------------------------------------------------- |
| `console`      | Writes events to the Sail server log at the `INFO` level. This is the default.                 |
| `http`         | Sends events to the OpenLineage HTTP API (`POST {lineage.http.url}/{lineage.http.endpoint}`).  |
| `file`         | Appends events to the file at `lineage.file.path`, one JSON object per line.                   |
| `object_store` | Writes each event as a separate JSON object under the directory at `lineage.object_store.url`. |

For the `http` transport, set `SAIL_LINEAGE__HTTP__API_KEY` if the backend requires authentication.
The key is sent as a bearer token in the `Authorization` header.

The `object_store` transport supports the same object stores as reading and writing data
(e.g. `s3://`, `hdfs://`, `gs://`, `abfss://`, and `file://`), and uses the same credentials.
For example, the standard AWS environment variables such as `AWS_ENDPOINT_URL` and `AWS_SHARED_CREDENTIALS_FILE`
are used for S3. Each event is written as `<timestamp>_<sequence>_<id>.json` in the directory, where `<timestamp>` is
the time in milliseconds when the event is written, and `<sequence>` is a zero-padded counter of the server process,
so that sorting the file names of a server gives the order in which the events are emitted.

```bash
export SAIL_LINEAGE__ENABLED=true
export SAIL_LINEAGE__TRANSPORT=object_store
export SAIL_LINEAGE__OBJECT_STORE__URL=s3://bucket/lineage/

sail spark server
```

See the [configuration](/guide/configuration/) page for the full list of `lineage.*` options.

## Run Events

For each query that reads or writes datasets, Sail emits a `START` event when the query starts,
followed by one of the events below when the query ends.

| Event      | Description                                                                        |
| ---------- | ---------------------------------------------------------------------------------- |
| `COMPLETE` | All results of the query have been produced.                                       |
| `FAIL`     | The query failed. The error is reported in the `errorMessage` run facet.           |
| `ABORT`    | The query was cancelled, or the client stopped reading the results before the end. |

Queries and commands that do not involve any dataset (e.g. `SET` or `CREATE VIEW`) do not emit events.

Each session also has an application run, which is the parent of all query runs in the session.
Similar to the OpenLineage Spark integration, the application run emits a `START` event before the first query run
of the session, and a `COMPLETE` event when the session is released (e.g. via `spark.stop()`),
when the session times out, or when the server shuts down.
Sessions that do not run any query involving datasets do not emit application run events.

Events are queued and sent in the background, so lineage emission never blocks query execution.
If the backend is unavailable, the failures are logged as warnings and the events are dropped.
When the server shuts down, it waits up to `lineage.shutdown_timeout_secs` seconds for the queued events to be sent.

## Jobs

Each run belongs to a job in the namespace configured by `lineage.namespace` (`sail` by default).
The job of the application run is named `<app>`, and has the `APPLICATION` job type.
The job of a query run has a name of the form `<app>.<operation>[.<output>]`, where:

- `<app>` is the Spark application name (`spark.app.name`), or `sail` if it is not set.
- `<operation>` is one of `query`, `write`, `delete`, or `merge`.
- `<output>` is the name of the dataset written by the query, if any.
  The table name is used for catalog tables.

For example, the following PySpark code emits events for a job named `daily_report.write.sail_default_report`.

```python
spark = SparkSession.builder.remote("sc://localhost:50051").appName("daily_report").getOrCreate()
spark.sql("CREATE TABLE report AS SELECT * FROM parquet.`s3://bucket/events`")
```

## Session Options

Spark Connect clients can set the following Spark configuration options for their session.
The options use the same keys as the OpenLineage Spark integration,
so orchestrators that inject these keys into Spark jobs (e.g. Airflow) work with Sail as well.

| Option                                                                                                                     | Description                                                                                                                                                 |
| -------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `spark.openlineage.namespace`                                                                                              | The job namespace, which overrides `lineage.namespace`.                                                                                                     |
| `spark.openlineage.parentJobNamespace`<br>`spark.openlineage.parentJobName`<br>`spark.openlineage.parentRunId`             | The parent run (e.g. an Airflow task) of the application run, reported in the `parent` run facet. All three options must be set. The run ID must be a UUID. |
| `spark.openlineage.rootParentJobNamespace`<br>`spark.openlineage.rootParentJobName`<br>`spark.openlineage.rootParentRunId` | The root parent run (e.g. an Airflow DAG run), reported in the `parent` run facet along with the parent run.                                                |
| `spark.openlineage.job.tags`                                                                                               | Tags reported in the `tags` job facet, in the form `key:value[:source]` and separated by `;`. The source is `CONFIG` if it is not specified.                |
| `spark.openlineage.run.tags`                                                                                               | Tags reported in the `tags` run facet, in the same form as `spark.openlineage.job.tags`.                                                                    |

The `parent` run facet of a query run refers to the application run of the session.
The root of the query run is the root parent run if it is set, or otherwise the parent run if it is set,
or otherwise the application run.
The application run uses the options that are set when the first query run of the session starts.

```python
spark = (
    SparkSession.builder.remote("sc://localhost:50051")
    .config("spark.openlineage.parentJobNamespace", "airflow")
    .config("spark.openlineage.parentJobName", "daily_report.load")
    .config("spark.openlineage.parentRunId", "01890cb8-f7a4-7c8d-b1a5-2b6f6d8e9a10")
    .getOrCreate()
)
```

## Datasets

Input datasets are found from the data sources in the final query plan,
including data sources referenced by views and temporary views.
Output datasets are recorded for writes (e.g. `DataFrameWriter`, `INSERT`, and `CREATE TABLE AS SELECT`),
`DELETE`, and `MERGE`.

Datasets are named by their physical location according to the
[OpenLineage naming conventions](https://openlineage.io/docs/spec/naming).

| Location                                              | Namespace                                        | Name            |
| ----------------------------------------------------- | ------------------------------------------------ | --------------- |
| `/path/to/data` or `file:///path/to/data`             | `file`                                           | `/path/to/data` |
| `s3://bucket/path` (also `s3a://`)                    | `s3://bucket`                                    | `path`          |
| `gs://bucket/path`                                    | `gs://bucket`                                    | `path`          |
| `abfss://container@account.dfs.core.windows.net/path` | `abfss://container@account.dfs.core.windows.net` | `path`          |
| `hdfs://host:port/path`                               | `hdfs://host:port`                               | `/path`         |

For catalog tables, the fully qualified table name is reported in the `symlinks` dataset facet.
Catalog tables without a location are named by the table name in the `lineage.namespace` namespace.

The following dataset facets are reported when the information is available.

| Facet                  | Description                                                                  |
| ---------------------- | ---------------------------------------------------------------------------- |
| `schema`               | The columns of the dataset, including nested fields.                         |
| `dataSource`           | The namespace of the dataset.                                                |
| `storage`              | The table format (e.g. `delta` or `iceberg`) or the file format.             |
| `symlinks`             | The catalog table name.                                                      |
| `lifecycleStateChange` | `CREATE` or `OVERWRITE` for output datasets that are created or overwritten. |

### JDBC Datasets

For reads with the [JDBC data source](/guide/sources/jdbc/), the input datasets are the database tables,
named according to the OpenLineage naming conventions.
The `dbtable` option can be a table name or a subquery, and the `query` option is a subquery.
For subqueries, the SQL is parsed and one input dataset is reported for each referenced table,
including tables in joins, common table expressions, set operations, and subqueries in the `SELECT`,
`WHERE`, `HAVING`, and `ON` clauses.

| JDBC URL                                                                 | Namespace                        | Name                    |
| ------------------------------------------------------------------------ | -------------------------------- | ----------------------- |
| `jdbc:postgresql://host:port/database`                                   | `postgres://host:port`           | `database.schema.table` |
| `jdbc:redshift://cluster.id.region.redshift.amazonaws.com:port/database` | `redshift://cluster.region:port` | `database.schema.table` |
| `jdbc:mysql://host:port/database`                                        | `mysql://host:port`              | `database.table`        |
| `jdbc:sqlserver://host:port;databaseName=database`                       | `mssql://host:port`              | `database.schema.table` |

Unqualified table names use the default schema (`public` for PostgreSQL and Redshift, and `dbo` for SQL Server),
and the default port of the database is used if the URL does not specify a port.
The namespace only contains the host and port, so the user name and password
in the URL or the reader options never appear in lineage events.

## Embedding Sail in Python Applications

When a Python application runs Sail in-process with `SparkConnectServer`, the server must be stopped
before the process exits, so that the application runs of open sessions are completed and all queued events
are delivered. Start the server in the background and stop it in a `finally` block.
The environment variables must be set before creating the `SparkConnectServer` instance.

```python
import os
import uuid

from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession

os.environ["SAIL_LINEAGE__ENABLED"] = "true"
os.environ["SAIL_LINEAGE__TRANSPORT"] = "object_store"
os.environ["SAIL_LINEAGE__OBJECT_STORE__URL"] = f"s3://bucket/lineage/{dag_id}/{task_id}/{run_id}/"


def tag_value(value: str) -> str:
    # `:` and `;` are separators in the tag options.
    return value.replace(":", "_").replace(";", "_")


# A deterministic run ID, so that the orchestrator and Sail refer to the same parent run.
parent_run_id = uuid.uuid5(uuid.NAMESPACE_URL, f"{dag_id}/{task_id}/{run_id}/{try_number}")

server = SparkConnectServer()
server.start(background=True)
try:
    _, port = server.listening_address
    spark = (
        SparkSession.builder.remote(f"sc://localhost:{port}")
        .appName("daily_report")
        .config("spark.openlineage.namespace", "my_namespace")
        .config("spark.openlineage.parentJobNamespace", "airflow")
        .config("spark.openlineage.parentJobName", f"{dag_id}.{task_id}")
        .config("spark.openlineage.parentRunId", str(parent_run_id))
        .config("spark.openlineage.job.tags", f"dag_id:{tag_value(dag_id)};task_id:{tag_value(task_id)}")
        .config("spark.openlineage.run.tags", f"try_number:{try_number}")
        .getOrCreate()
    )
    try:
        ...  # Run the job.
    finally:
        spark.stop()
finally:
    # Waits until the remaining events are delivered or `lineage.shutdown_timeout_secs` elapses.
    server.stop()
```

::: warning
Do not call `server.start(background=False)` in a daemon thread. The server cannot be stopped in this case,
so events that are still queued when the process exits are lost. This is likely for transports with network
latency (e.g. HTTP or Amazon S3), and the application `COMPLETE` event is lost for sessions that are not stopped.
:::

The object store transport uses the same credentials as reading and writing data in the object store,
e.g. the `AWS_*` environment variables for Amazon S3.

## Limitations

- Column-level lineage is not supported yet.
- JDBC input datasets do not have the `schema` facet, since the data source schema describes the result of the query
  rather than each table. Lineage is only reported for the JDBC URLs listed above, and the table or query
  is parsed with the Sail SQL parser, so database-specific syntax (e.g. `[bracketed]` identifiers in SQL Server)
  may not be recognized. In that case, no input dataset is reported for the read, and the query still runs.
- Events that are still queued are not sent if the server process exits without shutting down the server
  (e.g. when a Python process with a background `SparkConnectServer` exits without calling `server.stop()`).
- Streaming queries emit a `START` event when the query starts, and an `ABORT` event when the query is stopped.
- The transport is configured on the Sail server, so the `spark.openlineage.transport.*` options
  used by the OpenLineage Spark integration are ignored. Other `spark.openlineage.*` options not
  listed above are ignored as well.
