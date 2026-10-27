---
title: Observability
rank: 13
---

# Observability

You can observe Sail through its logs, the system catalog, and the OpenTelemetry traces, metrics, and logs that it can export.

## Logging

Sail writes logs to standard error.
Currently, the `RUST_LOG` environment variable controls the logging level.
The default level is `info`.
This may change in the future.

```bash
env RUST_LOG=debug sail spark server
```

Each log line shows the target of the message after its level.
`RUST_LOG` also accepts a comma-separated list of directives that set the level for each target.
For example, `RUST_LOG=info,sail_execution=debug` shows debug messages only from targets that start with `sail_execution`.

## System Catalog

The [system catalog](/guide/catalog/system/) exposes the internal state of Sail as tables that you can query with Spark SQL.
The `system.session.options` table shows the configuration options of the server process.
Use it to confirm that your environment variables took effect.
It does not show settings that you set only for workers.
In cluster mode, the `system.cluster.workers` table and the tables in the `system.execution` database show the workers, jobs, stages, and task attempts of each session.

```python
spark.sql("SELECT * FROM system.execution.tasks").show()
```

The system catalog keeps its data in memory by default.
To keep the data on disk instead, set `catalog.system.store` to `disk` and set `catalog.system.disk.path` to an existing directory.
The disk store has no compatibility guarantee across Sail versions.
Only one Sail process can use a directory at a time.

Refer to the [Security Baseline](/guide/deployment/security-baseline#logs-telemetry-and-system-tables) page for who can read the system catalog, and what it, the logs, and telemetry can contain.

## Telemetry

Sail can export traces, metrics, and logs to an OpenTelemetry collector using the OpenTelemetry Protocol (OTLP).
Export is disabled by default.
Enable each signal that you want to export, and set the endpoint of the collector.

```bash
export SAIL_TELEMETRY__EXPORT_TRACES=true
export SAIL_TELEMETRY__EXPORT_METRICS=true
export SAIL_TELEMETRY__EXPORT_LOGS=true
export SAIL_TELEMETRY__EXPORTER__OTLP__ENDPOINT="http://otel-collector:4317"
```

Sail exports telemetry over gRPC.
The `telemetry.exporter.otlp.protocol` option currently has no effect.
The [Telemetry options](/reference/configuration/#telemetry) in the configuration reference control the export intervals and batch sizes.

You can also query metrics without a collector.
When you enable both `telemetry.export_metrics` and `telemetry.exporter.system.enabled`, Sail writes metrics to the `system.telemetry.metrics` table.
In cluster mode, workers send their metrics for this table to the server if the workers also have both options enabled.

On Kubernetes, worker pods do not inherit the telemetry options of the server.
Refer to the [Cluster Configuration and Sizing](/guide/deployment/configuration#settings-passed-to-workers) page for the settings that the server passes to workers and how to set the others.

## Metrics

Most metrics describe one partition of one operator.
Their attributes identify the session, job, partition, and operator.
You can use them to attribute the cost of a query to individual operators.
In cluster mode, they also identify the stage and the task attempt.
The [Metrics](./metrics.md) page lists every metric.
