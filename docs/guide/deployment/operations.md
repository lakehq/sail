---
title: Operations and Limits
rank: 9
---

# Operations and Limits

## Readiness

The provided server uses gRPC startup and readiness probes, which call the health service for `spark.connect.SparkConnectService`.
The health service reports `SERVING` as soon as the server registers the Spark Connect service at process start, before any session, driver, or worker exists.
A passing probe therefore does not mean that the server can create sessions, launch workers, reach storage, or return correct results.
The probe also does not test Service routing, NetworkPolicy, or authentication.
To check a deployment, run a Spark Connect query through the same path that your clients use.

The base has no liveness probe, because restarting the server ends every session that it hosts.
Add a liveness probe only if you have found a state from which the server cannot recover.

## Sessions and Timeouts

`spark.session_timeout_secs`, which defaults to `900`, counts the time since the last client request in a session.
Task progress and execution heartbeats do not reset it.
A reattachable client resets it with each `ReattachExecute` or `ReleaseExecute` request, and PySpark uses reattachable execution by default.
A result stream that is not reattachable sends no client requests.
If it runs longer than the timeout, Sail removes the session and stops the query.
If the client disconnects, only a reattachable operation can resume its result stream.

Sail keeps old session state.
The session manager keeps the ID of every session as running, deleted, or failed.
It rejects a new session that reuses the ID of a session that is no longer running.
The system catalog also keeps the session, job, stage, task, and worker rows.
With the default in-memory store, the memory of the server therefore grows with the number of sessions.
Monitor it on servers that run for a long time.

## Shutdown

The server shuts down gracefully on `SIGINT`.
It has no handler for `SIGTERM`.
In the provided images, the server is the first process in its container.
As the first process, it ignores `SIGTERM` and keeps running until Kubernetes kills it with `SIGKILL` at the end of the termination grace period.
`deployments/production/Dockerfile` declares `STOPSIGNAL SIGINT`.
With container runtimes that use the stop signal of the image, such as containerd and CRI-O, Kubernetes stops a server built from it with `SIGINT`.
An image built with the [production image guide](./docker-images/production.md) declares no stop signal.
Worker pods run `sail worker` as the first process, which has no handler for `SIGINT` or `SIGTERM`.
A deleted worker pod keeps running until Kubernetes kills it with `SIGKILL` at the end of the termination grace period.

## Observability

See [Observability](/guide/observability/) for logs, the system catalog, and telemetry.
On Kubernetes, the `disk` store of the system catalog writes to `catalog.system.disk.path` in the server pod.
That path must be a writable volume, because the provided pods use a read-only root filesystem.
Sail does not include an OpenTelemetry collector, an observability backend, or dashboards.

## Upgrades

Replacing the server pod ends every session on it, including during an upgrade or a rollback.
The base PodDisruptionBudget keeps Kubernetes from evicting the server pod.
Draining its node waits until you delete the server pod yourself.
Options and behavior can change between releases.
Refer to the [Changelog](/reference/changelog/) before you upgrade.
