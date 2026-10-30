---
title: Security Baseline
rank: 7
---

# Security Baseline

This baseline assumes that you trust the people who write workloads and administer the deployment.
Python code and user-defined functions can use the operating system, network, data, and external identity that their pods have.
The baseline does not isolate workloads from each other.

## Spark Connect Endpoint

Neither Sail nor the `ClusterIP` Service provides authentication, authorization, or encryption for the Spark Connect endpoint.

Clients supply the Spark Connect `user_id` and `session_id` values.
Sail does not authorize requests by `user_id`.
Any caller that knows a session ID can use or release that session.

If clients connect from outside your trusted network, put an access layer in front of the Service.
The access layer must do the following.

- Terminate TLS.
- Authenticate the caller.
- Authorize Spark Connect methods.
- Keep long-lived HTTP/2 streams open.

The gRPC server always exposes reflection, and you cannot disable it.
Keep reflection requests inside the trusted network.

The provided [Kubernetes resources](./kubernetes.md#production-deployment) include no access layer.
Do not expose them through a public load balancer.

## Internal Traffic

Sail does not encrypt driver-worker RPC or Flight shuffle.
Setting `cluster.enable_tls` makes the driver and worker clients connect with TLS and verify the server certificate against the system root certificates.
The Sail servers that these clients connect to do not serve TLS.
Internal connections then work only if something in front of each Sail process terminates TLS with a certificate that the clients trust.

Keep the Celeborn endpoints on a private network, because the native Apache Celeborn client of Sail uses plaintext connections.
See [Client Limits](./shuffle.md#client-limits) for details.

## Kubernetes Identities

The server ServiceAccount can `get`, `create`, and `deletecollection` pods in its namespace.
Standard RBAC cannot restrict pod creation or `deletecollection` by pod name, and the server can get any pod in the namespace.
Use a dedicated namespace for Sail, and control who can change its ServiceAccounts, policies, and admission configuration.

The worker ServiceAccount disables automatic mounting of its token.

Anyone who can change the worker pod template can replace the generated image, command, environment, ServiceAccount, or restart policy of worker pods.
They can also add containers.
Restrict who can change the template in the same way as you restrict who can change the server Deployment.
See [Worker Pod Template](./configuration.md#worker-pod-template) for the merge rules.

## Pod Security

The server and the worker pod template run as a non-root user with the `RuntimeDefault` seccomp profile.
They drop all capabilities, disable privilege escalation, and use a read-only root filesystem.
They mount writable scratch storage at `/tmp`.
The namespace of the example overlay enforces the Restricted Pod Security Standard.

## Network Policy

The provided NetworkPolicy allows the following ingress.

- Any port, from pods labeled `app.kubernetes.io/name: sail` in the same namespace.
  Driver-worker RPC and Flight shuffle need this, because they use ports that the operating system assigns by default.
- Port `50051`, from pods labeled `sail.lakesail.com/spark-connect-client: "true"` in the same namespace or in a namespace labeled `sail.lakesail.com/spark-connect-access: "true"`.

It denies all other ingress to these pods, except traffic from their own node, which Kubernetes always allows.
The base does not restrict egress, because the DNS, Kubernetes API, storage, catalog, telemetry, and identity endpoints that Sail needs differ by environment.

The policy selects pods by label.
Anyone who can create or relabel a pod in the namespace can match it.

## Images and Credentials

Use the same image digest for the server and the workers.

Object store credentials must reach the server and each worker that reads or writes the data.
Which processes need catalog credentials depends on the catalog.
Never put credentials in the image, command arguments, or committed manifests, including an inline token in `SAIL_CATALOG__LIST`.

## Logs, Telemetry, and System Tables

Logs, telemetry, and the tables of the `system` catalog can contain SQL, paths, configuration, errors, user identifiers that clients supply, and data values.
Any session on the server can read the system catalog tables.
Sail shows known secret configuration fields as `[REDACTED]`.
It does not redact other content, restrict the system tables by session, or remove old data.
