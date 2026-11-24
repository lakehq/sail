---
title: Unity Catalog
rank: 3
---

# Unity Catalog

The Unity Catalog provider in Sail allows you to connect to an external catalog that exposes
the [Unity Catalog API](https://docs.unitycatalog.io/).

## Options

Unity Catalog can be configured using the following options.

- `type` (required): The catalog provider. Set this option to `unity`.
- `name` (required): The catalog name.
- `uri` (optional): The base URI of the Unity Catalog API. The default is `http://localhost:8080/api/2.1/unity-catalog`.
- `default_catalog` (optional): The default catalog name. The default is `unity`.
- `token` (optional): The authentication token for the Unity Catalog API. The default is an empty value, which disables authentication. See [Unity Catalog Authentication and Authorization](https://docs.unitycatalog.io/server/auth/) for more information.

- `user_agent` (optional): The value of the `User-Agent` header that Sail sends with every request to the Unity Catalog API, and with the requests for an access token. The value is a list of products separated by spaces, each written as `<name>` or `<name>/<version>`, such as `MyPlatform/1.2 MyConnector/0.4`. The default is `Sail/<version> Delta Spark`. See [User-Agent](#user-agent).

See [Common Options](./index.md#common-options) for caching configuration.

## Environment Variables

::: warning
The environment variables only allow configuring one Unity Catalog provider instance per session. We may stop supporting these environment variables in future versions of Sail when we support the options in the catalog provider list.
:::

Sail automatically loads Unity Catalog configuration from environment variables with the following prefixes:

- `DATABRICKS_*`: Databricks-specific configuration.
- `UNITY_*`: Unity Catalog configuration.
- `UC_*`: Short-form Unity Catalog configuration.

The following table lists the supported configuration keys and their corresponding environment variable names:

- `DATABRICKS_HOST`, `DATABRICKS_WORKSPACE_URL`, `UNITY_HOST`, `UNITY_WORKSPACE_URL`, `UC_HOST`, `UC_WORKSPACE_URL`

  The URL of the workspace, such as `https://<workspace>.cloud.databricks.com`. The Unity Catalog API path `/api/2.1/unity-catalog` is added if the URL has no path.

- `DATABRICKS_TOKEN`, `DATABRICKS_ACCESS_TOKEN`, `UNITY_ACCESS_TOKEN`, `UC_ACCESS_TOKEN`

  The access token.

- `DATABRICKS_CLIENT_ID`, `UNITY_CLIENT_ID`, `UC_CLIENT_ID`

  The client ID.

- `DATABRICKS_CLIENT_SECRET`, `UNITY_CLIENT_SECRET`, `UC_CLIENT_SECRET`

  The client secret. When the client ID and the client secret are set without the authority ID, the access token is requested from the OAuth endpoint of the workspace (`/oidc/v1/token` at the host of the `uri`).

- `DATABRICKS_AUTHORITY_ID`, `UNITY_AUTHORITY_ID`, `UC_AUTHORITY_ID`

  The authority ID.

- `DATABRICKS_AUTHORITY_HOST`, `UNITY_AUTHORITY_HOST`, `UC_AUTHORITY_HOST`

  The authority host.

- `DATABRICKS_MSI_ENDPOINT`, `UNITY_MSI_ENDPOINT`, `UC_MSI_ENDPOINT`

  The MSI endpoint.

- `DATABRICKS_MSI_RESOURCE_ID`, `UNITY_MSI_RESOURCE_ID`, `UC_MSI_RESOURCE_ID`

  The MSI resource ID.

- `DATABRICKS_OBJECT_ID`, `UNITY_OBJECT_ID`, `UC_OBJECT_ID`

  The object ID.

- `DATABRICKS_FEDERATED_TOKEN_FILE`, `UNITY_FEDERATED_TOKEN_FILE`, `UC_FEDERATED_TOKEN_FILE`

  The federated token file.

- `DATABRICKS_USE_AZURE_CLI`, `UNITY_USE_AZURE_CLI`, `UC_USE_AZURE_CLI`

  Whether to use Azure CLI to fetch token.

- `DATABRICKS_ALLOW_HTTP_URL`, `UNITY_ALLOW_HTTP_URL`, `UC_ALLOW_HTTP_URL`

  Whether to allow HTTP URL.

- `DATABRICKS_USER_AGENT`, `UNITY_USER_AGENT`, `UC_USER_AGENT`

  The value of the `User-Agent` header. See the `user_agent` option.

## User-Agent

Sail sends a `User-Agent` header with every request to the Unity Catalog API, and with the requests for an access token. The default value names Sail and its version, followed by the products `Delta` and `Spark`. For example, Sail 0.7.3 sends the following header.

```text
User-Agent: Sail/0.7.3 Delta Spark
```

The products `Delta` and `Spark` say that Sail implements the Delta protocol and the Spark API. Databricks accepts requests of the Unity Catalog Delta API that write only from clients that it recognizes by the products in this header. This applies to requests for storage credentials for writing, and to requests that create staging tables, create tables, or commit to tables. A header that contains both a `Delta` product and a `Spark` product is accepted, and a header that contains only `Sail/<version>` is rejected. Requests that read are not affected. The open source Unity Catalog server does not examine the header.

You can replace the default value with the `user_agent` option, or with one of the environment variables `DATABRICKS_USER_AGENT`, `UNITY_USER_AGENT`, and `UC_USER_AGENT`. The value that you set is sent as it is.

```bash
export SAIL_CATALOG__LIST='[{type="unity", name="sail", uri="https://<workspace>.cloud.databricks.com/api/2.1/unity-catalog", default_catalog="main", token="...", user_agent="Sail/0.7.3 Delta Spark MyPlatform/1.2"}]'
```

If the server rejects the header, the statement fails with an error like the following one. In this case, set `user_agent` to a value that the server accepts. Databricks accepts the default value of Sail.

```text
Unity Catalog rejected the `User-Agent` header that Sail sent. Set the `user_agent` option of the catalog to a value that the server accepts, see https://docs.lakesail.com/sail/latest/guide/catalog/unity.html#user-agent. Failed to create staging table: ... The UC Delta API requires clients to identify the calling application in the User-Agent header. The provided User-Agent 'MyPlatform/1.2' is insufficient. ...
```

## Storage Credentials

For a Delta table whose commits are managed by the catalog, Sail asks Unity Catalog for temporary storage credentials for the location of the table, and reads and writes the table with these credentials. The credentials are only used in the session that requested them, and Sail requests new credentials before they expire. If the catalog does not issue credentials for a table, Sail accesses the location with the storage credentials of the environment.

### Cluster Mode

When Sail runs in the `local-cluster` or `kubernetes-cluster` mode, the driver requests the storage credentials from the catalog, and the workers get them from the driver.

- The driver sends each worker the locations of the tables that the session uses, and a random token of the session, together with the tasks. No storage credentials are sent with the tasks.
- A worker sends the token to the driver to get the storage credentials for a location when it first reads or writes the location, and again before the credentials expire.
- With the token, a worker can get the storage credentials for reading or writing every table that the session has used, as far as the catalog issues them to the user of the session.

::: warning
The token and the storage credentials are sent between the driver and the workers without encryption. Sail serves the driver port and the worker ports without TLS, and the `cluster.enable_tls` option does not change this.

You must run the driver and the workers in a network that protects their ports and the traffic between them, for example with a service mesh that uses mutual TLS, or with Kubernetes network policies that only let the driver pod and the worker pods of Sail connect to each other.
:::

## Example

```bash
export UNITY_ALLOW_HTTP_URL='false'
export SAIL_CATALOG__LIST='[{type="unity", name="sail", uri="https://catalog.example.com", default_catalog="meow", token="..."}]'
```
