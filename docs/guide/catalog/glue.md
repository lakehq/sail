---
title: AWS Glue
rank: 4
---

# AWS Glue Data Catalog

The AWS Glue catalog provider in Sail allows you to connect to an external [AWS Glue Data Catalog](https://aws.amazon.com/glue/).

## Configuration

AWS Glue catalog can be configured using the following options:

- `type` (required): The string `glue`.
- `name` (required): The name of the catalog.
- `region` (optional): The AWS region (e.g., `us-east-1`). If not set, it uses the default region from the AWS credential provider chain.
- `endpoint_url` (optional): The custom endpoint URL. Useful for VPC endpoints or local development with tools like [LocalStack](https://localstack.cloud/) or [Moto](https://github.com/getmoto/moto).

You can use any AWS credential provider supported by the AWS SDK to authenticate with AWS Glue.

## Caching

Sail provides an optional caching layer to improve performance when listing databases and tables in a large Glue catalog. Caching is configured per catalog provider instance and is disabled by default.

The following caching options can be set alongside the provider options above:

- `database_cache_enabled` (optional): Whether to enable caching for database listings. Defaults to `false`.
- `database_cache_size` (optional): Maximum number of entries in the database list cache. Setting the value to `0` disables the limit.
- `database_cache_ttl_secs` (optional): Time-to-live in seconds for cached database listings. Setting the value to `0` disables the TTL.
- `table_cache_enabled` (optional): Whether to enable caching for table and view listings. Defaults to `false`.
- `table_cache_size` (optional): Maximum number of entries in the table/view list cache. Setting the value to `0` disables the limit.
- `table_cache_ttl_secs` (optional): Time-to-live in seconds for cached table/view listings. Setting the value to `0` disables the TTL.

When an operation that modifies the catalog (such as `CREATE TABLE`, `DROP TABLE`, `CREATE VIEW`, or `DROP VIEW`) is performed through Sail, the relevant cache entries are automatically invalidated to ensure consistency.

::: info
The caching layer is a generic feature available to all remote catalog providers (Glue, Hive Metastore, Iceberg REST, Unity, OneLake). The configuration options are the same across all providers.
:::

## Support Status

The following operations are supported:

- Database operations: `CREATE DATABASE`, `DROP DATABASE`, `SHOW DATABASES`.
- Table operations: `CREATE TABLE`, `DROP TABLE`, `SHOW TABLES`, `DESCRIBE TABLE`.
- View operations: `CREATE VIEW`, `DROP VIEW`, `SHOW VIEWS`.
- Table formats: Hive-style tables (`parquet`, `csv`, `json`, `orc`, `avro`) and Iceberg tables (via the Glue OpenTableFormatInput API).
- Partition transforms for Iceberg tables (identity, year, month, day, hour, bucket, truncate).

The following limitations apply:

- `ALTER TABLE` is a no-op at the catalog layer. For Delta or Iceberg tables, property changes are persisted by the storage-side commit.
- `CREATE OR REPLACE VIEW` is not supported by the Glue API.
- `PURGE` option on `DROP TABLE` is not supported.
- Multi-level database namespaces are not supported (Glue uses flat database names).

## Examples

### Basic Configuration

```bash
export SAIL_CATALOG__LIST='[{type="glue", name="sail", region="us-west-2"}]'
```

### Custom Endpoint

```bash
# Using a custom endpoint (e.g., LocalStack)
export SAIL_CATALOG__LIST='[{type="glue", name="sail", region="us-east-1", endpoint_url="http://localhost:4566"}]'
```

### With Caching Enabled

```bash
export SAIL_CATALOG__LIST='[{
  "type": "glue",
  "name": "sail",
  "region": "us-east-1",
  "database_cache_enabled": true,
  "database_cache_size": 100,
  "database_cache_ttl_secs": 3600,
  "table_cache_enabled": true,
  "table_cache_size": 1000,
  "table_cache_ttl_secs": 1800
}]'
```

- `database_cache_enabled`: Whether to enable caching for database metadata.
- `database_cache_size`: The maximum number of database metadata entries to cache. When the limit is reached, older entries are evicted.
- `database_cache_ttl_secs`: The time-to-live (TTL) for cached database metadata in seconds.
- `table_cache_enabled`: Whether to enable caching for table metadata.
- `table_cache_size`: The maximum number of table metadata entries to cache. When the limit is reached, older entries are evicted.
- `table_cache_ttl_secs`: The time-to-live (TTL) for cached table metadata in seconds.
