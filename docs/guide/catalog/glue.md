---
title: AWS Glue
rank: 4
---

# AWS Glue Data Catalog

The AWS Glue catalog provider in Sail allows you to connect to an external [AWS Glue Data Catalog](https://aws.amazon.com/glue/).

AWS Glue catalog can be configured using the following options:

- `type` (required): The string `glue`.
- `name` (required): The name of the catalog.
- `region` (optional): The AWS region (e.g., `us-east-1`). If not set, it uses the default region from the AWS credential provider chain.
- `endpoint_url` (optional): The custom endpoint URL.
- `cache_db_enable` (optional): Whether to enable caching for database listings. Defaults to `false`.
- `cache_table_enable` (optional): Whether to enable caching for table listings. Defaults to `false`.
- `cache_ttl_secs` (optional): The time-to-live in seconds for the cache. Defaults to `1800` (30 minutes).

You can use any AWS credential provider supported by the AWS SDK to authenticate with AWS Glue.

## Caching

To improve performance when listing databases and tables in a large Glue catalog, you can enable the internal cache. 

The cache is shared across all Spark and Flight sessions on the same server instance. When an operation that modifies the catalog (like `CREATE TABLE` or `DROP TABLE`) is performed through Sail, the relevant cache entries are automatically invalidated to ensure consistency.

### Configuration Example

You can enable the cache using environment variables:

```bash
export SAIL_GLUE__CACHE__DB__ENABLE=true
export SAIL_GLUE__CACHE__TABLE__ENABLE=true
export SAIL_GLUE__CACHE__TTL_SECS=3600
```

## Examples

```bash
export SAIL_CATALOG__LIST='[{type="glue", name="sail", region="us-west-2"}]'

# Using a custom endpoint (e.g., LocalStack)
export SAIL_CATALOG__LIST='[{type="glue", name="sail", region="us-east-1", endpoint_url="http://localhost:4566"}]'
```
