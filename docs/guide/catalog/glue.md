---
title: AWS Glue
rank: 4
---

# AWS Glue Data Catalog

The AWS Glue catalog provider in Sail allows you to connect to an external [AWS Glue Data Catalog](https://aws.amazon.com/glue/).

AWS Glue catalog can be configured using the following options:

- `type` (required): The string `glue`.
- `name` (required): The name of the catalog.
- `catalog_id` (optional): The AWS Glue Data Catalog ID. If not set, AWS uses the account ID associated with the active credentials.
- `region` (optional): The AWS region (e.g., `us-east-1`). If not set, it uses the default region from the AWS credential provider chain.
- `endpoint_url` (optional): The custom endpoint URL.

See [Common Options](./index.md#common-options) for caching configuration.

You can use any AWS credential provider supported by the AWS SDK to authenticate with AWS Glue.

## Examples

```bash
export SAIL_CATALOG__LIST='[{type="glue", name="sail", region="us-west-2"}]'

# Using an explicit AWS Glue Data Catalog ID
export SAIL_CATALOG__LIST='[{type="glue", name="sail", catalog_id="123456789012", region="us-west-2"}]'

# Using a custom endpoint (e.g., LocalStack)
export SAIL_CATALOG__LIST='[{type="glue", name="sail", region="us-east-1", endpoint_url="http://localhost:4566"}]'

# Enabling caching for database and table listings
export SAIL_CATALOG__LIST='[{type="glue", name="sail", region="us-west-2", database_cache_type="global", database_cache_size=100, database_cache_ttl_secs=3600, table_cache_type="global", table_cache_size=1000, table_cache_ttl_secs=3600}]'

# Configuring cache scope (global or session)
# Global scope shares the cache across all sessions.
# Session scope keeps the cache private to a single session.
export SAIL_CATALOG__LIST='[{type="glue", name="sail", region="us-west-2", database_cache_type="session"}]'
```
