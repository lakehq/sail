---
title: AWS Glue
rank: 4
---

# AWS Glue Data Catalog

The AWS Glue catalog provider in Sail allows you to connect to an external [AWS Glue Data Catalog](https://aws.amazon.com/glue/).

## Options

AWS Glue catalog can be configured using the following options.

- `type` (required): The catalog provider. Set this option to `glue`.
- `name` (required): The catalog name.
- `catalog_id` (optional): The AWS Glue Data Catalog ID. If this option is not set, AWS uses the account ID associated with the active credentials.
- `region` (optional): The AWS region. Set this option to a region such as `us-east-1`. If it is not set, AWS uses the default region from the AWS credential provider chain.
- `endpoint_url` (optional): The custom endpoint URL.

See [Common Options](./index.md#common-options) for caching configuration.

You can use any AWS credential provider supported by the AWS SDK to authenticate with AWS Glue.

## Examples

This example uses the AWS credential provider chain with a catalog in `us-west-2`.

```bash
export SAIL_CATALOG__LIST='[{type="glue", name="sail", region="us-west-2"}]'
```

This example selects an AWS Glue Data Catalog by ID.

```bash
export SAIL_CATALOG__LIST='[{type="glue", name="sail", catalog_id="123456789012", region="us-west-2"}]'
```

This example uses a custom endpoint, such as a LocalStack instance.

```bash
export SAIL_CATALOG__LIST='[{type="glue", name="sail", region="us-east-1", endpoint_url="http://localhost:4566"}]'
```

This example enables shared caching for database and table listings.

```bash
export SAIL_CATALOG__LIST='[{type="glue", name="sail", region="us-west-2", database_cache_type="global", database_cache_size=100, database_cache_ttl_secs=3600, table_cache_type="global", table_cache_size=1000, table_cache_ttl_secs=3600}]'
```

This example uses a session-scoped database cache, which is private to one session.

```bash
export SAIL_CATALOG__LIST='[{type="glue", name="sail", region="us-west-2", database_cache_type="session"}]'
```
