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

You can use any AWS credential provider supported by the AWS SDK to authenticate with AWS Glue.

## Examples

```bash
export SAIL_CATALOG__LIST='[{type="glue", name="sail", region="us-west-2"}]'

# Using a custom endpoint (e.g., LocalStack)
export SAIL_CATALOG__LIST='[{type="glue", name="sail", region="us-east-1", endpoint_url="http://localhost:4566"}]'
```
