---
title: Data Access
rank: 1
---

# Data Access

Sail supports accessing data from various sources, including local files and cloud storage services.
You can use the `spark.read` and `spark.write` API with the following types of paths.

Relative file paths

: These are file paths relative to the current working directory, such as `path/data.json`.

`file://` URIs

: These are absolute file paths on the local file system, such as `file:///path/to/file`.

`s3://` URIs

: These are paths in AWS S3 or an S3-compatible object storage, such as `s3://bucket/path/to/data`.

`hdfs://` URIs

: These are paths in HDFS, such as `hdfs://namenode:port/path/to/data`.

`hf://` URIs

: These are paths for Hugging Face datasets, such as `hf://datasets/username/dataset@~parquet/train`.

::: info

- For local file systems, the path can refer to a file or a directory.
- For S3-compatible object storage services, the path can refer to an object or a key prefix.
  We assume the key prefix is followed by `/` and represents a directory.

:::

## Configuring AWS Credentials

You can configure AWS credentials using standard methods supported by the AWS tools and SDKs.
These methods include AWS `config` and `credentials` files,
EC2 instance profiles, and AWS access keys configured in environment variables.

::: info
You can refer to the [AWS documentation](https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html)
for more details about the credential providers.
:::

## Accessing Public Data on AWS S3

Some datasets on S3 allow public access without an AWS account.
You can skip retrieving AWS credentials using the following environment variables.
Note that `AWS_REGION` must match the region of the S3 bucket containing the data.

```text
AWS_SKIP_SIGNATURE=true
AWS_REGION=us-east-1
```

::: info
`AWS_SKIP_SIGNATURE` is not a standard environment variable used by AWS SDKs.
It is an environment variable recognized by Sail.
:::

## Configuring Hugging Face

Files in Hugging Face datasets are cached locally once downloaded.
The cache is shared with other Hugging Face Python tools.
The cache directory is `$HF_HOME/hub` where `$HF_HOME` is an environment variable with the default value `~/.cache/huggingface`.
You can set the `HF_HOME` environment variable to use a different cache directory.

You can set the `HF_ENDPOINT` environment variable to use a different Hugging Face API endpoint (e.g. a mirror). The default endpoint is `https://huggingface.co`.
