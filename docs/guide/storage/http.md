---
title: HTTP/HTTPS Storage
rank: 7
---

# HTTP/HTTPS Storage

Sail provides an object store implementation for generic HTTP servers following [RFC 2518](https://datatracker.ietf.org/doc/html/rfc2518), commonly known as [WebDAV](https://en.wikipedia.org/wiki/WebDAV).

Basic GET support works out of the box with most HTTP servers, even those that don't explicitly support RFC 2518. Other operations such as list, delete, copy, put, etc. will likely require server-side configuration. A list of HTTP servers with WebDAV support can be found [here](https://wiki.archlinux.org/title/WebDAV#Server).

## URL Format

```
http://example.com/path/to/file
https://example.com/path/to/file
```

## Supported Operations

- **Read (GET)**: Works out of the box with most HTTP servers
- **Write (PUT)**: Likely requires server-side configuration
- **Delete**: Likely requires server-side configuration
- **List**: Likely requires server-side configuration
- **Copy**: Likely requires server-side configuration
- **Multipart uploads**: Not currently supported

## Reading Data

### Basic Examples

```python
# Read CSV from a public URL
df = spark.read.option("header", "true").csv("https://example.com/data/sales.csv")

# Read Parquet file
df = spark.read.parquet("https://data.example.com/datasets/weather.parquet")

# Read JSON from API endpoint
df = spark.read.json("https://api.example.com/v1/data.json")
```

### Authentication

Configure authentication through environment variables:

```bash
# Basic authentication
export HTTP_BASIC_AUTH="username:password"

# Bearer token
export HTTP_BEARER_TOKEN="your-token-here"

# Custom headers
export HTTP_HEADER_Authorization="Bearer your-token"
export HTTP_HEADER_X_API_Key="your-api-key"
```

## Writing Data

Writing requires a WebDAV-configured server:

```python
# Write Parquet
df.write.mode("overwrite").parquet("https://webdav.example.com/data/output.parquet")

# Write CSV
df.write.option("header", "true").csv("https://webdav.example.com/data/results/")

# Write partitioned data
df.write.partitionBy("year").parquet("https://webdav.example.com/data/partitioned/")
```

## Special URL Handling

::: info
Some HTTPS URLs are automatically recognized as cloud storage and routed through the appropriate backend:

**Azure Storage** (URLs ending with):

- `dfs.core.windows.net`
- `blob.core.windows.net`
- `dfs.fabric.microsoft.com`
- `blob.fabric.microsoft.com`

**S3 Storage** (URLs ending with):

- `amazonaws.com` (URLs starting with `s3.amazonaws.com` strip the bucket from path)
- `r2.cloudflarestorage.com`
  :::

## Configuration

Set additional HTTP client options via environment variables:

```bash
# Connection timeout (seconds)
export HTTP_CONNECT_TIMEOUT="30"

# Read timeout (seconds)
export HTTP_TIMEOUT="300"

# Maximum retry attempts
export HTTP_MAX_RETRIES="3"
```

## Performance Considerations

- HTTP typically doesn't support range requests for parallel reading
- Frequently accessed resources are cached to reduce network calls
- Large files may require adjusted timeout settings
- Performance depends on network bandwidth and server capabilities
