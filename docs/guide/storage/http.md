---
title: HTTP/HTTPS Storage
rank: 7
---

# HTTP/HTTPS Storage

Sail provides an object store implementation for generic HTTP servers following RFC 2518, commonly known as WebDAV (Web Distributed Authoring and Versioning).

Basic GET support will work out of the box with most HTTP servers, even those that don't explicitly support RFC 2518. Other operations such as list, delete, copy, put, etc. will likely require server-side configuration. A list of HTTP servers with support can be found [here](https://wiki.archlinux.org/title/WebDAV#Server).

## URL Format

HTTP storage uses standard web URLs:

```
http://example.com/path/to/file
https://example.com/path/to/file
```

## Features and Limitations

### Supported Operations

- **Read (GET)**: :white_check_mark: Works out of the box with most HTTP servers
- **Write (PUT)**: :white_check_mark: Likely requires server-side configuration
- **Delete**: :white_check_mark: Likely requires server-side configuration
- **List**: :white_check_mark: Likely requires server-side configuration
- **Copy**: :white_check_mark: Likely requires server-side configuration
- **Multipart uploads**: :x: Not currently supported

### Supported Formats

Any file format that Sail supports can be read via HTTP:

- Parquet
- CSV
- JSON
- ORC
- Avro
- Text files

## Examples

### Reading Data

```python
# Read a CSV file from a public URL
df = spark.read.option("header", "true").csv("https://example.com/data/sales.csv")

# Read a Parquet file
df = spark.read.parquet("https://data.example.com/datasets/weather.parquet")

# Read JSON data from an API endpoint
df = spark.read.json("https://api.example.com/v1/data.json")
```

### Authentication

For URLs that require authentication, you can configure headers through environment variables:

```bash
# Basic authentication
export HTTP_BASIC_AUTH="username:password"

# Bearer token
export HTTP_BEARER_TOKEN="your-token-here"

# Custom headers
export HTTP_HEADER_Authorization="Bearer your-token"
export HTTP_HEADER_X_API_Key="your-api-key"
```

## Special URL Handling

::: info
Some HTTPS URLs are automatically recognized as cloud storage and will use the appropriate backend instead of the generic HTTP store:

**Azure Storage** (URLs ending with):

- `dfs.core.windows.net`
- `blob.core.windows.net`
- `dfs.fabric.microsoft.com`
- `blob.fabric.microsoft.com`

**S3 Storage** (URLs ending with):

- `amazonaws.com` (special handling: URLs starting with `s3.amazonaws.com` strip the bucket from path)
- `r2.cloudflarestorage.com`
  :::

## Use Cases

### Public Datasets

```python
# Read public government data
df = spark.read.csv("https://data.gov/datasets/census.csv")

# Read from research repositories
df = spark.read.parquet("https://research.example.edu/climate-data.parquet")
```

### REST APIs

```python
# Read JSON data from REST endpoints
df = spark.read.json("https://api.example.com/v2/products")

# Read paginated API responses
dfs = []
for page in range(1, 10):
    df = spark.read.json(f"https://api.example.com/data?page={page}")
    dfs.append(df)
result = dfs[0].union(*dfs[1:])
```

### Content Delivery Networks (CDN)

```python
# Read from CDN-hosted files
df = spark.read.parquet("https://cdn.example.com/datasets/transactions.parquet")
```

## Writing Data

To write data and perform other operations beyond basic reads, your server will likely require WebDAV configuration. A list of HTTP servers with WebDAV support can be found [here](https://wiki.archlinux.org/title/WebDAV#Server).

### Writing Examples

```python
# Write DataFrame to WebDAV server
df.write.mode("overwrite").parquet("https://webdav.example.com/data/output.parquet")

# Write CSV files
df.write.option("header", "true").csv("https://webdav.example.com/data/results/")

# Write partitioned data
df.write.partitionBy("year").parquet("https://webdav.example.com/data/partitioned/")
```

### WebDAV Operations

```python
# The HTTP store also supports other WebDAV operations through Sail's API
# These include delete, copy, and list operations when the server supports them
```

## Performance Considerations

- **No parallel reads**: Unlike cloud storage, HTTP typically doesn't support range requests for parallel reading
- **Caching**: Frequently accessed HTTP resources are cached to reduce network calls
- **Timeout configuration**: Large files may require adjusted timeout settings
- **Bandwidth limitations**: Performance depends on network bandwidth and server capabilities

## Configuration Options

Additional HTTP client configurations can be set via environment variables:

```bash
# Set connection timeout (in seconds)
export HTTP_CONNECT_TIMEOUT="30"

# Set read timeout (in seconds)
export HTTP_TIMEOUT="300"

# Set maximum retry attempts
export HTTP_MAX_RETRIES="3"
```
