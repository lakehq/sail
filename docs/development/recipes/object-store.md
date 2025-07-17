---
title: Object Store Testing
rank: 50
---

# Object Store Testing

The `compose.yml` file in the project defines the containerized local testing environment.
You can use container orchestration tools such as [Docker Compose](https://docs.docker.com/compose/)
or [Podman Compose](https://github.com/containers/podman-compose) to manage the containers that mock external services.

We use [MinIO](https://min.io/) for S3 and [Azurite](https://github.com/Azure/Azurite) for Azure Storage in local testing.
This guide shows you how to test object store data access locally.

## Managing the Containers

<!-- TODO: add instructions for Podman Compose -->

The following instructions use Docker Compose to manage the containers.

Run the following command to start all the services in the background.

```bash
docker compose up -d
```

Run the following command to stop and remove all the services, and delete the volumes.

```bash
docker compose down --volumes
```

## Running the Spark Connect Server

Use the following command to run the Spark Connect server.
The environment variables related to AWS must match the configuration `compose.yml`.

```bash
env \
  AWS_ACCESS_KEY_ID="sail" \
  AWS_SECRET_ACCESS_KEY="password" \
  AWS_ENDPOINT="http://localhost:19000" \
  AWS_VIRTUAL_HOSTED_STYLE_REQUEST="false" \
  AWS_ALLOW_HTTP="true" \
  AZURE_STORAGE_ACCOUNT_NAME="devstoreaccount1" \
  AZURE_STORAGE_ACCOUNT_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" \
  AZURE_STORAGE_ENDPOINT="http://localhost:10000/devstoreaccount1" \
  AZURE_STORAGE_USE_EMULATOR="true" \
  HADOOP_USER_NAME="sail" \
  hatch run scripts/spark-tests/run-server.sh
```

## Testing the Object Store

### S3

You need to create the bucket manually by visiting the MinIO console at <http://localhost:19001/>.
Log in with the credentials configured in `compose.yml`.

### Azure

First, install the Azure Storage Python client libraries:

```bash
pip install azure-storage-blob azure-storage-file-datalake
```

Then create the container using Python:

```python
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient

connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.create_container("foo")

datalake_service_client = DataLakeServiceClient.from_connection_string(connection_string)
file_system_client = datalake_service_client.create_file_system("meow")
```

You can then test the object store by using a [local PySpark session](./pyspark-local.md).

### Example Code

```python
# S3
path = "s3://foo/bar.parquet"
spark.sql("SELECT 1").write.parquet(path)
spark.read.parquet(path).show()

# Azure Blob Storage
path = "azure://foo/bar.parquet"
spark.sql("SELECT 1").write.parquet(path)
spark.read.parquet(path).show()

# Azure DataLake Storage
path = "abfss://meow/bar.parquet"
spark.sql("SELECT 1").write.parquet(path)
spark.read.parquet(path).show()
```
