---
title: Object Store Testing
rank: 50
---

# Object Store Testing

The `compose.yml` file in the project defines the containerized local testing environment.
You can use container orchestration tools such as [Docker Compose](https://docs.docker.com/compose/)
or [Podman Compose](https://github.com/containers/podman-compose) to manage the containers that mock external services.

We use [MinIO](https://min.io/) as the object store for local testing.
This guide shows you how to test AWS S3 data access locally.

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
  HADOOP_USER_NAME="sail" \
  scripts/spark-tests/run-server.sh
```

## Testing the Object Store

You can test the object store using a [local PySpark session](./pyspark-local.md).
Here is an example code snippet to use.

```python
path = "s3://foo/bar.json"
spark.sql("SELECT 1").write.json(path)
spark.read.json(path).show()
```

::: warning
You need to create the bucket manually by visiting the MinIO console at <http://localhost:19001/>.
Log in with the credentials configured in `compose.yml`.
:::
