import json
import logging
import os
import sys
from contextlib import asynccontextmanager
from typing import AsyncIterator

import uvicorn.server
from mcp.server.fastmcp import Context, FastMCP
from pyspark.sql import SparkSession


def _is_temp_view(table):
    return table.catalog is None and not table.namespace and table.tableType == "TEMPORARY" and table.isTemporary


def _describe_table(table):
    return {"name": table.name, "description": table.description}


def _describe_column(column):
    return {
        "name": column.name,
        "description": column.description,
        "dataType": column.dataType,
        "nullable": column.nullable,
    }


def configure_logging():
    """Configure logging for the MCP server.
    We modify the default Uvicorn logging configuration directly,
    since we cannot override the configuration of the underlying Uvicorn server
    when creating the MCP server.
    """
    uvicorn.config.LOGGING_CONFIG["handlers"]["default"] = {
        "class": "_sail_cli_native_logging.NativeHandler",
        "formatter": "default",
    }
    uvicorn.config.LOGGING_CONFIG["handlers"]["access"] = {
        "class": "_sail_cli_native_logging.NativeHandler",
        "formatter": "default",
    }


def override_default_logging_config():
    """Override the default logging configuration."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(message)s",
        handlers=[sys.modules["_sail_cli_native_logging"].NativeHandler()],
        force=True,
    )


def configure_fastmcp_log_level():
    """Set the default log level of the MCP server to "DEBUG",
    so that all log records are emitted from Python to Rust.
    The Rust logger can then filter the records based on the Rust log level.
    """
    if "FASTMCP_LOG_LEVEL" not in os.environ:
        os.environ["FASTMCP_LOG_LEVEL"] = "DEBUG"


def create_spark_mcp_server(host: str, port: int, spark_remote: str):
    @asynccontextmanager
    async def lifespan(server: FastMCP) -> AsyncIterator[SparkSession]:  # noqa: ARG001
        logging.info("Creating Spark session")
        spark = SparkSession.builder.remote(spark_remote).getOrCreate()
        yield spark
        logging.info("Stopping Spark session")
        spark.stop()

    mcp = FastMCP("Sail MCP server for Spark SQL", lifespan=lifespan, host=host, port=port)

    @mcp.tool()
    def list_local_directories(path: str, ctx: Context) -> str:  # noqa: ARG001
        """
        List all directories under a given path in the local file system.
        This method does not list directories recursively.

        Args:
            path: The file system path.
            ctx: The context object.

        Returns:
            A JSON array of directory names.
        """
        results = [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))]
        return json.dumps(results)

    @mcp.tool()
    def list_s3_directories(bucket: str, prefix: str, region: str, ctx: Context) -> str:  # noqa: ARG001
        """
        List all directories under a given key prefix in an AWS S3 bucket.
        `/` is considered to be the directory separator.
        This method does not list directories recursively.
        It is recommended to end the key prefix with a `/`. Otherwise, partial directory names may be returned.
        Here are a few examples, assuming that there is an object `s3://abc/foo/bar/baz.txt`.
        1. `{"bucket": "abc", "prefix": ""}` returns `["foo/"]`.
        2. `{"bucket": "abc", "prefix": "foo/"}` returns `["bar/"]`.
        3. `{"bucket": "abc", "prefix": "foo/bar/"}` returns `[]`.
        4. `{"bucket": "abc", "prefix": "foo"}` returns `["/"]`.
        5. `{"bucket": "abc", "prefix": "f"}` returns `["oo/"]`.

        Args:
            bucket: The AWS S3 bucket name.
            prefix: The AWS S3 key prefix.
            region: The AWS region (e.g. "us-east-1").
            ctx: The context object.

        Returns:
            A JSON array of directory names without the key prefix.
            Please concatenate the key prefix with the directory name (without any separator) to get the full path.
        """
        try:
            import boto3
        except ImportError as e:
            message = "Please install boto3 to use AWS functionalities in the MCP server."
            raise RuntimeError(message) from e

        s3 = boto3.client("s3", region_name=region)
        paginator = s3.get_paginator("list_objects_v2")
        response = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/")
        results = []
        for item in response:
            if "CommonPrefixes" in item:
                results.extend([x["Prefix"][len(prefix) :] for x in item["CommonPrefixes"]])
        return json.dumps(results)

    @mcp.tool()
    def create_parquet_view(name: str, path: str, ctx: Context) -> str:
        """
        Create a temporary view from a Parquet dataset.

        Args:
            name: The name of the temporary view.
            path: The path to the Parquet dataset.
            ctx: The context object.

        Returns:
            An empty JSON object.
        """
        spark: SparkSession = ctx.request_context.lifespan_context
        spark.read.parquet(path).createOrReplaceTempView(name)
        return json.dumps({})

    @mcp.tool()
    def create_csv_view(name: str, path: str, header: bool, ctx: Context) -> str:  # noqa: FBT001
        """
        Create a temporary view from a CSV dataset.

        Args:
            name: The name of the temporary view.
            path: The path to the CSV dataset.
            header: Whether the CSV file has a header row.
            ctx: The context object.

        Returns:
            An empty JSON object.
        """
        spark: SparkSession = ctx.request_context.lifespan_context
        spark.read.option("header", header).csv(path).createOrReplaceTempView(name)
        return json.dumps({})

    @mcp.tool()
    def create_json_view(name: str, path: str, ctx: Context) -> str:
        """
        Create a temporary view from a JSON dataset.

        Args:
            name: The name of the temporary view.
            path: The path to the JSON dataset.
            ctx: The context object.

        Returns:
            An empty JSON object.
        """
        spark: SparkSession = ctx.request_context.lifespan_context
        spark.read.json(path).createOrReplaceTempView(name)
        return json.dumps({})

    @mcp.tool()
    def describe_view(name: str, ctx: Context) -> str:
        """
        Describe a temporary view.

        Args:
            name: The name of the temporary view.
            ctx: The context object.

        Returns:
            A JSON object with the view description.
        """
        spark: SparkSession = ctx.request_context.lifespan_context
        table = spark.catalog.getTable(name)
        if not _is_temp_view(table):
            msg = f"not a temporary view: {name}"
            raise ValueError(msg)
        columns = spark.catalog.listColumns(name)
        output = _describe_table(table)
        output["columns"] = [_describe_column(c) for c in columns]
        return json.dumps(output)

    @mcp.tool()
    def drop_view(name: str, ctx: Context) -> str:
        """
        Drop a temporary view.
        An error will be returned if the view does not exist.

        Args:
            name: The name of the temporary view.
            ctx: The context object.

        Returns:
            An empty JSON object.
        """
        spark: SparkSession = ctx.request_context.lifespan_context
        spark.catalog.dropTempView(name)
        return json.dumps({})

    @mcp.tool()
    def list_views(ctx: Context) -> str:
        """
        List all temporary views.

        Args:
            ctx: The context object.

        Returns:
            A JSON array of objects.
        """
        spark: SparkSession = ctx.request_context.lifespan_context
        views = [_describe_table(t) for t in spark.catalog.listTables() if _is_temp_view(t)]
        return json.dumps(views)

    @mcp.tool()
    def execute_query(query: str, ctx: Context) -> str:
        """
        Execute a SQL query and return the results as a JSON array.

        Args:
            query: The SQL query to execute.
            ctx: The context object.

        Returns:
            A JSON array of objects.
        """
        spark: SparkSession = ctx.request_context.lifespan_context
        df = spark.sql(query)
        return df.toPandas().to_json(orient="records")

    return mcp


def main():
    """The Spark MCP server entrypoint (for development purposes only).

    This is used when running the Python script directly during development.
    It is not used by the Sail CLI to start the MCP server.
    """
    import argparse

    parser = argparse.ArgumentParser(description="Spark MCP server")
    parser.add_argument("--transport", default="sse", help="The transport for the MCP server", choices=["stdio", "sse"])
    parser.add_argument("--host", default="127.0.0.1", help="The host for the MCP server to bind to")
    parser.add_argument("--port", default=8000, type=int, help="The port for the MCP server to listen on")
    parser.add_argument("--spark-remote", required=True, help="The Spark remote address to connect to")
    args = parser.parse_args()

    mcp = create_spark_mcp_server(args.host, args.port, args.spark_remote)
    mcp.run(args.transport)


if __name__ == "__main__":
    main()
