import json
from contextlib import asynccontextmanager
from typing import AsyncIterator, Literal

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


def run_spark_mcp_server(transport: Literal["stdio", "sse"], host: str, port: int, spark_port: int):
    @asynccontextmanager
    async def spark_lifespan(server: FastMCP) -> AsyncIterator[SparkSession]:  # noqa: ARG001
        spark = SparkSession.builder.remote(f"sc://127.0.0.1:{spark_port}").getOrCreate()
        yield spark
        spark.stop()

    mcp = FastMCP("PySpark MCP server powered by Sail", lifespan=spark_lifespan, host=host, port=port)

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
    def execute_query(query: str, limit: int, ctx: Context) -> str:
        """
        Execute a SQL query and return the results as a JSON array.

        Args:
            query: The SQL query to execute.
            limit: The maximum number of rows to return.
            ctx: The context object.
        Returns:
            A JSON array of objects.
        """
        spark: SparkSession = ctx.request_context.lifespan_context
        df = spark.sql(query).limit(limit)
        return df.toPandas().to_json(orient="records")

    mcp.run(transport)


def main():
    """The Spark MCP server entrypoint (for development purposes only).

    This is used when running the Python script directly during development.
    It is not used by the Sail CLI to start the MCP server.
    """
    import argparse

    from pysail.spark import SparkConnectServer

    parser = argparse.ArgumentParser(description="Spark MCP server")
    parser.add_argument("--transport", default="sse", help="The transport for the MCP server", choices=["stdio", "sse"])
    parser.add_argument("--host", default="127.0.0.1", help="The host for the MCP server to bind to")
    parser.add_argument("--port", default=8000, type=int, help="The port for the MCP server to listen on")
    args = parser.parse_args()

    server = SparkConnectServer()
    server.start()
    server.init_telemetry()
    _, port = server.listening_address

    run_spark_mcp_server(args.transport, args.host, args.port, port)


if __name__ == "__main__":
    main()
