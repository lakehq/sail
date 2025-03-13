from contextlib import asynccontextmanager
from typing import AsyncIterator, Literal

from mcp.server.fastmcp import Context, FastMCP
from pyspark.sql import SparkSession


def run_spark_mcp_server(transport: Literal["stdio", "sse"], host: str, port: int, spark_port: int):
    @asynccontextmanager
    async def spark_lifespan(server: FastMCP) -> AsyncIterator[SparkSession]:  # noqa: ARG001
        spark = SparkSession.builder.remote(f"sc://127.0.0.1:{spark_port}").getOrCreate()
        yield spark
        spark.stop()

    mcp = FastMCP("PySpark MCP server powered by Sail", lifespan=spark_lifespan, host=host, port=port)

    @mcp.tool()
    def create_view(name: str, format: str, path: str, ctx: Context):  # noqa: A002
        spark: SparkSession = ctx.request_context.lifespan_context
        spark.read.format(format).load(path).createOrReplaceTempView(name)

    @mcp.tool()
    def describe_view(name: str) -> str:
        _ = name
        return ""

    @mcp.tool()
    def drop_view(name: str):
        _ = name

    @mcp.tool()
    def list_views() -> str:
        return ""

    @mcp.tool()
    async def execute_query(query: str, limit: int, ctx: Context) -> str:
        spark: SparkSession = ctx.request_context.lifespan_context
        df = spark.sql(query).limit(limit)
        return df.toPandas().to_markdown()

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
