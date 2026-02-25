"""Minimal Arrow Flight plugin server that provides an `upper_flight` UDF.

Usage:
    pip install pyarrow
    python plugin.py

Then start Sail with:
    cargo run -p sail-cli -- spark server --plugin-endpoint grpc://localhost:50052

Test with PySpark:
    spark.sql("SELECT upper_flight('hello world')").show()
"""

import json
import logging

import pyarrow as pa
from pyarrow import flight

logger = logging.getLogger(__name__)


class UpperPluginServer(flight.FlightServerBase):
    """A Flight plugin server that provides a string uppercase UDF."""

    def list_actions(self, _context):
        """Advertise available UDFs to Sail."""
        yield flight.ActionType(
            "udf.upper_flight",
            json.dumps({"volatility": "immutable"}),
        )

    def do_action(self, _context, action):
        """Handle return_type and ping requests."""
        if action.type.startswith("return_type."):
            schema = pa.schema([pa.field("result", pa.utf8())])
            sink = pa.BufferOutputStream()
            writer = pa.ipc.new_stream(sink, schema)
            writer.close()
            yield flight.Result(sink.getvalue())
        elif action.type == "ping":
            return
        else:
            msg = f"Unknown action: {action.type}"
            raise flight.FlightUnimplementedError(msg)

    def do_exchange(self, _context, _descriptor, reader, writer):
        """Execute the UDF: convert the first column to uppercase."""
        table = reader.read_all()
        col = table.column(0)
        result = pa.array(
            [
                s.as_py().upper() if s.as_py() is not None else None
                for s in col
            ]
        )
        result_table = pa.table({"result": result})
        writer.begin(result_table.schema)
        writer.write_table(result_table)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    server = UpperPluginServer(location="grpc://0.0.0.0:50052")  # noqa: S104
    logger.info("Flight plugin server listening on grpc://0.0.0.0:50052")
    logger.info("Providing UDF: upper_flight(string) -> string")
    server.serve()
