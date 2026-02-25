"""Example: multiple string UDFs in a single plugin server."""

import pyarrow as pa
import pyarrow.compute as pc
from sail_plugin import PluginServer

server = PluginServer(port=50052)


@server.udf(return_type=pa.utf8())
def upper_flight(col: pa.StringArray) -> pa.Array:
    """Convert strings to uppercase."""
    return pc.utf8_upper(col)


@server.udf(return_type=pa.utf8())
def reverse_flight(col: pa.StringArray) -> pa.Array:
    """Reverse each string."""
    return pa.array(
        [s[::-1] if s else None for s in col.to_pylist()]
    )


@server.udf(return_type=pa.int32())
def word_count(col: pa.StringArray) -> pa.Array:
    """Count words in each string."""
    return pa.array(
        [len(s.split()) if s else None for s in col.to_pylist()]
    )


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    server.serve()
