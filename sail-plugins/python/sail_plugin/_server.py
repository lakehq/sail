"""Sail plugin SDK — write UDFs in Python with a simple decorator."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa
from pyarrow import flight

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

_MIN_DESCRIPTOR_PATH_LEN = 2


@dataclass
class _UdfEntry:
    name: str
    func: Callable
    return_type: pa.DataType
    volatility: str
    input_types: list[pa.DataType] | None


def udf(
    return_type: pa.DataType,
    *,
    volatility: str = "immutable",
    input_types: list[pa.DataType] | None = None,
    name: str | None = None,
):
    """Decorator to register a function as a Sail UDF.

    Args:
        return_type: Arrow data type of the result column.
        volatility: One of "immutable", "stable", or "volatile".
        input_types: Optional list of expected Arrow input types (for documentation).
        name: Override the function name (defaults to the Python function name).

    Example::

        @udf(return_type=pa.utf8())
        def my_upper(col: pa.StringArray) -> pa.Array:
            return pa.array([s.upper() if s else None for s in col.to_pylist()])
    """

    def decorator(func: Callable) -> Callable:
        func._sail_udf = _UdfEntry(  # noqa: SLF001
            name=name or func.__name__,
            func=func,
            return_type=return_type,
            volatility=volatility,
            input_types=input_types,
        )
        return func

    return decorator


@dataclass
class PluginServer:
    """Arrow Flight server that exposes decorated UDFs to Sail.

    Args:
        port: Port to listen on (default 50052).
        host: Bind address (default "0.0.0.0").

    Example::

        server = PluginServer(port=50052)

        @server.udf(return_type=pa.utf8())
        def my_upper(col):
            return pa.array([s.upper() if s else None for s in col.to_pylist()])

        server.serve()
    """

    port: int = 50052
    host: str = "0.0.0.0"  # noqa: S104
    _udfs: dict[str, _UdfEntry] = field(default_factory=dict, init=False)

    def udf(
        self,
        return_type: pa.DataType,
        *,
        volatility: str = "immutable",
        input_types: list[pa.DataType] | None = None,
        name: str | None = None,
    ):
        """Register a UDF on this server. Same args as the module-level ``@udf``."""

        def decorator(func: Callable) -> Callable:
            udf_name = name or func.__name__
            self._udfs[udf_name] = _UdfEntry(
                name=udf_name,
                func=func,
                return_type=return_type,
                volatility=volatility,
                input_types=input_types,
            )
            return func

        return decorator

    def register(self, func: Callable) -> None:
        """Register a function decorated with the module-level ``@udf``."""
        entry = getattr(func, "_sail_udf", None)
        if entry is None:
            msg = f"{func.__name__} is not decorated with @udf"
            raise ValueError(msg)
        self._udfs[entry.name] = entry

    def serve(self) -> None:
        """Start the Flight server (blocking)."""
        location = f"grpc://{self.host}:{self.port}"
        flight_server = _FlightHandler(location=location, udfs=self._udfs)
        logger.info("Sail plugin server listening on %s", location)
        for name in self._udfs:
            logger.info("  UDF: %s", name)
        flight_server.serve()


class _FlightHandler(flight.FlightServerBase):
    """Internal Flight server that routes requests to registered UDFs."""

    def __init__(self, *, location: str, udfs: dict[str, _UdfEntry]):
        super().__init__(location=location)
        self._udfs = udfs

    def list_actions(self, _context):
        for entry in self._udfs.values():
            yield flight.ActionType(
                f"udf.{entry.name}",
                json.dumps({"volatility": entry.volatility}),
            )

    def do_action(self, _context, action):
        if action.type.startswith("return_type."):
            udf_name = action.type[len("return_type."):]
            entry = self._udfs.get(udf_name)
            if entry is None:
                msg = f"Unknown UDF: {udf_name}"
                raise flight.FlightUnimplementedError(msg)
            schema = pa.schema([pa.field("result", entry.return_type)])
            sink = pa.BufferOutputStream()
            writer = pa.ipc.new_stream(sink, schema)
            writer.close()
            yield flight.Result(sink.getvalue())
        elif action.type == "ping":
            return
        else:
            msg = f"Unknown action: {action.type}"
            raise flight.FlightUnimplementedError(msg)

    def do_exchange(self, _context, descriptor, reader, writer):
        if not descriptor.path or len(descriptor.path) < _MIN_DESCRIPTOR_PATH_LEN:
            msg = "Expected descriptor path: ['udf', '<name>']"
            raise flight.FlightServerError(msg)
        udf_name = descriptor.path[1].decode("utf-8") if isinstance(
            descriptor.path[1], bytes
        ) else descriptor.path[1]

        entry = self._udfs.get(udf_name)
        if entry is None:
            msg = f"Unknown UDF: {udf_name}"
            raise flight.FlightUnimplementedError(msg)

        table = reader.read_all()
        columns = [table.column(i) for i in range(table.num_columns)]
        result = entry.func(*columns)

        if not isinstance(result, (pa.Array, pa.ChunkedArray)):
            result = pa.array(result)

        # Cast to the promised return type if needed (e.g. int64 -> int32).
        if result.type != entry.return_type:
            result = result.cast(entry.return_type)

        result_table = pa.table({"result": result})
        writer.begin(result_table.schema)
        writer.write_table(result_table)
