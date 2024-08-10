from typing import Tuple

from pysail import _native

__all__ = [
    "SparkConnectServer",
]


class SparkConnectServer:
    """The Spark Connect server that uses Sail as the computation engine."""
    def __init__(self, ip: str, port: int) -> None:
        """Create a new Spark Connect server.

        :param ip: The IP address to bind the server to.
        :param port: The port to bind the server to.
        """
        self._inner = _native.spark.SparkConnectServer(ip, port)

    def init_telemetry(self) -> None:
        """Initialize OpenTelemetry for the server."""
        self._inner.init_telemetry()

    def start(self, *, background=False) -> None:
        """Start the server.

        :param background: Whether to start the server in a background thread.
        """
        self._inner.start(background=background)

    def stop(self) -> None:
        """Stop the server."""
        self._inner.stop()

    @property
    def listening_address(self) -> Tuple[str, int] | None:
        """The address that the server is listening on,
        or ``None`` if the server is not running.
        """
        return self._inner.listening_address

    @property
    def running(self) -> bool:
        """Whether the server is running."""
        return self._inner.running
