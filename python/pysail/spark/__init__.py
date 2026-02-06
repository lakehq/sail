from __future__ import annotations

from pysail import _native

__all__ = [
    "SparkConnectServer",
]


class SparkConnectServer:
    """The Spark Connect server that uses Sail as the computation engine."""

    def __init__(self, ip: str = "127.0.0.1", port: int = 0) -> None:
        """Create a new Spark Connect server.
        By default, the server will bind to localhost on a random port.

        :param ip: The IP address to bind the server to.
        :param port: The port to bind the server to.
        """
        self._inner = _native.spark.SparkConnectServer(ip, port)

    def start(self, *, background=True) -> None:
        """Start the server.

        :param background: Whether to start the server in a background thread.
        """
        self._inner.start(background=background)

    def stop(self) -> None:
        """Stop the server."""
        self._inner.stop()

    @property
    def listening_address(self) -> tuple[str, int] | None:
        """The address that the server is listening on,
        or ``None`` if the server is not running.
        The address is a tuple of the IP address and port.
        """
        return self._inner.listening_address

    @property
    def running(self) -> bool:
        """Whether the server is running."""
        return self._inner.running
