from pysail import _native


class FlightSqlServer:
    """The Arrow Flight SQL server that uses Sail as the computation engine."""

    def __init__(self, ip: str = "127.0.0.1", port: int = 0) -> None:
        self._inner = _native.flight.FlightSqlServer(ip, port)

    def start(self, *, background=True) -> None:
        self._inner.start(background=background)

    def stop(self) -> None:
        self._inner.stop()

    @property
    def listening_address(self) -> tuple[str, int] | None:
        return self._inner.listening_address

    @property
    def running(self) -> bool:
        return self._inner.running
