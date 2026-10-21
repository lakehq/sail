"""ADBC clients connect directly to an in-process Sail Flight SQL server."""

import os

import pytest
from adbc_driver_flightsql import DatabaseOptions, dbapi


def connect(uri, **kwargs):
    return dbapi.connect(
        uri,
        autocommit=True,
        db_kwargs={DatabaseOptions.WITH_COOKIE_MIDDLEWARE.value: "true", **kwargs},
    )


@pytest.fixture(scope="module")
def flight_uri():
    from pysail.flight import FlightSqlServer

    if "SAIL_FLIGHT_URI" in os.environ:
        yield os.environ["SAIL_FLIGHT_URI"]
        return

    server = FlightSqlServer("127.0.0.1", 0)
    server.start(background=True)
    host, port = server.listening_address
    yield f"grpc://{host}:{port}"
    server.stop()


@pytest.fixture
def flight_connection(flight_uri):
    with connect(flight_uri) as connection:
        yield connection
