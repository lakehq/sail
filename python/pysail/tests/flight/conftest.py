"""ADBC clients connect directly to an in-process Sail Flight SQL server."""

import os
from contextlib import contextmanager

import pytest
from adbc_driver_flightsql import DatabaseOptions, dbapi


def connect(uri, **kwargs):
    return dbapi.connect(
        uri,
        autocommit=True,
        db_kwargs={DatabaseOptions.WITH_COOKIE_MIDDLEWARE.value: "true", **kwargs},
    )


@contextmanager
def local_flight_uri():
    from pysail.flight import FlightSqlServer

    server = FlightSqlServer("127.0.0.1", 0)
    server.start(background=True)
    try:
        host, port = server.listening_address
        yield f"grpc://{host}:{port}"
    finally:
        server.stop()


@pytest.fixture(scope="module")
def flight_uri():
    if "SAIL_FLIGHT_URI" in os.environ:
        yield os.environ["SAIL_FLIGHT_URI"]
        return
    with local_flight_uri() as uri:
        yield uri


@pytest.fixture
def flight_connection(flight_uri):
    with connect(flight_uri) as connection:
        yield connection
