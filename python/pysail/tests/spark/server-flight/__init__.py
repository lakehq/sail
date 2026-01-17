"""Tests for Sail Arrow Flight SQL server integration.

These tests require a running Sail Flight SQL server and are excluded from
the default test suite. To run these tests, use:

    pytest python/pysail/tests/spark/server-flight/

Or set the SAIL_FLIGHT_URI environment variable and run:

    SAIL_FLIGHT_URI=grpc://localhost:32010 pytest python/pysail/tests/spark/server-flight/
"""

