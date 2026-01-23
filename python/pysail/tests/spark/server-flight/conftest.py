"""Configuration for Arrow Flight SQL tests."""

import pytest


def pytest_configure(config):
    """Register custom markers for Flight SQL tests."""
    config.addinivalue_line(
        "markers",
        "flight_sql: mark test as requiring a running Flight SQL server",
    )
