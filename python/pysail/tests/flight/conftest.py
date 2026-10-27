"""Configuration for Arrow Flight SQL tests."""

import pytest


def pytest_configure(config: pytest.Config) -> None:
    # Suppress ADBC autocommit warnings - Flight SQL doesn't support disabling autocommit
    config.addinivalue_line(
        "filterwarnings",
        "ignore:Cannot disable autocommit:Warning",
    )
