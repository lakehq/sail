"""Configuration for Arrow Flight SQL tests."""


def pytest_configure(config):
    """Register custom markers for Flight SQL tests."""
    config.addinivalue_line(
        "markers",
        "flight_sql: mark test as requiring a running Flight SQL server",
    )
    # Suppress ADBC autocommit warnings - Flight SQL doesn't support disabling autocommit
    config.addinivalue_line(
        "filterwarnings",
        "ignore:Cannot disable autocommit:Warning",
    )
