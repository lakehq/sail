"""Configuration for Arrow Flight SQL tests."""


def pytest_configure(config):
    # Suppress ADBC autocommit warnings - Flight SQL doesn't support disabling autocommit
    config.addinivalue_line(
        "filterwarnings",
        "ignore:Cannot disable autocommit:Warning",
    )
