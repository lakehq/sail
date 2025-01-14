def pytest_configure(config):
    """Configure pytest.

    We include the tests in the installed package so that the user can test the installation
    via `pytest --pyargs pysail`.
    We must override the configuration here instead of using `pytest.ini` or `pyproject.toml`
    since these files are not part of the installed package.
    """
    config.inicfg["doctest_optionflags"] = "ELLIPSIS NORMALIZE_WHITESPACE IGNORE_EXCEPTION_DETAIL NUMBER"
    config._inicache.clear()  # noqa: SLF001
