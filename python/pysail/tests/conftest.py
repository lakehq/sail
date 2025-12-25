def pytest_configure(config):
    """Configure pytest.

    We include the tests in the installed package so that the user can test the installation
    via `pytest --pyargs pysail`.
    We must customize the configuration here instead of using `pytest.ini` or `pyproject.toml`
    since these files are not part of the installed package.
    """

    # Note: configuration set via `config.inicfg` may not have an effect due to the cache used
    # in `config.getini()`. In such a case, we may have to clear the INI cache in `config`.
    # Since clearing the cache requires access to the private attribute of `config`, we do not
    # do it here unless absolutely necessary, to avoid compatibility issues with future versions
    # of pytest.
    # In common cases, plugins only access the configuration for the first time after this hook,
    # so the cache is not a problem.
    config.inicfg["doctest_optionflags"] = "ELLIPSIS NORMALIZE_WHITESPACE IGNORE_EXCEPTION_DETAIL"

    # Syrupy snapshots:
    # Default Syrupy format is Amber (`.ambr`), but we prefer standard YAML multi-doc files.
    default_ext = getattr(config.option, "default_extension", None)
    if default_ext is None:
        config.option.default_extension = "pysail.tests.snapshot_yaml.YamlSnapshotExtension"
