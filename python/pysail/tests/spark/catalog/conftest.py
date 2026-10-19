import pytest

from pysail.testing.spark.session import spark_connect_server


@pytest.fixture(scope="package")
def remote():
    # The catalog tests rely on the default memory catalog of a particular name.
    # We explicitly configure it so that the tests are skipped when `SPARK_REMOTE` is set,
    # since the remote server started outside the test suite may have incompatible catalog configuration.
    with spark_connect_server(
        envs={
            "SAIL_CATALOG__DEFAULT_CATALOG": "",
            "SAIL_CATALOG__DEFAULT_DATABASE": '["default"]',
            "SAIL_CATALOG__LIST": '[{name="sail", type="memory", initial_database=["default"], initial_database_comment="default database"}]',
        }
    ) as server:
        yield server.remote
