import pytest


@pytest.fixture(scope="module", autouse=True)
def spark_doctest(doctest_namespace):
    del doctest_namespace


# This is a placeholder so that skipped catalog integration tests
# does not cause pytest to exit with code 5 (no tests collected)
# when running the workflow in CI.
# FIXME: Remove this test once catalog integration tests are no longer skipped.
@pytest.mark.catalog_integration
def test_catalog_integration_placeholder():
    pass
