"""A pytest plugin for PySpark tests.
"""
import os
import shlex
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import List

import pytest


def _is_spark_testing():
    return os.environ.get("SPARK_TESTING") == "1"


@pytest.fixture(scope="session", autouse=_is_spark_testing())
def spark_working_dir(tmp_path_factory):
    import pyspark

    working_dir = tmp_path_factory.mktemp("spark-working-dir-")

    # Copy the test support data to the working directory
    # since some tests use relative paths to access the data.
    test_support_dir = Path(pyspark.__file__).parent / "python" / "test_support"
    shutil.copytree(
        test_support_dir,
        working_dir / "python" / "test_support",
    )

    os.chdir(working_dir)
    yield


@pytest.fixture(scope="class", autouse=_is_spark_testing())
def spark_env_var(tmp_path_factory):
    """This fixture sets up the environment for each PySpark test class
    in a similar way to the `python/run-tests.py` script, so that we can
    use pytest to run the tests.
    """
    tmp_dir = tmp_path_factory.mktemp("spark-tmp-dir-")
    java_options = " ".join(
        [
            f"-Djava.io.tmpdir={shlex.quote(tmp_dir.as_posix())}",
            "-Dio.netty.tryReflectionSetAccessible=true",
            "-Xss4M",
        ]
    )
    warehouse_dir = tmp_dir / "spark-warehouse"
    spark_args = [
        "--conf",
        f"spark.driver.extraJavaOptions={shlex.quote(java_options)}",
        "--conf",
        f"spark.executor.extraJavaOptions={shlex.quote(java_options)}",
        "--conf",
        f"spark.sql.warehouse.dir={shlex.quote(warehouse_dir.as_posix())}",
        "pyspark-shell",
    ]
    os.environ["TMPDIR"] = tmp_dir.as_posix()
    os.environ["PYSPARK_SUBMIT_ARGS"] = " ".join(spark_args)
    yield


@pytest.fixture(scope="module", autouse=_is_spark_testing())
def spark_doctest_session(doctest_namespace, request):
    if request.config.option.doctestmodules:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("doctest").remote("local").getOrCreate()
        doctest_namespace["spark"] = spark
        yield
        spark.stop()
    else:
        yield


@dataclass
class TestMarker:
    keywords: List[str]
    reason: str


SKIPPED_SPARK_TESTS = [
    TestMarker(
        keywords=["test_can_create_multiple_sessions_to_different_remotes"],
        reason="Client keeps retrying setting session config for invalid remote endpoints",
    ),
    TestMarker(
        keywords=["TorchDistributorDataLoaderUnitTests", "test_data_loader"],
        reason="Flaky test",
    ),
    TestMarker(
        keywords=["SparkConnectSessionWithOptionsTest"],
        reason="Invalid runtime config keys are not passed to remote sessions",
    ),
    TestMarker(
        keywords=["test_to_local_iterator_not_fully_consumed"],
        reason="Flaky test",
    ),
    TestMarker(
        keywords=["test_to_local_iterator"],
        reason="Flaky test",
    ),
    TestMarker(
        keywords=["test_to_local_iterator_prefetch"],
        reason="Flaky test",
    ),
    TestMarker(
        keywords=["SparkSessionTestCase", "test_session.py"],
        reason="Client keeps retrying setting session config for invalid remote endpoints",
    ),
    TestMarker(
        keywords=["SparkInstallationTestCase", "test_install_spark"],
        reason="Expensive test",
    ),
    TestMarker(
        keywords=["test_python_segfault", "test_worker.py"],
        reason="SPARK-46130: Flaky with Python 3.12",
    ),
]


def pytest_collection_modifyitems(
    session: pytest.Session, config: pytest.Config, items: List[pytest.Item]
) -> None:
    if not _is_spark_testing():
        return

    for item in items:
        for test in SKIPPED_SPARK_TESTS:
            if all(k in item.keywords for k in test.keywords):
                item.add_marker(pytest.mark.skip(reason=test.reason))
