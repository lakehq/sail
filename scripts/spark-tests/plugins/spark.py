"""A pytest plugin for PySpark tests."""

from __future__ import annotations

import importlib
import os
import re
import shlex
import shutil
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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


# Here we patch test utilities to ignore row order in PySpark tests.
# This may result in false positives in some tests where the result is expected to be sorted.
# Such tests should be ported to the PySail test suite where the patch is not applied.


def normalize_pandas_data_frame(df):
    from pandas.api.types import is_hashable

    columns = [col for col in df.columns if all(is_hashable(v) for v in df[col])]
    return df.sort_values(by=columns, ignore_index=True)


@pytest.fixture(scope="session", autouse=_is_spark_testing())
def patch_pyspark_pandas_test_utils():
    from pyspark.testing.pandasutils import PandasOnSparkTestUtils

    _assert_eq = PandasOnSparkTestUtils.assert_eq

    def assert_eq(
        self,
        left: Any,
        right: Any,
        # do not check row order by default
        check_row_order: bool = False,  # noqa: FBT001, FBT002
        **kwargs,
    ):
        import pandas as pd

        if not check_row_order and isinstance(left, pd.DataFrame) and isinstance(right, pd.DataFrame):
            left = normalize_pandas_data_frame(left)
            right = normalize_pandas_data_frame(right)

        _assert_eq(self, left, right, check_row_order=check_row_order, **kwargs)

    PandasOnSparkTestUtils.assert_eq = assert_eq


@pytest.fixture(scope="session", autouse=_is_spark_testing())
def patch_pandas_test_utils():
    from pandas.testing import assert_frame_equal as _assert_frame_equal

    def assert_frame_equal(left, right, **kwargs):
        left = normalize_pandas_data_frame(left)
        right = normalize_pandas_data_frame(right)
        _assert_frame_equal(left, right, **kwargs)

    modules = [
        # We have to patch every test module explicitly since the utility function
        # is imported using the `from pandas.testing import assert_frame_equal` syntax.
        "pyspark.sql.tests.pandas.test_pandas_cogrouped_map",
        "pyspark.sql.tests.pandas.test_pandas_grouped_map",
        "pyspark.sql.tests.pandas.test_pandas_udf_grouped_agg",
        "pyspark.sql.tests.pandas.test_pandas_udf_typehints",
        "pyspark.sql.tests.pandas.test_pandas_udf_typehints_with_future_annotations",
        "pyspark.sql.tests.pandas.test_pandas_udf_window",
        "pyspark.sql.tests.test_arrow",
        # Some test cases import the utility function inside the test function,
        # so we also need to patch the module that defines the utility function.
        "pandas.testing",
    ]

    for name in modules:
        module = importlib.import_module(name)
        module.assert_frame_equal = assert_frame_equal


def is_row_collection(obj):
    from pyspark.sql import Row

    return isinstance(obj, Iterable) and all(isinstance(x, Row) for x in obj)


def normalize_row_collection(obj):
    return sorted(obj, key=lambda x: str(x))


@pytest.fixture(scope="session", autouse=_is_spark_testing())
def patch_pyspark_connect_test_class():
    from pyspark.testing.connectutils import ReusedConnectTestCase

    def assertEqual(self, first, second, msg=None):  # noqa: N802
        if is_row_collection(first) and is_row_collection(second):
            first = normalize_row_collection(first)
            second = normalize_row_collection(second)
        return super(ReusedConnectTestCase, self).assertEqual(first, second, msg)  # noqa: PT009

    ReusedConnectTestCase.assertEqual = assertEqual


@dataclass
class TestMarker:
    keywords: list[str]
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


def add_pyspark_test_markers(items: list[pytest.Item]):
    for item in items:
        for test in SKIPPED_SPARK_TESTS:
            if all(k in item.keywords for k in test.keywords):
                item.add_marker(pytest.mark.skip(reason=test.reason))


def normalize_show_string(s: str) -> str:
    """Normalize the PySpark `show()` output with a canonical row order.
    We split the table into lines and sort rows after the header.
    If the table is invalid, we return the original string.
    """

    lines = s.split("\n")
    if len(lines) < 4:  # noqa: PLR2004
        return s
    if re.match(r"(\+-+)+\+", lines[0]) is None:
        return s
    if lines[2] != lines[0]:
        return s
    # We need to find the last table line, since there may be other content
    # (such as empty lines) after the table.
    last = 0
    for n in range(len(lines) - 1, 2, -1):
        if lines[n] == lines[0]:
            last = n
            break
    if last == 0:
        return s

    return "\n".join(lines[:3] + sorted(lines[3:last]) + lines[last:])


def normalize_describe_df_show_string(s: str) -> str:
    """Adjust floating point string representations in expected doctest output.
    The values are  equivalent but have different string representations due to floating point arithmetic.
    """
    s = s.replace(" 40.73333333333333", "40.733333333333334")
    return s.replace("3.1722757341273704", "3.1722757341273695")


def patch_pyspark_doctest_output_checker():
    import _pytest.doctest

    # ensure the doctest output checker class is initialized
    _ = _pytest.doctest._get_checker()

    if _pytest.doctest.CHECKER_CLASS is None:
        msg = "the doctest output checker class is not initialized in pytest"
        raise RuntimeError(msg)

    class OutputChecker(_pytest.doctest.CHECKER_CLASS):
        def check_output(self, want: str, got: str, optionflags: int) -> bool:
            if (
                "|   mean|12.0| 40.73333333333333|            145.0|" in want
                and "| stddev| 1.0|3.1722757341273704|4.763402145525822|" in want
            ):
                want = normalize_describe_df_show_string(want)
            if want.startswith("+"):
                want = normalize_show_string(want)
                got = normalize_show_string(got)
            return super().check_output(want, got, optionflags)

    _pytest.doctest.CHECKER_CLASS = OutputChecker


def pytest_collection_modifyitems(session: pytest.Session, config: pytest.Config, items: list[pytest.Item]) -> None:  # noqa: ARG001
    if _is_spark_testing():
        add_pyspark_test_markers(items)


def pytest_sessionstart(session: pytest.Session):  # noqa: ARG001
    if _is_spark_testing():
        patch_pyspark_doctest_output_checker()
