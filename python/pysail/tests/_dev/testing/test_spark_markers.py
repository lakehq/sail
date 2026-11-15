"""Test support resolution and the collection order without starting Spark."""

import pytest
from packaging.specifiers import SpecifierSet

pytest_plugins = ["pytester", "pysail.testing.spark.markers"]


def test_resolve_spark_markers():
    from pysail.testing.spark.markers import SparkSupport, resolve_spark_markers

    markers = [
        pytest.mark.spark(version=">=4", cross_version=True, reason="UDF protocol").mark,
        pytest.mark.spark(version=">=4.1,<4.2", skip_if_jvm=True, reason="Sail extension").mark,
        pytest.mark.spark(version="!=4.1.1", reason="UDF protocol").mark,
    ]
    assert resolve_spark_markers(markers) == [
        SparkSupport(version=SpecifierSet(">=4"), cross_version=True, reason="UDF protocol"),
        SparkSupport(version=SpecifierSet(">=4.1,<4.2"), skip_if_jvm=True, reason="Sail extension"),
        SparkSupport(version=SpecifierSet("!=4.1.1"), reason="UDF protocol"),
    ]
    assert resolve_spark_markers([]) == []


@pytest.mark.parametrize("version", ["4", "4.", ">=4..1", ">=-1", ">=4.x", "<4.2 || >=5"])
def test_invalid_spark_version_specifier(version):
    from pysail.testing.spark.markers import resolve_spark_markers

    with pytest.raises(ValueError, match="Invalid specifier"):
        resolve_spark_markers([pytest.mark.spark(version=version).mark])


def test_unrestricted_spark_version():
    from pysail.testing.spark.markers import SparkSupport, resolve_spark_markers

    assert resolve_spark_markers([pytest.mark.spark(version="").mark]) == [SparkSupport()]


@pytest.mark.parametrize("flag", ["cross_version", "skip_if_jvm"])
def test_conflicting_spark_markers(flag):
    from pysail.testing.spark.markers import resolve_spark_markers

    with pytest.raises(ValueError, match=f"conflicting spark marker values for {flag}"):
        resolve_spark_markers([pytest.mark.spark(**{flag: True}).mark, pytest.mark.spark(**{flag: False}).mark])


@pytest.mark.parametrize(
    ("args", "kwargs"),
    [
        (("4",), {}),
        ((), {"unknown": True}),
        ((), {"since": "4.1"}),
        ((), {"version": 4}),
        ((), {"version": None}),
        ((), {"cross_version": "yes"}),
        ((), {"skip_if_jvm": 1}),
        ((), {"reason": 1}),
    ],
)
def test_invalid_spark_marker_arguments(args, kwargs):
    from pysail.testing.spark.markers import resolve_spark_markers

    with pytest.raises((TypeError, ValueError)):
        resolve_spark_markers([pytest.mark.spark(*args, **kwargs).mark])


@pytest.mark.parametrize(
    ("version", "jvm", "passed", "skipped"),
    [
        ("3.5.9", False, 4, 4),
        ("4.0.5", False, 5, 3),
        ("4.1.0", False, 5, 3),
        ("4.1.0", True, 4, 4),
        ("4.2.1", False, 4, 4),
    ],
)
def test_spark_collection(pytester, version, jvm, passed, skipped):
    pytester.makeconftest(
        f"""
        pytest_plugins = ["pysail.testing.spark.markers"]

        def pytest_configure(config):
            from pysail.testing.spark import markers
            markers.pyspark_version = lambda: tuple(map(int, {version!r}.split(".")))
            markers.is_jvm_spark = lambda: {jvm!r}
        """
    )
    pytester.makepyfile(
        """
        import pytest

        pytestmark = pytest.mark.spark(version=">=3", cross_version=True)

        @pytest.mark.spark(version=">=4")
        @pytest.mark.spark(version=">=4.1")
        def test_minimum_version():
            pass

        @pytest.mark.spark(skip_if_jvm=True, reason="Sail extension")
        def test_sail_only():
            pass

        @pytest.mark.parametrize("value", [
            1,
            pytest.param(2, marks=pytest.mark.spark(version=">=99", skip_if_jvm=True)),
        ])
        def test_parameter(value):
            pass

        @pytest.mark.spark(version="<4.1")
        def test_upper_bound():
            pass

        @pytest.mark.spark(version=">=4")
        @pytest.mark.spark(version="<4.2")
        def test_range():
            pass

        @pytest.mark.spark(version="!=4.1.0")
        def test_excluded_version():
            pass

        @pytest.mark.spark(version="==4.1.*")
        def test_wildcard():
            pass
        """
    )
    result = pytester.runpytest_subprocess("--strict-markers", "-rs")
    result.assert_outcomes(passed=passed, skipped=skipped)
    result.stdout.fnmatch_lines(["*Requires PySpark *>=99*"])
    if jvm:
        result.stdout.fnmatch_lines(["*SKIPPED* Sail extension"])


@pytest.mark.parametrize(
    ("version", "jvm", "reason"),
    [
        ((3, 5, 9), False, "New client API"),
        ((4, 2, 0), True, "Sail-only API"),
        ((4, 2, 0), False, None),
    ],
)
def test_spark_skip_reasons_are_separate(pytester, version, jvm, reason):
    pytester.makeconftest(
        f"""
        import pytest

        pytest_plugins = ["pysail.testing.spark.markers"]

        def pytest_configure(config):
            from pysail.testing.spark import markers
            markers.pyspark_version = lambda: {version!r}
            markers.is_jvm_spark = lambda: {jvm!r}

        @pytest.hookimpl(wrapper=True)
        def pytest_collection_modifyitems(items):
            yield
            for item in items:
                reasons = [mark.kwargs["reason"] for mark in item.iter_markers("skipif")]
                assert sorted(reasons) == ["New client API", "Sail-only API", "Supported client API"]
        """
    )
    pytester.makepyfile(
        """
        import pytest

        pytestmark = pytest.mark.spark(version=">=3", reason="Supported client API")

        @pytest.mark.spark(version=">=4", reason="New client API")
        @pytest.mark.spark(skip_if_jvm=True, reason="Sail-only API")
        def test_support():
            pass
        """
    )
    result = pytester.runpytest_subprocess("--strict-markers", "-rs")
    if reason is None:
        result.assert_outcomes(passed=1)
    else:
        result.assert_outcomes(skipped=1)
        result.stdout.fnmatch_lines([f"SKIPPED *: {reason}"])


@pytest.mark.parametrize(
    ("marker", "error"),
    [
        (
            "@pytest.mark.spark(cross_version=True)\n@pytest.mark.spark(cross_version=False)",
            "conflicting spark marker values for cross_version",
        ),
        ('@pytest.mark.spark(version="4.1")', "Invalid specifier*4.1*"),
    ],
)
def test_spark_marker_collection_error(pytester, marker, error):
    pytester.makeconftest('pytest_plugins = ["pysail.testing.spark.markers"]')
    pytester.makepyfile(f"import pytest\n{marker}\ndef test_invalid():\n    pass\n")
    result = pytester.runpytest_subprocess()
    assert result.ret == pytest.ExitCode.USAGE_ERROR
    result.stderr.fnmatch_lines([f"*test_invalid: {error}*"])


def test_bdd_and_doctest_spark_markers(pytester):
    pytester.makeconftest(
        """
        from pysail.tests._dev.spark.conftest import pytest_bdd_apply_tag

        pytest_plugins = ["pysail.testing.spark.markers", "pysail.testing.spark.doctest"]

        def pytest_configure(config):
            from pysail.testing.spark import markers
            markers.pyspark_version = lambda: (3, 5, 9)
            markers.is_jvm_spark = lambda: True
        """
    )
    pytester.makefile(
        ".feature",
        support="""
        Feature: Spark support
          @spark-4.1
          Scenario: New Spark API
            Then this must be skipped

          @sail-only
          Scenario: Sail extension
            Then this must be skipped
        """,
    )
    pytester.makepyfile(
        """
        from pytest_bdd import scenarios, then

        scenarios("support.feature")

        @then("this must be skipped")
        def fail():
            raise AssertionError("test was not skipped")
        """
    )
    pytester.makefile(".txt", test_arrow_scalar_udf=">>> raise AssertionError('doctest was not skipped')\n")
    result = pytester.runpytest_subprocess("--doctest-glob=*.txt", "--strict-markers", "-rs")
    result.assert_outcomes(skipped=3)
    result.stdout.fnmatch_lines(["*Uses arrow_udf*", "*Requires PySpark >=4.1*", "*Tests a Sail-only feature*"])
