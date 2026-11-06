"""Exercise suite inference and standard pytest marker selection."""

import json
from pathlib import Path

import pytest

from pysail.testing import suites

pytest_plugins = ["pytester"]


@pytest.fixture
def suite_tests(pytester, monkeypatch):
    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    for name in (
        "PYSAIL_TEST_SUITE_MANIFEST",
        "PYSAIL_TEST_SEED",
        "PYSAIL_TEST_SPARK_VERSIONS",
        "PYSAIL_TEST_SUITE_CHOICES",
    ):
        monkeypatch.delenv(name, raising=False)
    pytester.makeini("[pytest]\nmarkers =\n    function(group)")
    pytester.makeconftest('pytest_plugins = ["pysail.testing.suites", "pysail.testing.spark.markers"]')
    monkeypatch.setattr(suites, "relative_test_path", lambda path: path.relative_to(pytester.path))
    pytester.makepyfile(
        test_cases="""
        import pytest

        def test_default(): pass

        @pytest.mark.function(group="nullability")
        def test_function(): pass

        @pytest.mark.integration(group="hms")
        @pytest.mark.function(group="columnargs")
        def test_integration(): pass
        """
    )
    udf = pytester.path / "spark" / "udf"
    udf.mkdir(parents=True)
    (udf / "test_udf.py").write_text("def test_udf(): pass\n")
    return pytester


def test_suite_inventory_and_default_integration_execution(suite_tests, monkeypatch):
    manifest = suite_tests.path / "suites.json"
    monkeypatch.setenv("PYSAIL_TEST_SUITE_MANIFEST", str(manifest))
    result = suite_tests.runpytest()
    result.assert_outcomes(passed=4)
    assert json.loads(manifest.read_text()) == [
        {"name": "default", "test_count": 1},
        {"name": "function-nullability", "test_count": 1},
        {"name": "integration-hms", "test_count": 1},
        {"name": "udf", "test_count": 1},
    ]


def test_standard_suite_selection(suite_tests):
    result = suite_tests.runpytest("-m", "suite(name='udf') or suite(name='function-nullability')")
    result.assert_outcomes(passed=2, deselected=2)


def test_suite_selection_combines_with_integration_marker(suite_tests):
    result = suite_tests.runpytest("-m", "suite(name='integration-hms') and integration")
    result.assert_outcomes(passed=1, deselected=3)


def test_suite_selection_combines_with_keyword(suite_tests):
    result = suite_tests.runpytest("-m", "suite(name='udf') or suite(name='default')", "-k", "udf")
    result.assert_outcomes(passed=1, deselected=3)


def test_unknown_suite_runs_nothing(suite_tests):
    result = suite_tests.runpytest("-m", "suite(name='missing')")
    result.assert_outcomes(deselected=4)
    assert result.ret == pytest.ExitCode.NO_TESTS_COLLECTED


def test_inferred_integration_marker_is_available_to_pytest_filter(suite_tests):
    catalog = suite_tests.path / "catalog" / "glue"
    catalog.mkdir(parents=True)
    (catalog / "test_catalog.py").write_text("def test_catalog(): pass\n")
    result = suite_tests.runpytest("-m", "suite(name='integration-glue') and integration")
    result.assert_outcomes(passed=1, deselected=4)


@pytest.mark.parametrize(
    ("path", "group"),
    [
        ("catalog/hms/test_catalog.py", "integration-hms"),
        ("spark/catalog/unity/test_catalog.py", "integration-unity"),
        ("catalog/iceberg/test_catalog.py", "integration-iceberg-rest"),
        ("spark/catalog/glue/test_catalog.py", "integration-glue"),
        ("celeborn/test_shuffle.py", "integration-celeborn"),
        ("spark/dataframe/test_data.py", "dataframe"),
        ("spark/delta/test_delta.py", "delta"),
        ("spark/iceberg/test_read.py", "iceberg"),
        ("spark/datasource/test_read.py", "datasource"),
        ("spark/function/test_features.py", "function"),
        ("spark/function/test_lazy_python_udf.py", "udf"),
        ("spark/execution/test_execution.py", "distributed"),
        ("spark/test_tpch.py", "benchmark"),
        ("spark/test_tpcds.py", "benchmark"),
        ("spark/test_clickbench.py", "benchmark"),
        ("utils/test_proxy.py", "default"),
    ],
)
def test_path_inference(suite_tests, path, group):
    item = suite_tests.getitems("def test_example(): pass")[0]
    assert suites.infer_suite(item, Path(path)) == group


def test_explicit_integration_group_is_preserved(suite_tests):
    item = suite_tests.getitems("def test_example(): pass")[0]
    item.add_marker(pytest.mark.integration(group="custom"))
    assert suites.infer_suite(item, Path("catalog/hms/test_catalog.py")) == "integration-custom"


def test_jdbc_integration_has_its_own_suite(suite_tests):
    item = suite_tests.getitems("def test_example(): pass")[0]
    item.add_marker(pytest.mark.integration)
    assert suites.infer_suite(item, Path("spark/datasource/test_jdbc.py")) == "integration-jdbc"
    assert suites.infer_suite(item, Path("spark/catalog/test_table.py")) == "integration-other"


@pytest.mark.parametrize("group", [None, "nullability", "columnargs", "lambda", "sketch"])
def test_function_groups_override_the_function_directory(suite_tests, group):
    item = suite_tests.getitems("def test_example(): pass")[0]
    item.add_marker(pytest.mark.function if group is None else pytest.mark.function(group=group))
    assert suites.infer_suite(item, Path("spark/function/test_features.py")) == (
        f"function-{group}" if group else "function"
    )


def test_explicit_suite_is_preserved(suite_tests):
    test_file = suite_tests.makepyfile(
        test_custom="""
        import pytest
        @pytest.mark.suite(name="custom")
        def test_custom(): pass
    """
    )
    delta = suite_tests.path / "spark" / "delta"
    delta.mkdir(parents=True)
    test_file.rename(delta / test_file.name)
    result = suite_tests.runpytest("-m", "suite(name='custom')")
    result.assert_outcomes(passed=1, deselected=4)


@pytest.mark.parametrize(
    "source",
    [
        """
        import pytest
        pytestmark = pytest.mark.suite(name="delta")
        @pytest.mark.suite(name="slow")
        def test_conflict(): pass
        """,
        """
        import pytest
        class TestSuites:
            pytestmark = pytest.mark.suite(name="delta")
            @pytest.mark.suite(name="slow")
            def test_conflict(self): pass
        """,
        """
        import pytest
        @pytest.mark.suite(name="delta")
        @pytest.mark.suite(name="slow")
        def test_conflict(): pass
        """,
        """
        import pytest
        @pytest.mark.suite(name="delta")
        @pytest.mark.parametrize("value", [pytest.param(1, marks=pytest.mark.suite(name="slow"))])
        def test_conflict(value): pass
        """,
    ],
    ids=["module", "class", "function", "parameter"],
)
def test_conflicting_suite_markers_are_rejected(suite_tests, source):
    suite_tests.makepyfile(test_conflict=source)
    result = suite_tests.runpytest("-m", "suite(name='slow')")
    assert result.ret == pytest.ExitCode.USAGE_ERROR
    result.stderr.fnmatch_lines(["*test_conflict.py::*: conflicting suite markers: delta, slow*"])


def test_repeated_identical_suite_markers_are_allowed(suite_tests):
    suite_tests.makepyfile(
        test_repeated="""
        import pytest
        pytestmark = pytest.mark.suite(name="slow")
        @pytest.mark.suite(name="slow")
        @pytest.mark.suite(name="slow")
        def test_repeated(): pass
        """
    )
    result = suite_tests.runpytest("-m", "suite(name='slow')")
    result.assert_outcomes(passed=1, deselected=4)


def test_overlapping_function_groups_are_independent_of_marker_order(suite_tests):
    first, second = suite_tests.getitems("def test_first(): pass\ndef test_second(): pass")
    for item, groups in [(first, ["lambda", "nullability"]), (second, ["nullability", "lambda"])]:
        for group in groups:
            item.add_marker(pytest.mark.function(group=group))
    assert suites.infer_suite(first, Path("test.py")) == suites.infer_suite(second, Path("test.py"))


def test_explicit_suite_choices(suite_tests, monkeypatch):
    monkeypatch.setenv("PYSAIL_TEST_SUITE_CHOICES", '["integration-hms", "udf"]')
    result = suite_tests.runpytest("-v")
    result.assert_outcomes(passed=2, deselected=2)
    result.stdout.fnmatch_lines(["*test_integration PASSED*"])


def test_all_suites_disabled(suite_tests, monkeypatch):
    monkeypatch.setenv("PYSAIL_TEST_SUITE_CHOICES", "[]")
    result = suite_tests.runpytest()
    result.assert_outcomes(deselected=4)
    assert result.ret == pytest.ExitCode.NO_TESTS_COLLECTED


def test_inventory_includes_disabled_suites(suite_tests, monkeypatch):
    manifest = suite_tests.path / "suites.json"
    monkeypatch.setenv("PYSAIL_TEST_SUITE_MANIFEST", str(manifest))
    monkeypatch.setenv("PYSAIL_TEST_SUITE_CHOICES", '["default"]')
    result = suite_tests.runpytest("--collect-only", "-q")
    assert result.ret == pytest.ExitCode.OK
    assert json.loads(manifest.read_text()) == [
        {"name": "default", "test_count": 1},
        {"name": "function-nullability", "test_count": 1},
        {"name": "integration-hms", "test_count": 1},
        {"name": "udf", "test_count": 1},
    ]


def test_matrix_assignment_respects_groups_and_spark_requirements(suite_tests, monkeypatch):
    versions = ["3.5.9", "4.0.4", "4.1.3", "4.2.0"]
    suite_tests.makeconftest("""
        import json
        from pathlib import Path
        pytest_plugins = ["pysail.testing.suites", "pysail.testing.spark.markers"]
        def pytest_collection_finish(session):
            Path("selected.json").write_text(json.dumps([item.name for item in session.items]))
    """)
    suite_tests.makepyfile(
        test_groups="""
        import pytest
        @pytest.mark.suite(name="grouped")
        @pytest.mark.spark(version=">=4", reason="First feature")
        def test_first(): pass
        @pytest.mark.spark(reason="Different reason", version=">=4")
        @pytest.mark.suite(name="grouped")
        def test_second(): pass
        @pytest.mark.suite(name="grouped")
        @pytest.mark.spark(version=">=4.1", cross_version=True)
        def test_cross_version(): pass
        @pytest.mark.spark(version="<4.2")
        @pytest.mark.spark(version=">=4.1")
        def test_intersection(): pass
        @pytest.mark.spark(version="!=4.1.3,>=4", cross_version=True)
        def test_exclusion(): pass
        @pytest.mark.spark(version=">=99")
        def test_unsupported(): pass
    """
    )
    monkeypatch.setenv("PYSAIL_TEST_SEED", "seed-1")
    monkeypatch.setenv("PYSAIL_TEST_SPARK_VERSIONS", json.dumps(versions))
    assignments = {}
    for version in versions:
        monkeypatch.setattr(suites, "pyspark_version", lambda v=version: tuple(map(int, v.split("."))))
        result = suite_tests.runpytest("--collect-only", "-q", "test_groups.py")
        assert result.ret in (pytest.ExitCode.OK, pytest.ExitCode.NO_TESTS_COLLECTED)
        assignments[version] = json.loads((suite_tests.path / "selected.json").read_text())
    assert [v for v, names in assignments.items() if "test_first" in names] == [
        v for v, names in assignments.items() if "test_second" in names
    ]
    assert sum("test_first" in names for names in assignments.values()) == 1
    assert "test_first" not in assignments["3.5.9"]
    assert [v for v, names in assignments.items() if "test_cross_version" in names] == ["4.1.3", "4.2.0"]
    assert [v for v, names in assignments.items() if "test_intersection" in names] == ["4.1.3"]
    assert [v for v, names in assignments.items() if "test_exclusion" in names] == ["4.0.4", "4.2.0"]
    assert all("test_unsupported" not in names for names in assignments.values())


def test_assignment_is_independent_of_filters_collection_order_and_marker_order(suite_tests):
    first, second = suite_tests.getitems("def test_first(): pass\ndef test_second(): pass")
    for item, requirements in [(first, [">=4", "<4.2"]), (second, ["<4.2", ">=4"])]:
        for requirement in requirements:
            item.add_marker(pytest.mark.spark(version=requirement))
    versions = ["3.5.9", "4.0.4", "4.1.3", "4.2.0"]
    selected = suites.assigned_versions(first, "sql", "seed", versions)
    assert selected == suites.assigned_versions(second, "sql", "seed", versions[::-1])
    assert {tuple(suites.assigned_versions(first, "sql", str(seed), versions)) for seed in range(20)} == {
        ("4.0.4",),
        ("4.1.3",),
    }


@pytest.mark.parametrize(
    ("seed", "versions"),
    [
        (None, '["4.2.0"]'),
        ("seed", None),
        ("seed", "[]"),
        ("seed", "{}"),
        ("seed", "invalid"),
        ("seed", "[1]"),
        ("seed", '["invalid"]'),
        ("seed", '["4.2.0", "4.2.0"]'),
        ("seed", '["99.0.0"]'),
    ],
)
def test_invalid_matrix_configuration(suite_tests, monkeypatch, seed, versions):
    if seed is not None:
        monkeypatch.setenv("PYSAIL_TEST_SEED", seed)
    if versions is not None:
        monkeypatch.setenv("PYSAIL_TEST_SPARK_VERSIONS", versions)
    result = suite_tests.runpytest()
    assert result.ret == pytest.ExitCode.USAGE_ERROR
    result.stderr.fnmatch_lines(["*Set PYSAIL_TEST_SEED and PYSAIL_TEST_SPARK_VERSIONS*"])


@pytest.mark.parametrize("choices", ["{}", "invalid", "null", '"udf"', "[true]", "[1]", '["bad_name"]'])
def test_invalid_checklist_choices(suite_tests, monkeypatch, choices):
    monkeypatch.setenv("PYSAIL_TEST_SUITE_CHOICES", choices)
    result = suite_tests.runpytest()
    assert result.ret == pytest.ExitCode.USAGE_ERROR


def test_doctest_and_udf_markers_are_resolved_before_assignment(suite_tests, monkeypatch):
    suite_tests.makeconftest("""
        from pathlib import Path
        import pytest
        pytest_plugins = ["pysail.testing.suites", "pysail.testing.spark.markers", "pysail.testing.spark.doctest"]
        @pytest.hookimpl(tryfirst=True)
        def pytest_collection_modifyitems(items):
            for item in items:
                if item.path.is_relative_to(Path("spark/udf").resolve()):
                    item.add_marker(pytest.mark.spark(cross_version=True))
    """)
    suite_tests.makefile(".txt", test_arrow_scalar_udf=">>> 1 + 1\n2\n")
    monkeypatch.setenv("PYSAIL_TEST_SEED", "seed")
    monkeypatch.setenv("PYSAIL_TEST_SPARK_VERSIONS", '["3.5.9", "4.1.3"]')
    for version, count in [("3.5.9", 1), ("4.1.3", 2)]:
        monkeypatch.setattr(suites, "pyspark_version", lambda v=version: tuple(map(int, v.split("."))))
        result = suite_tests.runpytest(
            "--collect-only", "--doctest-glob=*.txt", "spark/udf", "test_arrow_scalar_udf.txt"
        )
        result.stdout.fnmatch_lines(["*1/2 tests collected*" if count == 1 else "*2 tests collected*"])
