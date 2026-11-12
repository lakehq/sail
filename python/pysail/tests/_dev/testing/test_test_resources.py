"""Exercise resource discovery and deterministic PySpark assignment."""

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from pysail.testing import suites
from pysail.testing.resources import resource

pytest_plugins = ["pytester"]


@pytest.fixture
def resource_tests(pytester, monkeypatch):
    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    for name in (
        "PYSAIL_TEST_SUITE_MANIFEST",
        "PYSAIL_TEST_SEED",
        "PYSAIL_TEST_SPARK_VERSIONS",
        "PYSAIL_TEST_SUITE_CHOICES",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(suites, "relative_test_path", lambda path, **_: path.relative_to(pytester.path))
    pytester.makeconftest("""
        import json
        from pathlib import Path
        from pysail.testing.suites import item_affinity
        pytest_plugins = ["pysail.testing.suites", "pysail.testing.spark.markers"]

        def pytest_collection_finish(session):
            Path("selected.json").write_text(json.dumps({
                item.nodeid: item_affinity(item)
                for item in session.items
            }))
    """)
    return pytester


def collect_affinities(pytester, *args):
    result = pytester.runpytest("--collect-only", "-q", *args)
    assert result.ret in (pytest.ExitCode.OK, pytest.ExitCode.NO_TESTS_COLLECTED)
    return json.loads((pytester.path / "selected.json").read_text())


@pytest.fixture
def bdd_resource_tests(resource_tests):
    with (resource_tests.path / "conftest.py").open("a") as file:
        file.write('\npytest_plugins.append("pytest_bdd.plugin")\n')
    for directory, content in [
        (
            "one",
            "Feature: First feature\n"
            "  Scenario: First case\n"
            "    Given a service\n"
            "  Scenario Outline: Outlined case\n"
            "    Given value <value>\n"
            "    Examples:\n"
            "      | value |\n"
            "      | 1     |\n"
            "      | 2     |\n",
        ),
        ("two", "Feature: Second feature\n  Scenario: Second case\n    Given a service\n"),
    ]:
        path = resource_tests.path / directory
        path.mkdir()
        (path / "shared.feature").write_text(content)
    return resource_tests


@pytest.mark.parametrize("weight", [0.5, 1, 10])
def test_bdd_affinity_uses_feature_paths_and_preserves_resource_overrides(bdd_resource_tests, weight):
    bdd_resource_tests.makepyfile(
        test_loader=f"""
        import pytest
        from pytest_bdd import scenarios
        from pysail.testing.resources import resource

        scenarios("one", "two")

        @pytest.fixture(autouse=True)
        @resource(name="service", weight={weight})
        def service():
            raise AssertionError("Collection must not execute fixtures")

        def test_plain(): pass
        """,
        test_other_loader="""
        from pytest_bdd import scenarios
        scenarios("one/shared.feature")
        """,
    )
    expected = {
        "test_loader.py::test_first_case": ["feature", "one/shared.feature"],
        "test_loader.py::test_outlined_case[1]": ["feature", "one/shared.feature"],
        "test_loader.py::test_outlined_case[2]": ["feature", "one/shared.feature"],
        "test_loader.py::test_second_case": ["feature", "two/shared.feature"],
        "test_loader.py::test_plain": ["module", "test_loader.py"],
        "test_other_loader.py::test_first_case": ["feature", "one/shared.feature"],
        "test_other_loader.py::test_outlined_case[1]": ["feature", "one/shared.feature"],
        "test_other_loader.py::test_outlined_case[2]": ["feature", "one/shared.feature"],
    }
    if weight > 1:
        expected = {
            nodeid: ["resource", "service"] if nodeid.startswith("test_loader.py::") else key
            for nodeid, key in expected.items()
        }
    assert collect_affinities(bdd_resource_tests) == expected
    filtered = collect_affinities(bdd_resource_tests, "test_loader.py", "-k", "one/shared.feature")
    assert filtered == {
        nodeid: key
        for nodeid, key in expected.items()
        if nodeid.startswith(("test_loader.py::test_first_case", "test_loader.py::test_outlined_case"))
    }


def test_bdd_assignment_preserves_version_requirements_and_cross_version(bdd_resource_tests, monkeypatch):
    bdd_resource_tests.makepyfile(
        test_loader="""
        import pytest
        from pytest_bdd import scenario

        @scenario("one/shared.feature", "First case")
        def test_unrestricted(): pass

        @pytest.mark.spark(version=">=4.1")
        @pytest.mark.spark(version="<4.2")
        @scenario("one/shared.feature", "First case")
        def test_restricted(): pass

        @pytest.mark.spark(version=">=4.1", cross_version=True)
        @scenario("one/shared.feature", "First case")
        def test_cross_version(): pass
    """
    )
    versions = ["3.5.9", "4.1.4", "4.2.1"]
    monkeypatch.setenv("PYSAIL_TEST_SEED", "bdd")
    monkeypatch.setenv("PYSAIL_TEST_SPARK_VERSIONS", json.dumps(versions))
    assignments = {}
    for version in versions:
        monkeypatch.setattr(suites, "pyspark_version", lambda v=version: tuple(map(int, v.split("."))))
        for nodeid, key in collect_affinities(bdd_resource_tests).items():
            assert key == ["feature", "one/shared.feature"]
            assignments.setdefault(nodeid.split("::")[-1], []).append(version)
    assert len(assignments["test_unrestricted"]) == 1
    assert assignments["test_restricted"] == ["4.1.4"]
    assert assignments["test_cross_version"] == ["4.1.4", "4.2.1"]


def test_module_affinity_includes_parameters_classes_and_doctests(resource_tests):
    resource_tests.makepyfile(
        test_first="""
        import pytest
        @pytest.mark.parametrize("value", [1, 2])
        def test_parameter(value): pass
        class TestMethods:
            def test_method(self): pass
        """,
        test_second="def test_other(): pass",
    )
    resource_tests.makefile(".txt", test_example=">>> 1 + 1\n2\n")
    selected = collect_affinities(resource_tests, "--doctest-glob=*.txt")
    assert selected == {
        "test_first.py::test_parameter[1]": ["module", "test_first.py"],
        "test_first.py::test_parameter[2]": ["module", "test_first.py"],
        "test_first.py::TestMethods::test_method": ["module", "test_first.py"],
        "test_second.py::test_other": ["module", "test_second.py"],
        "test_example.txt::test_example.txt": ["module", "test_example.txt"],
    }


def test_resources_propagate_through_indirect_autouse_and_usefixtures_dependencies(resource_tests):
    with (resource_tests.path / "conftest.py").open("a") as file:
        file.write("""
import pytest
from pysail.testing.resources import resource

@pytest.fixture(scope="session")
@resource(name="cluster", weight=10)
def cluster():
    raise AssertionError("Collection must not execute fixtures")

@pytest.fixture
def client(cluster): pass
""")
    resource_tests.makepyfile(
        test_direct="def test_direct(cluster): pass",
        test_indirect="""
        import pytest
        def test_indirect(client): pass
        @pytest.mark.usefixtures("client")
        def test_marked(): pass
        """,
        test_autouse="""
        import pytest
        @pytest.fixture(autouse=True)
        def setup(client): pass
        def test_automatic(): pass
        """,
    )
    selected = collect_affinities(resource_tests)
    assert set(selected) == {
        "test_direct.py::test_direct",
        "test_indirect.py::test_indirect",
        "test_indirect.py::test_marked",
        "test_autouse.py::test_automatic",
    }
    assert all(key == ["resource", "cluster"] for key in selected.values())
    assert collect_affinities(resource_tests, "test_indirect.py", "-k", "marked") == {
        "test_indirect.py::test_marked": ["resource", "cluster"]
    }


def test_highest_weight_wins_without_merging_modules(resource_tests):
    resource_tests.makepyfile("""
        import pytest
        from pysail.testing.resources import resource

        @pytest.fixture
        @resource(name="hms", weight=10)
        def hms(): pass

        @pytest.fixture
        @resource(name="delta", weight=20)
        def delta(hms): pass

        def test_hms(hms): pass
        def test_delta(delta): pass
        def test_plain(): pass
    """)
    selected = collect_affinities(resource_tests)
    by_name = {nodeid.split("::")[-1]: key for nodeid, key in selected.items()}
    assert by_name["test_hms"] == ["resource", "hms"]
    assert by_name["test_delta"] == ["resource", "delta"]
    assert by_name["test_plain"][0] == "module"


@pytest.mark.parametrize("weight", [0.5, 1, 1.5])
def test_module_has_weight_one(resource_tests, weight):
    resource_tests.makepyfile(
        test_weight=f"""
        import pytest
        from pysail.testing.resources import resource
        @pytest.fixture
        @resource(name="service", weight={weight})
        def service(): pass
        def test_example(service): pass
    """
    )
    expected = ["resource", "service"] if weight > 1 else ["module", "test_weight.py"]
    assert collect_affinities(resource_tests) == {"test_weight.py::test_example": expected}


@pytest.mark.parametrize("cross_version", [False, True])
@pytest.mark.parametrize("weight", [0.5, 10])
def test_equal_resource_weights_are_rejected_even_below_the_winner(resource_tests, weight, cross_version):
    resource_tests.makepyfile(
        test_conflict=f"""
        import pytest
        from pysail.testing.resources import resource
        @pytest.fixture
        @resource(name="a", weight={weight})
        def first(): pass
        @pytest.fixture
        @resource(name="b", weight={weight})
        def second(): pass
        @pytest.fixture
        @resource(name="winner", weight=100)
        def winner(): pass
        @pytest.mark.spark(cross_version={cross_version})
        def test_example(first, second, winner): pass
    """
    )
    result = resource_tests.runpytest("--collect-only", "-q")
    assert result.ret == pytest.ExitCode.USAGE_ERROR
    result.stderr.fnmatch_lines([f"*test_conflict.py::test_example: resources have equal weight {weight}: a, b*"])


@pytest.mark.parametrize("second_weight", [10, 20])
def test_repeated_resource_names_must_agree_on_weight(resource_tests, second_weight):
    resource_tests.makepyfile(
        test_repeated=f"""
        import pytest
        from pysail.testing.resources import resource
        @pytest.fixture
        @resource(name="cluster", weight=10)
        def first(): pass
        @pytest.fixture
        @resource(name="cluster", weight={second_weight})
        def second(first): pass
        def test_example(first, second): pass
    """
    )
    if second_weight == 10:  # noqa: PLR2004
        assert collect_affinities(resource_tests) == {"test_repeated.py::test_example": ["resource", "cluster"]}
    else:
        result = resource_tests.runpytest("--collect-only", "-q")
        assert result.ret == pytest.ExitCode.USAGE_ERROR
        result.stderr.fnmatch_lines(["*resource 'cluster' has conflicting weights:*"])


def test_fixture_overrides_parent_requests_aliases_and_direct_parameters(resource_tests):
    with (resource_tests.path / "conftest.py").open("a") as file:
        file.write("""
import pytest
from pysail.testing.resources import resource
@pytest.fixture
@resource(name="parent", weight=10)
def service(): pass
""")
    resource_tests.makepyfile(
        test_override="""
        import pytest
        @pytest.fixture
        def service(): pass
        def test_example(service): pass
        """,
        test_parent="""
        import pytest
        @pytest.fixture
        def service(service): pass
        def test_example(service): pass
        """,
        test_parameter="""
        import pytest
        @pytest.mark.parametrize("service", [1])
        def test_example(service): pass
        """,
        test_class="""
        import pytest
        from pysail.testing.resources import resource
        class TestExample:
            @pytest.fixture(name="service")
            @resource(name="child", weight=20)
            def renamed(self, service): pass
            def test_example(self, service): pass
        """,
    )
    assert collect_affinities(resource_tests) == {
        "test_override.py::test_example": ["module", "test_override.py"],
        "test_parent.py::test_example": ["resource", "parent"],
        "test_parameter.py::test_example[1]": ["module", "test_parameter.py"],
        "test_class.py::TestExample::test_example": ["resource", "child"],
    }


def test_resource_assignment_preserves_version_preferences_and_cross_version(resource_tests, monkeypatch):
    with (resource_tests.path / "conftest.py").open("a") as file:
        file.write("""
import pytest
from pysail.testing.resources import resource
@pytest.fixture
@resource(name="shared", weight=10)
def service(): pass
""")
    resource_tests.makepyfile(
        test_first="""
        import pytest
        def test_unrestricted(service): pass
        @pytest.mark.spark(version=">=4")
        def test_restricted(service): pass
        @pytest.mark.spark(version=">=4.1")
        @pytest.mark.spark(version="<4.2")
        def test_intersection(service): pass
        @pytest.mark.spark(version=">=4.1", cross_version=True)
        def test_cross_version(service): pass
        @pytest.mark.spark(version=">=99")
        def test_unsupported(service): pass
        """,
        test_second="def test_other_module(service): pass",
    )
    versions = ["3.5.9", "4.0.5", "4.1.4", "4.2.1"]
    monkeypatch.setenv("PYSAIL_TEST_SPARK_VERSIONS", json.dumps(versions))
    for seed in ("first", "second"):
        monkeypatch.setenv("PYSAIL_TEST_SEED", seed)
        assignments = {}
        for version in versions:
            monkeypatch.setattr(suites, "pyspark_version", lambda v=version: tuple(map(int, v.split("."))))
            selected = collect_affinities(resource_tests)
            for nodeid in selected:
                assignments.setdefault(nodeid.split("::")[-1], []).append(version)
            filtered = collect_affinities(resource_tests, "test_second.py", "-k", "other_module")
            assert filtered == {nodeid: key for nodeid, key in selected.items() if "other_module" in nodeid}
        preferred = assignments["test_unrestricted"]
        assert len(preferred) == 1
        assert assignments["test_other_module"] == preferred
        assert len(assignments["test_restricted"]) == 1
        assert assignments["test_restricted"][0] in versions[1:]
        if preferred[0] != "3.5.9":
            assert assignments["test_restricted"] == preferred
        assert assignments["test_intersection"] == ["4.1.4"]
        assert assignments["test_cross_version"] == ["4.1.4", "4.2.1"]
        assert "test_unsupported" not in assignments


@pytest.mark.parametrize("name", [None, "", " ", " leading", "trailing ", 1])
def test_invalid_resource_names(name):
    with pytest.raises(ValueError, match="Resource name"):
        resource(name=name, weight=10)


@pytest.mark.parametrize("weight", [None, "10", True, 0, -1, float("inf"), float("nan")])
def test_invalid_resource_weights(weight):
    with pytest.raises(ValueError, match="Resource weight"):
        resource(name="service", weight=weight)


def test_resource_requires_the_documented_decorator_order():
    @pytest.fixture
    def service():
        pass

    with pytest.raises(TypeError, match=r"Place @resource below @pytest\.fixture"):
        resource(name="service", weight=10)(service)


def test_module_paths_are_stable_and_distinguish_development_tests(monkeypatch, tmp_path):
    installed = Path(suites.__file__).resolve().parents[1] / "tests"
    for dev in (installed / "_dev", tmp_path / "source" / "tests" / "_dev"):
        monkeypatch.setitem(sys.modules, "pysail.tests._dev", SimpleNamespace(__file__=str(dev / "__init__.py")))
        path = Path("spark/test_example.py")
        assert suites.relative_test_path(installed / path, strip_dev=False) == path
        assert suites.relative_test_path(dev / path, strip_dev=False) == Path("_dev") / path
        assert suites.relative_test_path(dev / path) == path
