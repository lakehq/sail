# SPDX-License-Identifier: Apache-2.0

"""Check shared pytest configuration outside the project configuration file."""

import pytest

pytest_plugins = ["pytester"]


@pytest.fixture
def shared_configuration(pytester, monkeypatch):
    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    for name in (
        "PYSAIL_TEST_SUITE_MANIFEST",
        "PYSAIL_TEST_SEED",
        "PYSAIL_TEST_SPARK_VERSIONS",
        "PYSAIL_TEST_SUITE_CHOICES",
    ):
        monkeypatch.delenv(name, raising=False)
    pytester.makeconftest('pytest_plugins = ["pysail.testing.fixtures"]')
    return pytester


@pytest.mark.parametrize("marker", ["yamlsnapshot", "misspelled"])
def test_strict_marker_collection(shared_configuration, marker):
    shared_configuration.makepyfile(f"import pytest\n@pytest.mark.{marker}\ndef test_example(): pass\n")

    result = shared_configuration.runpytest_subprocess("--collect-only", "-q")

    if marker == "misspelled":
        assert result.ret == pytest.ExitCode.INTERRUPTED
        result.assert_outcomes(errors=1)
        result.stdout.fnmatch_lines(["*'misspelled' not found in `markers` configuration option*"])
    else:
        assert result.ret == pytest.ExitCode.OK
        result.stdout.fnmatch_lines(["*1 test collected*"])


@pytest.mark.parametrize(
    "args",
    [(), ("-o", "doctest_optionflags=NUMBER ELLIPSIS ELLIPSIS")],
    ids=["ini", "command-line"],
)
def test_doctest_optionflags_are_fixed(shared_configuration, args):
    shared_configuration.makeini("[pytest]\ndoctest_optionflags = NUMBER DONT_ACCEPT_TRUE_FOR_1")
    shared_configuration.makepyfile("""
        def test_flags(pytestconfig):
            assert pytestconfig.getini("doctest_optionflags") == [
                "ELLIPSIS",
                "NORMALIZE_WHITESPACE",
                "IGNORE_EXCEPTION_DETAIL",
            ]
    """)

    result = shared_configuration.runpytest_subprocess("-q", *args)

    result.assert_outcomes(passed=1)
