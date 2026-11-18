"""Check shared pytest configuration outside the project configuration file."""

import pytest

pytest_plugins = ["pytester"]


@pytest.mark.parametrize("marker", ["yamlsnapshot", "misspelled"])
def test_strict_marker_collection(pytester, monkeypatch, marker):
    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    for name in (
        "PYSAIL_TEST_SUITE_MANIFEST",
        "PYSAIL_TEST_SEED",
        "PYSAIL_TEST_SPARK_VERSIONS",
        "PYSAIL_TEST_SUITE_CHOICES",
    ):
        monkeypatch.delenv(name, raising=False)
    pytester.makeconftest('pytest_plugins = ["pysail.testing.fixtures"]')
    pytester.makepyfile(f"import pytest\n@pytest.mark.{marker}\ndef test_example(): pass\n")

    result = pytester.runpytest_subprocess("--collect-only", "-q")

    if marker == "misspelled":
        assert result.ret == pytest.ExitCode.INTERRUPTED
        result.assert_outcomes(errors=1)
        result.stdout.fnmatch_lines(["*'misspelled' not found in `markers` configuration option*"])
    else:
        assert result.ret == pytest.ExitCode.OK
        result.stdout.fnmatch_lines(["*1 test collected*"])
