import os
import shutil
import subprocess
import sys

import pytest

import pysail


def _write_dev_packages(root, location):
    for name in ("tests", "testing"):
        package = root / name / "_dev"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text(f"LOCATION = {location!r}\n")
        (package / "helper.py").write_text("VALUE = 'helper'\n")


@pytest.mark.parametrize(
    "location",
    [
        "adjacent",
        "external",
        "override",
        "absent",
        "invalid",
        "invalid_override",
        "empty",
        "missing_tests",
        "missing_testing",
    ],
)
def test_dev_package_import(tmp_path, location):
    """Resolve both development packages without replacing installed parent packages."""
    package = tmp_path / "pysail"
    package.mkdir()
    shutil.copyfile(pysail.__file__, package / "__init__.py")
    (package / "regular.py").write_text("VALUE = 'regular'\n")
    for name in ("tests", "testing"):
        parent = package / name
        parent.mkdir()
        (parent / "__init__.py").write_text("LOCATION = 'installed'\n")
    env = {key: value for key, value in os.environ.items() if key != "PYSAIL_SOURCE_PATH"}
    if location in {"adjacent", "override", "invalid_override", "empty"}:
        _write_dev_packages(package, "adjacent")
    external = tmp_path / "checkout" / "python" / "pysail"
    if location in {"external", "override", "invalid", "invalid_override", "missing_tests", "missing_testing"}:
        external.mkdir(parents=True)
        env["PYSAIL_SOURCE_PATH"] = str(external)
        if location not in {"invalid", "invalid_override"}:
            _write_dev_packages(external, "external")
        if location.startswith("missing_"):
            (external / location.removeprefix("missing_") / "_dev" / "__init__.py").unlink()
    if location == "empty":
        env["PYSAIL_SOURCE_PATH"] = ""

    expected = "external" if location == "override" else location
    script = """
import importlib
import importlib.util
import sys

sys.path.insert(0, sys.argv[1])
import pysail
import pysail.regular

assert pysail.regular.VALUE == 'regular'
for name in ('tests', 'testing'):
    parent = importlib.import_module(f'pysail.{name}')
    assert parent.LOCATION == 'installed'
    module = f'pysail.{name}._dev'
    assert module not in sys.modules
    if sys.argv[2] == 'absent':
        assert importlib.util.find_spec(module) is None
    else:
        package = importlib.import_module(module)
        helper = importlib.import_module(f'{module}.helper')
        assert package.LOCATION == sys.argv[2]
        assert helper.VALUE == 'helper'
        assert package.__spec__.submodule_search_locations
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", script, str(tmp_path), expected],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    if location in {"invalid", "invalid_override", "empty", "missing_tests", "missing_testing"}:
        assert result.returncode != 0
        assert "ImportError: PYSAIL_SOURCE_PATH is set" in result.stderr
        root = tmp_path if location == "empty" else external
        name = "testing" if location == "missing_testing" else "tests"
        assert str(root / name / "_dev" / "__init__.py") in result.stderr
    else:
        assert result.returncode == 0, result.stderr
