import os
import sys
from importlib.abc import MetaPathFinder
from importlib.util import spec_from_file_location
from pathlib import Path


def _register_dev_packages():
    # `PYSAIL_SOURCE_PATH` points to `python/pysail` in a source checkout, allowing
    # its development packages to be used with an installed release of `pysail`.
    path = os.environ.get("PYSAIL_SOURCE_PATH")
    root = (Path(path) if path is not None else Path(__file__).parent).resolve()
    modules = {}
    for name in ("tests", "testing"):
        init = root / name / "_dev" / "__init__.py"
        if init.is_file():
            modules[f"pysail.{name}._dev"] = init
        elif path is not None:
            msg = f"PYSAIL_SOURCE_PATH is set, but the development package initializer was not found: {init}"
            raise ImportError(msg)

    class DevPackageFinder(MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            del path, target
            if fullname in modules:
                return spec_from_file_location(fullname, modules[fullname])
            return None

    # Prefer explicitly configured packages over the adjacent `_dev` directories.
    # This makes `_dev` packages importable, but `pytest --pyargs` still needs the
    # explicit package name `pysail.tests._dev` to collect tests from an external checkout.
    if modules:
        sys.meta_path.insert(0, DevPackageFinder())


_register_dev_packages()

# The version must be the same as the one defined in `pyproject.toml`.
# We have a CI step that verifies this.
# We cannot use the Hatch dynamic version feature since the project
# may be built with Maturin outside of Hatch.
__version__: str = "0.7.3"

__all__ = ["__version__"]
