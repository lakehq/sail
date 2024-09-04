import pysail
from pysail import _native


def test_version():
    v1, v2, v3, *_ = pysail.__version__.split(".")
    major, minor, patch = _native._SAIL_VERSION.split(".")  # noqa: SLF001
    assert v1 == major
    assert v2 == minor
    assert v3 == patch or v3.startswith((f"{patch}a", f"{patch}b", f"{patch}rc"))
