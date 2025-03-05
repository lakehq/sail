# The version must be the same as the one defined in `pyproject.toml`.
# We have a CI step that verifies this.
# We cannot use the Hatch dynamic version feature since the project
# may be built with Maturin outside of Hatch.
__version__: str = "0.2.2"

__all__ = ["__version__"]
