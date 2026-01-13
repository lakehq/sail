# The version must be the same as the one defined in `pyproject.toml`.
# We have a CI step that verifies this.
# We cannot use the Hatch dynamic version feature since the project
# may be built with Maturin outside of Hatch.
__version__: str = "0.4.6"

# Auto-import datasource module to trigger @register decorators
# This ensures example datasources are registered when pysail is loaded
try:
    from pysail.spark import datasource as _datasource  # noqa: F401
except ImportError:
    pass  # Datasource module not available

__all__ = ["__version__"]
