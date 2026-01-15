"""
PySpark compatibility utilities for unpickling client-registered DataSources.

When a PySpark client calls spark.dataSource.register(MyDS), PySpark pickles
the class with cloudpickle. The pickled bytes contain references to pyspark
modules (e.g., 'pyspark.sql.datasource.DataSource'). When Sail unpickles this,
Python needs to be able to resolve those module paths.

This module provides shims to map pyspark module paths to pysail equivalents.

Usage:
    Server-side (Sail): The shim is installed automatically when unpickling
    via unpickle_datasource_class(). No user action needed.

    Client-side: The shim is NOT installed automatically. If you need to test
    DataSource unpickling without PySpark installed, call install_pyspark_shim()
    explicitly:

        from pysail.spark.datasource import install_pyspark_shim
        install_pyspark_shim()

Warning:
    Do NOT use the shim in environments where real PySpark is also used.
    The shim modifies sys.modules globally and will shadow real PySpark modules.
"""

import logging
import sys
import types

_logger = logging.getLogger(__name__)
_shim_installed = False


def install_pyspark_shim(warn_if_pyspark_present: bool = True) -> bool:
    """
    Install module shims so PySpark-pickled DataSources can be unpickled in Sail.

    This function modifies sys.modules to redirect pyspark module paths to pysail:
        pyspark.sql.datasource -> pysail.spark.datasource
        pyspark.sql.datasource_internal -> pysail.spark.datasource
        pyspark.cloudpickle -> cloudpickle

    Args:
        warn_if_pyspark_present: If True, log a warning when real PySpark modules
            are already loaded. Set to False for server-side usage where this
            warning is not relevant.

    Returns:
        True if shim was installed successfully, False otherwise.

    Warning:
        This modifies global state (sys.modules). Do not use in environments
        where real PySpark is also needed in the same process.

    Example:
        # For testing DataSource unpickling without PySpark:
        from pysail.spark.datasource import install_pyspark_shim
        install_pyspark_shim()

        # Then unpickle your DataSource
        import cloudpickle
        ds_class = cloudpickle.loads(pickled_bytes)
    """
    global _shim_installed

    try:
        import cloudpickle
        from pysail.spark import datasource

        # Warn if real PySpark is already loaded (indicates potential conflict)
        if warn_if_pyspark_present and "pyspark" in sys.modules:
            existing = sys.modules["pyspark"]
            # Check if it's the real PySpark (has __file__) vs our stub
            if hasattr(existing, "__file__") and existing.__file__ is not None:
                _logger.warning(
                    "Real PySpark is already imported. Installing the pysail shim "
                    "will shadow pyspark.sql.datasource with pysail's implementation. "
                    "This may cause issues if you need both PySpark and PySail "
                    "DataSource APIs in the same process."
                )

        # Create mock pyspark module hierarchy if it doesn't exist
        if "pyspark" not in sys.modules:
            pyspark_mod = types.ModuleType("pyspark")
            pyspark_mod.__path__ = []  # Make it a package
            sys.modules["pyspark"] = pyspark_mod

        if "pyspark.sql" not in sys.modules:
            pyspark_sql_mod = types.ModuleType("pyspark.sql")
            pyspark_sql_mod.__path__ = []  # Make it a package
            sys.modules["pyspark.sql"] = pyspark_sql_mod
            sys.modules["pyspark"].sql = pyspark_sql_mod

        # Map pyspark.cloudpickle to cloudpickle
        sys.modules["pyspark.cloudpickle"] = cloudpickle
        sys.modules["pyspark"].cloudpickle = cloudpickle

        # Map datasource modules to pysail
        sys.modules["pyspark.sql.datasource"] = datasource
        sys.modules["pyspark.sql.datasource_internal"] = datasource

        _shim_installed = True
        return True
    except ImportError as e:
        _logger.error("Failed to install PySpark shim: %s", e)
        return False


def is_shim_installed() -> bool:
    """Check if the PySpark compatibility shim has been installed."""
    return _shim_installed


def unpickle_datasource_class(pickled_bytes: bytes):
    """
    Unpickle a PySpark DataSource class with shim support.

    This is the server-side unpickling function called by Sail. It automatically
    installs the PySpark shim before unpickling.

    Args:
        pickled_bytes: The cloudpickle bytes containing the DataSource class

    Returns:
        The unpickled DataSource class

    Raises:
        Exception if unpickling fails
    """
    import cloudpickle

    # Install shim before unpickling (server-side, no warning needed)
    install_pyspark_shim(warn_if_pyspark_present=False)

    # Unpickle the class
    return cloudpickle.loads(pickled_bytes)
