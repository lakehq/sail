"""
PySpark compatibility utilities for unpickling client-registered DataSources.

When a PySpark client calls spark.dataSource.register(MyDS), PySpark pickles
the class with cloudpickle. The pickled bytes contain references to pyspark
modules (e.g., 'pyspark.sql.datasource.DataSource'). When Sail unpickles this,
Python needs to be able to resolve those module paths.

This module provides shims to map pyspark module paths to pysail equivalents.
"""

import sys
import types


def install_pyspark_shim():
    """
    Install module shims so PySpark-pickled DataSources can be unpickled in Sail.

    Maps:
        pyspark.sql.datasource -> pysail.spark.datasource
        pyspark.cloudpickle -> cloudpickle

    This must be called before cloudpickle.loads() on any PySpark-pickled data.

    Returns:
        True if successful, False otherwise
    """
    try:
        import cloudpickle
        from pysail.spark import datasource

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

        return True
    except ImportError as e:
        print(f"PySpark shim error: {e}")
        return False


def unpickle_datasource_class(pickled_bytes: bytes):
    """
    Unpickle a PySpark DataSource class with shim support.

    Args:
        pickled_bytes: The cloudpickle bytes containing the DataSource class

    Returns:
        The unpickled DataSource class

    Raises:
        Exception if unpickling fails
    """
    import cloudpickle

    # Install shim before unpickling
    install_pyspark_shim()

    # Unpickle the class
    return cloudpickle.loads(pickled_bytes)
