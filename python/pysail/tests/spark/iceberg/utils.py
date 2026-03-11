import os
from pathlib import Path

import pandas as pd

try:
    from pyiceberg.catalog import load_catalog

    HAS_PYICEBERG = True
except ImportError:
    HAS_PYICEBERG = False


def pyiceberg_local_location(path: Path) -> str:
    """Return a local location string that PyIceberg can use across platforms."""
    resolved = path.resolve()
    if os.name == "nt":
        # PyIceberg's PyArrowFileIO treats raw "C:\\..." paths as scheme "c".
        # Use "file://C:/..." so parse_location returns scheme=file and path "C:/...".
        drive = resolved.drive.rstrip(":")
        tail = resolved.as_posix().lstrip("/")
        if drive:
            return f"file://{drive}:/{tail.split(':/', 1)[-1]}"
        return resolved.as_uri()
    return resolved.as_uri()


def create_sql_catalog(tmp_path: Path):
    if not HAS_PYICEBERG:
        msg = "PyIceberg is required for create_sql_catalog"
        raise ImportError(msg)

    warehouse_path = tmp_path / "warehouse"
    warehouse_path.mkdir(parents=True, exist_ok=True)
    catalog = load_catalog(
        "test_catalog",
        type="sql",
        uri=f"sqlite:///{tmp_path.as_posix()}/pyiceberg_catalog.db",
        warehouse=pyiceberg_local_location(warehouse_path),
    )
    try:  # noqa: SIM105
        catalog.create_namespace("default")
    except Exception:  # noqa: S110, BLE001
        pass
    return catalog


def pyiceberg_to_pandas(table, sort_by=None, dtypes_like: pd.Series | None = None):
    if not HAS_PYICEBERG:
        msg = "PyIceberg is required for pyiceberg_to_pandas"
        raise ImportError(msg)
    df = table.scan().to_arrow().to_pandas()
    if sort_by is not None:
        if isinstance(sort_by, list | tuple):
            df = df.sort_values(list(sort_by)).reset_index(drop=True)
        else:
            df = df.sort_values(sort_by).reset_index(drop=True)
    if dtypes_like is not None:
        df = df.astype(dtypes_like)
    return df
