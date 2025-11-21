from __future__ import annotations

from typing import TYPE_CHECKING

from pyiceberg.catalog import load_catalog

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    import pandas as pd


def create_sql_catalog(tmp_path: Path):
    warehouse_path = tmp_path / "warehouse"
    warehouse_path.mkdir(parents=True, exist_ok=True)
    catalog = load_catalog(
        "test_catalog",
        type="sql",
        uri=f"sqlite:///{tmp_path}/pyiceberg_catalog.db",
        warehouse=f"file://{warehouse_path}",
    )
    try:  # noqa: SIM105
        catalog.create_namespace("default")
    except Exception:  # noqa: S110, BLE001
        pass
    return catalog


def create_catalog(tmp_path: Path, rest_config: Mapping[str, str] | None = None):
    if rest_config is None:
        return create_sql_catalog(tmp_path)
    catalog = load_catalog(
        "rest",
        type="rest",
        uri=rest_config["uri"],
        warehouse=rest_config["warehouse"],
        **{
            "s3.endpoint": rest_config["s3_endpoint"],
            "s3.access-key-id": rest_config["s3_access_key"],
            "s3.secret-access-key": rest_config["s3_secret_key"],
        },
    )
    try:  # noqa: SIM105
        catalog.create_namespace("default")
    except Exception:  # noqa: S110, BLE001
        pass
    return catalog


def pyiceberg_to_pandas(table, sort_by=None, dtypes_like: pd.Series | None = None):
    df = table.scan().to_arrow().to_pandas()
    if sort_by is not None:
        if isinstance(sort_by, (list, tuple)):
            df = df.sort_values(list(sort_by)).reset_index(drop=True)
        else:
            df = df.sort_values(sort_by).reset_index(drop=True)
    if dtypes_like is not None:
        df = df.astype(dtypes_like)
    return df
