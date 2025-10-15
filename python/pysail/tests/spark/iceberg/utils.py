from pathlib import Path

from pyiceberg.catalog import load_catalog


def create_sql_catalog(tmp_path: Path):
    warehouse_path = tmp_path / "warehouse"
    warehouse_path.mkdir(parents=True, exist_ok=True)
    catalog = load_catalog(
        "test_catalog",
        type="sql",
        uri=f"sqlite:///{tmp_path}/pyiceberg_catalog.db",
        warehouse=f"file://{warehouse_path}",
    )
    try:
        catalog.create_namespace("default")
    except Exception:
        pass
    return catalog


