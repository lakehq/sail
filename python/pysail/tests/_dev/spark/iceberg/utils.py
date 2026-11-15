import os
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

import pandas as pd
import pytest
from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import ManifestContent, read_manifest_list
from pyiceberg.typedef import EMPTY_DICT, Properties

from pysail.testing._dev.spark.steps.iceberg import _current_snapshot, _find_latest_metadata, _pyarrow_input_file

_WINDOWS_DRIVE_PREFIX_LENGTH = 2
_PYICEBERG_FILE_IO_IMPL = f"{__name__}.WindowsLocalPyArrowFileIO"


def _preserve_encoded_windows_colons(path: str) -> str:
    drive_prefix_len = (
        _WINDOWS_DRIVE_PREFIX_LENGTH
        if len(path) >= _WINDOWS_DRIVE_PREFIX_LENGTH and path[0].isalpha() and path[1] == ":"
        else 0
    )
    return path[:drive_prefix_len] + path[drive_prefix_len:].replace(":", "%3A")


class WindowsLocalPyArrowFileIO(PyArrowFileIO):
    @staticmethod
    def parse_location(location: str, properties: Properties = EMPTY_DICT) -> tuple[str, str, str]:
        uri = urlparse(location)
        if os.name == "nt" and uri.scheme == "file":
            path = url2pathname(f"//{uri.netloc}{uri.path}" if uri.netloc else uri.path)
            path = _preserve_encoded_windows_colons(path)
            return uri.scheme, uri.netloc, path
        return PyArrowFileIO.parse_location(location, properties)


def _local_file_path(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme not in ("", "file"):
        pytest.fail(f"cannot inspect non-local file URI: {uri}")
    return Path(url2pathname(f"//{parsed.netloc}{parsed.path}" if parsed.netloc else parsed.path))


def _current_manifest_entries(table_path: Path, content: ManifestContent):
    metadata = _find_latest_metadata(table_path)
    snapshot = _current_snapshot(metadata)
    io = WindowsLocalPyArrowFileIO()
    entries = []
    for manifest in read_manifest_list(_pyarrow_input_file(io, snapshot["manifest-list"])):
        if manifest.content == content:
            entries.extend(manifest.fetch_manifest_entry(io))
    return entries


def pyiceberg_file_io_properties() -> dict[str, str]:
    return {"py-io-impl": _PYICEBERG_FILE_IO_IMPL}


def create_sql_catalog(tmp_path: Path):
    warehouse_path = tmp_path / "warehouse"
    warehouse_path.mkdir(parents=True, exist_ok=True)
    catalog = load_catalog(
        "test_catalog",
        type="sql",
        uri=f"sqlite:///{tmp_path}/pyiceberg_catalog.db",
        warehouse=warehouse_path.as_uri(),
        **pyiceberg_file_io_properties(),
    )
    try:  # noqa: SIM105
        catalog.create_namespace("default")
    except Exception:  # noqa: S110, BLE001
        pass
    return catalog


def pyiceberg_to_pandas(table, sort_by=None, dtypes_like: pd.Series | None = None):
    df = table.scan().to_arrow().to_pandas()
    if sort_by is not None:
        if isinstance(sort_by, list | tuple):
            df = df.sort_values(list(sort_by)).reset_index(drop=True)
        else:
            df = df.sort_values(sort_by).reset_index(drop=True)
    if dtypes_like is not None:
        df = df.astype(dtypes_like)
    return df
