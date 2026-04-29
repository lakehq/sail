import pandas as pd
import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


@pytest.fixture
def sample_df(spark):
    schema = StructType(
        [
            StructField("col1", StringType(), True),
            StructField("col2", IntegerType(), True),
        ]
    )
    data = [("a", 1), ("b", 2), ("c", 3), (None, 4)]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_pandas_df():
    data = [("a", 1), ("b", 2), ("c", 3), (None, 4)]
    columns = ["col1", "col2"]
    return pd.DataFrame(data, columns=columns).astype(
        {
            "col1": "string",
            "col2": "Int32",
        }
    )


# Heterogeneous content used by `test_mixed_directory.py` to verify Spark-parity
# directory listing: the reader must see the four data files and must skip the
# three hidden ones (`_SUCCESS`, `_random`, `.something`).
_TEXT_FIXTURE_FILES = {
    "lower.csv": "name,age\nLower,30\n",
    "upper.CSV": "name,age\nUpper,50\n",
    "lower.json": '{"id":1,"value":"a"}\n{"id":2,"value":"b"}\n',
    "test.txt": "name,age\nTest,50\n",
    "_SUCCESS": "",
    "_random": "garbage\n",
    ".something": "garbage\n",
}


@pytest.fixture(scope="module")
def mixed_files_dir(tmp_path_factory):
    """A directory containing mixed-extension data files plus hidden marker
    files. Used by `test_mixed_directory.py` to verify Spark-parity behavior:
    every non-hidden file gets read regardless of extension, and the
    `_*` / `.*` hidden files are skipped."""
    path = tmp_path_factory.mktemp("mixed_files")
    for name, content in _TEXT_FIXTURE_FILES.items():
        (path / name).write_text(content)
    return path


@pytest.fixture(scope="module")
def mixed_files_with_parquet_dir(tmp_path_factory):
    """Same as `mixed_files_dir` but also includes two snappy-compressed
    parquet files (with realistic Spark-style names). Used by
    `test_mixed_directory.txt` (doctest) for exact-output comparison."""
    path = tmp_path_factory.mktemp("mixed_files_with_parquet")
    for name, content in _TEXT_FIXTURE_FILES.items():
        (path / name).write_text(content)
    parquet_uuid = "c2095e05-dd7d-4ea5-9cb9-2b30e32d5e29"
    pd.DataFrame({"name": ["P0a", "P0b"], "age": [60, 65]}).to_parquet(
        str(path / f"part-00000-{parquet_uuid}-c000.snappy.parquet"),
        compression="snappy",
    )
    pd.DataFrame({"name": ["P9a", "P9b"], "age": [90, 95]}).to_parquet(
        str(path / f"part-00009-{parquet_uuid}-c000.snappy.parquet"),
        compression="snappy",
    )
    return path


@pytest.fixture(scope="module", autouse=True)
def _datasource_doctest_namespace(doctest_namespace, mixed_files_with_parquet_dir):
    # Make the parquet-inclusive fixture path available to `.txt` doctests in
    # this directory as `MIXED_DIR`.
    doctest_namespace["MIXED_DIR"] = str(mixed_files_with_parquet_dir)
