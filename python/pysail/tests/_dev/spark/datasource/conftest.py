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
    "upper.JSON": '{"id":1,"value":"A"}\n{"id":2,"value":"B"}\n',
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


@pytest.fixture(scope="module", autouse=True)
def _datasource_doctest_namespace(doctest_namespace, mixed_files_dir):
    # Make the fixture path available to `.txt` doctests in this directory
    # as `MIXED_DIR`. Parquet files are intentionally excluded — reading
    # binary parquet bytes as CSV/JSON yields non-printable garbage that
    # can't be meaningfully snapshot-tested.
    doctest_namespace["MIXED_DIR"] = str(mixed_files_dir)
