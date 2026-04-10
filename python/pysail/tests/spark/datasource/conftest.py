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
