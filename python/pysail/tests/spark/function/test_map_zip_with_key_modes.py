import math

import pyarrow as pa
import pytest
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.common import pyspark_version


@pytest.mark.skipif(pyspark_version() < (4, 1), reason="The Java map key equality mode requires Spark 4.1+")
@pytest.mark.parametrize("java_collections", [False, True])
@pytest.mark.parametrize("key_type", [pa.float32(), pa.float64()])
def test_map_zip_with_floating_key_modes(spark, java_collections, key_type):
    # Arrow permits duplicate NaN keys and preserves the original signed zero.
    spark.conf.set("spark.sql.mapZipWithUsesJavaCollections", str(java_collections).lower())
    source = spark.createDataFrame(
        pa.table(
            {
                "left": pa.array(
                    [[(-0.0, 1), (float("nan"), 2), (float("nan"), 3)]],
                    type=pa.map_(key_type, pa.int32()),
                ),
                "right": pa.array(
                    [[(0.0, 4), (float("nan"), 5)]],
                    type=pa.map_(key_type, pa.int32()),
                ),
            }
        )
    )
    entries = source.select(
        F.map_entries(
            F.map_zip_with(
                "left", "right", lambda _key, left, right: F.coalesce(left, F.lit(0)) + F.coalesce(right, F.lit(0))
            )
        )
    ).first()[0]
    assert [entry.value for entry in entries] == ([5, 7] if java_collections else [5, 2, 3, 5])
    assert math.copysign(1, entries[0].key) == (1 if java_collections else -1)
    assert all(math.isnan(entry.key) for entry in entries[1:])
