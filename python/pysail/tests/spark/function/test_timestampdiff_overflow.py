import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql.types import Row

I64_MIN = -(2**63)
I64_MAX = 2**63 - 1


@pytest.fixture(scope="module")
def ntz_extremes(spark, tmp_path_factory):
    """TIMESTAMP_NTZ values at the ends of the microsecond range, one view per value, so that
    each query reads a single row."""
    directory = tmp_path_factory.mktemp("timestampdiff_overflow")
    views = {"ntz_min": I64_MIN, "ntz_zero": 0, "ntz_max": I64_MAX}
    for name, micros in views.items():
        path = directory / f"{name}.parquet"
        pq.write_table(pa.table({"ntz": pa.array([micros], pa.timestamp("us"))}), path)
        spark.read.parquet(str(path)).createOrReplaceTempView(name)
    yield
    for name in views:
        spark.catalog.dropTempView(name)


def timestampdiff_days(spark, view):
    return spark.sql(f"SELECT timestampdiff(DAY, ntz, TIMESTAMP '2020-06-14 00:00:00') AS days FROM {view}").collect()


@pytest.mark.parametrize(
    ("session_timezone", "view"),
    [
        ("America/Los_Angeles", "ntz_max"),
        ("-08:00", "ntz_max"),
        ("+08:00", "ntz_min"),
    ],
    indirect=["session_timezone"],
)
@pytest.mark.usefixtures("ntz_extremes", "session_timezone")
def test_timestampdiff_timestamp_ntz_cast_overflow(spark, view):
    # Spark casts TIMESTAMP_NTZ to TIMESTAMP when the other value is one, which fails with
    # long overflow when the wall-clock time in the session time zone is beyond the range.
    with pytest.raises(Exception, match="long overflow"):
        timestampdiff_days(spark, view)


@pytest.mark.parametrize(
    ("session_timezone", "view", "days"),
    [
        ("America/Los_Angeles", "ntz_zero", 18427),
        ("America/Los_Angeles", "ntz_min", 106770418),
        ("-08:00", "ntz_min", 106770418),
        ("+08:00", "ntz_max", -106733564),
        ("UTC", "ntz_max", -106733564),
    ],
    indirect=["session_timezone"],
)
@pytest.mark.usefixtures("ntz_extremes", "session_timezone")
def test_timestampdiff_timestamp_ntz_cast_in_range(spark, view, days):
    assert timestampdiff_days(spark, view) == [Row(days=days)]
