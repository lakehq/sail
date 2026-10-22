from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql import types as T  # noqa: N812
from pyspark.sql.types import Row


@pytest.mark.parametrize("session_timezone", ["Europe/Amsterdam"], indirect=True)
def test_convert_timezone_with_tz_aware_data(spark, session_timezone):
    _ = session_timezone

    schema = T.StructType(
        [
            T.StructField("id", T.StringType(), False),
            T.StructField("ts", T.TimestampType(), True),
        ]
    )
    df = spark.createDataFrame(
        [
            ("a", datetime(2023, 1, 1, 10, tzinfo=ZoneInfo("Europe/Amsterdam"))),
            ("b", datetime(2023, 1, 1, 10, tzinfo=ZoneInfo("Asia/Shanghai"))),
            ("c", datetime(2023, 1, 1, 10, tzinfo=ZoneInfo("America/Los_Angeles"))),
        ],
        schema=schema,
    )

    actual = (
        df.select(
            F.col("id"),
            F.date_format(
                F.convert_timezone(
                    F.lit("Europe/Amsterdam"),
                    F.lit("UTC"),
                    F.to_timestamp_ntz("ts"),
                ),
                "yyyy-MM-dd HH",
            ).alias("x"),
        )
        .orderBy("id")
        .collect()
    )

    expected = [
        Row(id="a", x="2023-01-01 09"),
        Row(id="b", x="2023-01-01 02"),
        Row(id="c", x="2023-01-01 18"),
    ]

    assert actual == expected
