from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark
import pytest
from pyspark.sql import SparkSession

from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.testing.spark.utils.jvm import classic_spark_mode, delta_spark_maven_coordinate

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture(scope="session", autouse=True)
def skip_if_jvm_spark():
    if is_jvm_spark():
        pytest.skip("Delta Lake tests for JVM Spark")


@pytest.fixture(scope="session")
def delta_jvm_spark(tmp_path_factory: pytest.TempPathFactory) -> Generator[SparkSession, None, None]:
    warehouse_dir = tmp_path_factory.mktemp("delta-jvm-warehouse")
    try:
        delta_package = delta_spark_maven_coordinate(pyspark.__version__)
    except RuntimeError as error:
        pytest.skip(str(error))
    with classic_spark_mode():
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("delta-column-mapping-interop")
            .config("spark.jars.packages", delta_package)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.warehouse.dir", str(warehouse_dir))
            .getOrCreate()
        )
        spark.conf.set("spark.sql.session.timeZone", "UTC")

    yield spark

    spark.stop()
