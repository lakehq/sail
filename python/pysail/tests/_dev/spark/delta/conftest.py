from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pyspark
import pytest
from pyspark.sql import SparkSession

from pysail.testing._dev.spark.utils.jvm import classic_spark_mode, delta_spark_maven_coordinate

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    root = Path(__file__).parent
    for item in items:
        if item.path.is_relative_to(root):
            item.add_marker(pytest.mark.spark(skip_if_jvm=True, reason="Tests Sail's Delta Lake integration"))
            if "delta_jvm_spark" in getattr(item, "fixturenames", ()):
                item.add_marker(
                    pytest.mark.spark(version="<4.3", reason="Delta JVM artifacts are not yet published for Spark 4.3")
                )


@pytest.fixture(scope="session")
def delta_jvm_spark(tmp_path_factory: pytest.TempPathFactory) -> Generator[SparkSession, None, None]:
    warehouse_dir = tmp_path_factory.mktemp("delta-jvm-warehouse")
    delta_package = delta_spark_maven_coordinate(pyspark.__version__)
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
