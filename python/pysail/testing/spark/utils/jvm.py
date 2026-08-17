from __future__ import annotations

import contextlib
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator


# Delta 4.1 embeds the Spark minor in its artifact name, while earlier releases
# use the Scala binary version only.
DELTA_SPARK_COORDINATES: dict[str, str] = {
    "3.5": "io.delta:delta-spark_2.12:3.3.2",
    "4.0": "io.delta:delta-spark_2.13:4.0.1",
    "4.1": "io.delta:delta-spark_4.1_2.13:4.3.1",
}


def delta_spark_maven_coordinate(spark_version: str) -> str:
    spark_minor = ".".join(spark_version.split(".")[:2])
    try:
        return DELTA_SPARK_COORDINATES[spark_minor]
    except KeyError as error:
        message = f"No Delta Maven coordinate mapping for Spark {spark_version}"
        raise RuntimeError(message) from error


@contextlib.contextmanager
def classic_spark_mode() -> Generator[None, None, None]:
    old_api_mode = os.environ.get("SPARK_API_MODE")
    old_remote = os.environ.pop("SPARK_REMOTE", None)
    old_connect_mode = os.environ.pop("SPARK_CONNECT_MODE_ENABLED", None)
    os.environ["SPARK_API_MODE"] = "classic"
    try:
        yield
    finally:
        if old_api_mode is None:
            os.environ.pop("SPARK_API_MODE", None)
        else:
            os.environ["SPARK_API_MODE"] = old_api_mode
        if old_remote is not None:
            os.environ["SPARK_REMOTE"] = old_remote
        if old_connect_mode is not None:
            os.environ["SPARK_CONNECT_MODE_ENABLED"] = old_connect_mode
