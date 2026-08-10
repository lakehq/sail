from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING
from urllib.request import urlopen

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql.types import Row

from pysail.testing.spark.session import spark_session_factory

if TYPE_CHECKING:
    from pysail.tests.spark.celeborn.conftest import MasterService

_SLEEP_SECONDS = 5


def _master_shuffle_ids(master: MasterService) -> list[str]:
    with urlopen(f"http://{master.host}:{master.http_port}/api/v1/shuffles", timeout=5) as response:  # noqa: S310
        return json.load(response)["shuffleIds"]


def _application_shuffle_ids(master: MasterService, session_id: str) -> list[str]:
    prefix = f"sail-session-{session_id}-"
    return [shuffle_id for shuffle_id in _master_shuffle_ids(master) if shuffle_id.startswith(prefix)]


def test_repartition_collect_with_celeborn_shuffle(remote):
    with spark_session_factory(remote) as sessions:
        spark = sessions.create()
        rows = (
            spark.createDataFrame([Row(id=i, group=i % 3) for i in range(30)])
            .repartition(6, "group")
            .groupBy("group")
            .count()
            .orderBy("group")
            .collect()
        )

    assert rows == [
        Row(group=0, count=10),
        Row(group=1, count=10),
        Row(group=2, count=10),
    ]


def test_consumed_celeborn_shuffle_data_is_removed(spark, celeborn_master: MasterService):
    @F.udf("long")
    def identity(value):
        time.sleep(_SLEEP_SECONDS)
        return value

    session_id = spark.session_id
    assert _application_shuffle_ids(celeborn_master, session_id) == []

    with ThreadPoolExecutor(max_workers=1) as executor:
        result = executor.submit(
            lambda: spark.range(2)
            .repartition(2)
            .groupBy()
            .count()
            .select(identity("count").alias("count"))
            .collect()
        )
        deadline = time.monotonic() + _SLEEP_SECONDS
        while not (shuffle_ids := _application_shuffle_ids(celeborn_master, session_id)):
            if result.done() or time.monotonic() >= deadline:
                break
            time.sleep(0.05)

        assert shuffle_ids, "the shuffle was never registered with the Celeborn master"
        assert result.result() == [Row(count=2)]

    assert _application_shuffle_ids(celeborn_master, session_id) == []
