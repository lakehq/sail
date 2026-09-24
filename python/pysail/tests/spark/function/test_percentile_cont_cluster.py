import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.fixture(scope="module")
def remote():
    envs = None if is_jvm_spark() else {"SAIL_MODE": "local-cluster"}
    with spark_connect_server(envs=envs) as server:
        yield server.remote


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug: cluster percentile loses values", strict=True)
def test_percentile_cont_float_case_in_cluster(spark):
    spark.conf.set("spark.sql.ansi.enabled", "false")
    df = spark.sql(
        "SELECT percentile_cont(0.5) WITHIN GROUP "
        "(ORDER BY CASE WHEN id = 0 THEN 1 ELSE CAST(2.5 AS FLOAT) END) AS result "
        "FROM range(3)"
    )
    assert df.collect() == [(2.5,)]
