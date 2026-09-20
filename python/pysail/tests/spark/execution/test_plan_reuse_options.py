import pytest
from pyspark.sql import Row

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = [
    pytest.mark.skipif(is_jvm_spark(), reason="Sail physical subplan reuse"),
    pytest.mark.timeout(30),
]


@pytest.mark.parametrize("mode", ["local", "local-cluster"])
def test_shared_limits_stop_without_spilling(mode):
    envs = {
        "SAIL_MODE": mode,
        "SAIL_OPTIMIZER__ENABLE_PLAN_REUSE": "true",
        "SAIL_RUNTIME__TEMPORARY_FILES__MAX_SIZE": "0",
    }
    with spark_connect_server(envs=envs) as server, spark_session_factory(server.remote) as sessions:
        spark = sessions.create()
        df = spark.sql("""
            WITH t AS (SELECT id FROM range(0, 10000000, 1, 1))
            SELECT id FROM (SELECT id FROM t LIMIT 1)
            UNION ALL SELECT id FROM (SELECT id FROM t LIMIT 1)
        """)
        assert "SharedPlanExec" not in df._explain_string()  # noqa: SLF001
        assert df.collect() == [Row(id=0), Row(id=0)]


def test_plan_reuse_is_disabled_by_default():
    with spark_connect_server() as server, spark_session_factory(server.remote) as sessions:
        spark = sessions.create()
        source = spark.range(10)
        df = source.unionAll(source)
        assert "SharedPlanExec" not in df._explain_string()  # noqa: SLF001
        assert df.count() == 20  # noqa: PLR2004
