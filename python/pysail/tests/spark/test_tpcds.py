from pathlib import Path

import duckdb
import pytest
from pandas.testing import assert_frame_equal

from pysail.tests.spark.tpc_common import is_ddl, normalize, read_sql_queries, run_duck_query

from .utils import is_jvm_spark, to_pandas  # noqa: TID252


@pytest.fixture(scope="module")
def duck():
    conn = duckdb.connect()
    conn.sql("CALL dsdgen(sf = 0.01)")
    return conn


@pytest.fixture(scope="module", autouse=True)
def data(spark, duck):
    tables = list(duck.sql("SHOW TABLES").df()["name"])
    for table in tables:
        df = duck.sql(f"SELECT * FROM {table}").fetch_arrow_table().to_pandas()  # noqa: S608
        spark.createDataFrame(df).createOrReplaceTempView(table)
    yield
    for table in tables:
        spark.catalog.dropTempView(table)


@pytest.mark.parametrize("query", [f"q{x + 1}" for x in range(99)])
@pytest.mark.skipif(is_jvm_spark(), reason="slow tests in JVM Spark")
def test_derived_tpcds_query_execution(spark, duck, query):
    for sql in read_sql(query):
        if is_ddl(sql):
            duck.sql(sql)
            spark.sql(sql)
            continue

        duck_df = normalize(run_duck_query(duck, sql))
        spark_df = normalize(to_pandas(spark.sql(sql)))
        assert_frame_equal(
            duck_df,
            spark_df,
            check_dtype=False,
            check_exact=False,
            rtol=1e-4,
            atol=1e-8,
            check_names=False,
        )


def read_sql(query):
    base_dir = Path(__file__).parent.parent.parent / "data" / "tpcds" / "queries"
    yield from read_sql_queries(base_dir, query)
