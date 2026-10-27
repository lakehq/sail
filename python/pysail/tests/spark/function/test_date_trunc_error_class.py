"""`date_trunc` / `trunc` reject a bad argument type with Spark's THROWABLE CLASS.

Spark raises `AnalysisException` while it analyzes the query. A `.feature` scenario cannot pin
this down: `Then query error` is `pytest.raises(Exception, match=...)`, which accepts any
exception whose message matches, so an `IllegalArgumentException` carrying the right text passes
it identically. Only asserting on the type catches a regression in the class.
"""

import pytest
from pyspark.errors import AnalysisException

# The base class both the classic and the Spark Connect clients derive their own from, so this
# holds whether the tests run against Spark JVM or against Sail.
DATATYPE_MISMATCH = [
    "SELECT date_trunc('DAY', 1) AS result",
    "SELECT date_trunc('INVALID', true) AS result",
    "SELECT date_trunc(array('YEAR'), TIMESTAMP '2024-01-01 00:00:00 UTC') AS result",
    "SELECT date_trunc('YEAR', array(1)) AS result",
    "SELECT trunc(1, 'YEAR') AS result",
    "SELECT trunc(array(1), 'YEAR') AS result",
]


@pytest.mark.date_trunc
@pytest.mark.trunc
@pytest.mark.parametrize("query", DATATYPE_MISMATCH)
def test_bad_argument_type_raises_analysis_exception(spark, query):
    with pytest.raises(AnalysisException) as error:
        spark.sql(query).collect()
    assert "DATATYPE_MISMATCH" in str(error.value)
