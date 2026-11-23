# SPDX-License-Identifier: Apache-2.0

import pytest
from pyspark.errors import AnalysisException


@pytest.mark.parametrize("command", ["SHOW TABLE EXTENDED IN dbt_missing LIKE '*'", "SHOW VIEWS IN dbt_missing"])
def test_missing_schema(spark, command):
    with pytest.raises(AnalysisException) as exc:
        spark.sql(command).collect()
    assert exc.value.getCondition() == "SCHEMA_NOT_FOUND"
    assert exc.value.getSqlState() == "42704"
    assert exc.value.getMessageParameters() == {"schemaName": "`sail`.`dbt_missing`"}
