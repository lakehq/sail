# SPDX-License-Identifier: Apache-2.0

import pytest
from pyspark.errors import AnalysisException


def test_missing_schema(spark):
    command = "SHOW TABLE EXTENDED IN dbt_missing LIKE '*'"
    with pytest.raises(AnalysisException) as exc:
        spark.sql(command).collect()
    assert exc.value.getCondition() == "SCHEMA_NOT_FOUND"
    assert exc.value.getSqlState() == "42704"
    assert exc.value.getMessageParameters() == {"schemaName": "`sail`.`dbt_missing`"}
