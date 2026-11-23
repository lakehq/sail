# SPDX-License-Identifier: Apache-2.0

import pytest
from pyspark.errors import AnalysisException


def test_list_relations_in_new_namespace(spark):
    with pytest.raises(AnalysisException) as exc:
        spark.sql("SHOW TABLE EXTENDED IN `new.db` LIKE '*'").collect()
    assert exc.value.getCondition() == "SCHEMA_NOT_FOUND"
    assert exc.value.getSqlState() == "42704"
    assert exc.value.getMessageParameters() == {"schemaName": "`sail`.`new.db`"}
