"""Tests for IDENTIFIER clause with parameterized SQL (named SQL parameters)."""

import pytest

from pysail.testing.spark.utils.common import pyspark_version

# Named SQL parameters via args=dict were introduced in Spark 3.4 / Spark Connect.
pytestmark = pytest.mark.skipif(
    pyspark_version() < (3, 4),
    reason="spark.sql() named SQL parameters require Spark 3.4+",
)


class TestIdentifierClauseWithVariables:
    """Tests for IDENTIFIER clause where the identifier name is a named SQL parameter."""

    def test_identifier_variable_column_in_select(self, spark):
        spark.sql(
            "CREATE OR REPLACE TEMPORARY VIEW t_id_var_select AS SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)"
        )
        result = spark.sql(
            "SELECT IDENTIFIER(:col) FROM t_id_var_select ORDER BY id",
            args={"col": "id"},
        ).collect()
        assert result == [(1,), (2,)]

    def test_identifier_variable_column_in_where(self, spark):
        spark.sql(
            "CREATE OR REPLACE TEMPORARY VIEW t_id_var_where AS SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)"
        )
        result = spark.sql(
            "SELECT id FROM t_id_var_where WHERE IDENTIFIER(:col) > 1",
            args={"col": "id"},
        ).collect()
        assert result == [(2,)]

    def test_identifier_variable_table_in_from(self, spark):
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW t_id_var_from AS SELECT * FROM VALUES (10), (20) AS t(val)")
        result = spark.sql(
            "SELECT * FROM IDENTIFIER(:tab) ORDER BY val",
            args={"tab": "t_id_var_from"},
        ).collect()
        assert result == [(10,), (20,)]

    def test_identifier_variable_constant_folding(self, spark):
        spark.sql(
            "CREATE OR REPLACE TEMPORARY VIEW t_id_fold AS SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)"
        )
        result = spark.sql(
            "SELECT IDENTIFIER(:tab || '.' || :col) FROM t_id_fold ORDER BY id",
            args={"tab": "t_id_fold", "col": "id"},
        ).collect()
        assert result == [(1,), (2,)]
