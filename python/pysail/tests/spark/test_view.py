import pandas as pd
import pytest
from pandas.testing import assert_frame_equal


@pytest.fixture(scope="module", autouse=True)
def base_table(spark):
    spark.sql("CREATE TABLE view_test_t (id INT, name STRING) USING parquet")
    spark.sql("INSERT INTO view_test_t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
    yield
    spark.sql("DROP VIEW IF EXISTS view_test_v")
    spark.sql("DROP VIEW IF EXISTS view_test_v_all")
    spark.sql("DROP TABLE view_test_t")


class TestPersistentView:
    """Tests for CREATE VIEW and SELECT from persistent views."""

    def test_create_and_read_view(self, spark):
        spark.sql("CREATE VIEW view_test_v AS SELECT * FROM view_test_t WHERE id = 1")
        actual = spark.sql("SELECT * FROM view_test_v").toPandas()
        expected = pd.DataFrame(
            {"id": [1], "name": ["Alice"]},
        ).astype({"id": "int32", "name": "str"})
        assert_frame_equal(actual, expected)

    def test_read_view_all_rows(self, spark):
        spark.sql("CREATE VIEW view_test_v_all AS SELECT * FROM view_test_t")
        actual = spark.sql("SELECT * FROM view_test_v_all").toPandas()
        expected = pd.DataFrame(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"]},
        ).astype({"id": "int32", "name": "str"})
        assert_frame_equal(actual, expected)


class TestTemporaryView:
    """Temporary views work — these serve as a baseline to confirm the test setup is valid."""

    def test_create_and_read_temp_view(self, spark):
        spark.sql("CREATE TEMPORARY VIEW view_test_tmp AS SELECT * FROM view_test_t WHERE id = 2")
        actual = spark.sql("SELECT * FROM view_test_tmp").toPandas()
        expected = pd.DataFrame(
            {"id": [2], "name": ["Bob"]},
        ).astype({"id": "int32", "name": "str"})
        assert_frame_equal(actual, expected)
