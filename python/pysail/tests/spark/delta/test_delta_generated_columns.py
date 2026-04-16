from datetime import date

from pyspark.sql import SparkSession

from pysail.testing.spark.utils.sql import escape_sql_string_literal


class TestDeltaGeneratedColumns:
    """Test Delta Lake generated columns via SQL and DataFrame API."""

    def test_generated_column_cast_as_date(self, spark: SparkSession, tmp_path):
        """Test GENERATED ALWAYS AS (CAST(col AS DATE)) via SQL."""
        delta_path = str(tmp_path / "gen_col_date")
        location = escape_sql_string_literal(delta_path)
        table_name = "gen_col_date_t"
        spark.sql(f"""
            CREATE TABLE {table_name} (
                id INT,
                event_time TIMESTAMP,
                event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
            )
            USING DELTA
            LOCATION '{location}'
        """)
        try:
            spark.sql(
                f"INSERT INTO {table_name} (id, event_time) VALUES (1, TIMESTAMP '2024-10-15 10:30:00'), (2, TIMESTAMP '2024-10-16 14:00:00')"  # noqa: S608
            )
            result = spark.read.format("delta").load(delta_path).sort("id").toPandas()
            assert list(result.columns) == ["id", "event_time", "event_date"]
            assert list(result["id"]) == [1, 2]
            assert list(result["event_date"]) == [date(2024, 10, 15), date(2024, 10, 16)]
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_generated_column_year_extraction(self, spark: SparkSession, tmp_path):
        """Test GENERATED ALWAYS AS (YEAR(col)) via SQL."""
        delta_path = str(tmp_path / "gen_col_year")
        location = escape_sql_string_literal(delta_path)
        table_name = "gen_col_year_t"
        spark.sql(f"""
            CREATE TABLE {table_name} (
                id INT,
                event_time TIMESTAMP,
                event_year INT GENERATED ALWAYS AS (YEAR(event_time))
            )
            USING DELTA
            LOCATION '{location}'
        """)
        try:
            spark.sql(
                f"INSERT INTO {table_name} (id, event_time) VALUES (1, TIMESTAMP '2024-10-15 10:30:00'), (2, TIMESTAMP '2023-05-20 08:00:00')"  # noqa: S608
            )
            result = spark.read.format("delta").load(delta_path).sort("id").toPandas()
            assert list(result["event_year"]) == [2024, 2023]
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_generated_column_read_back(self, spark: SparkSession, tmp_path):
        """Verify data round-trips correctly with generated columns."""
        delta_path = str(tmp_path / "gen_col_roundtrip")
        location = escape_sql_string_literal(delta_path)
        table_name = "gen_col_roundtrip_t"
        spark.sql(f"""
            CREATE TABLE {table_name} (
                id INT,
                ts TIMESTAMP,
                dt DATE GENERATED ALWAYS AS (CAST(ts AS DATE))
            )
            USING DELTA
            LOCATION '{location}'
        """)
        try:
            spark.sql(
                f"INSERT INTO {table_name} (id, ts) VALUES (1, TIMESTAMP '2024-01-01 00:00:00')"  # noqa: S608
            )
            df = spark.read.format("delta").load(delta_path)
            rows = df.collect()
            assert len(rows) == 1
            assert rows[0]["id"] == 1
            assert rows[0]["dt"] == date(2024, 1, 1)
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_generated_column_with_explicit_values(self, spark: SparkSession, tmp_path):
        """Test that explicit correct values are accepted for generated columns."""
        delta_path = str(tmp_path / "gen_col_explicit")
        location = escape_sql_string_literal(delta_path)
        table_name = "gen_col_explicit_t"
        spark.sql(f"""
            CREATE TABLE {table_name} (
                id INT,
                event_time TIMESTAMP,
                event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
            )
            USING DELTA
            LOCATION '{location}'
        """)
        try:
            spark.sql(
                f"INSERT INTO {table_name} VALUES (1, TIMESTAMP '2024-10-15 10:30:00', DATE '2024-10-15')"  # noqa: S608
            )
            result = spark.read.format("delta").load(delta_path).sort("id").toPandas()
            assert list(result["id"]) == [1]
            assert list(result["event_date"]) == [date(2024, 10, 15)]
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_generated_column_multiple_inserts(self, spark: SparkSession, tmp_path):
        """Test multiple inserts with generated columns."""
        delta_path = str(tmp_path / "gen_col_multi_insert")
        location = escape_sql_string_literal(delta_path)
        table_name = "gen_col_multi_t"
        spark.sql(f"""
            CREATE TABLE {table_name} (
                id INT,
                event_time TIMESTAMP,
                event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
            )
            USING DELTA
            LOCATION '{location}'
        """)
        try:
            spark.sql(
                f"INSERT INTO {table_name} (id, event_time) VALUES (1, TIMESTAMP '2024-10-15 10:30:00')"  # noqa: S608
            )
            spark.sql(
                f"INSERT INTO {table_name} (id, event_time) VALUES (2, TIMESTAMP '2024-10-16 14:00:00')"  # noqa: S608
            )
            result = spark.read.format("delta").load(delta_path).sort("id").toPandas()
            expected_count = 2
            assert len(result) == expected_count
            assert list(result["event_date"]) == [date(2024, 10, 15), date(2024, 10, 16)]
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
