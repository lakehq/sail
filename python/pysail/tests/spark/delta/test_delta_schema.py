from datetime import UTC, date, datetime

import pandas as pd
import pytest
from pyspark.sql.types import Row


class TestDeltaSchema:
    """Delta Lake schema-related tests"""

    @pytest.mark.skip(reason="Temporarily skipped")
    def test_delta_schema_evolution(self, spark, tmp_path):
        """Test Delta Lake schema evolution"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"
        initial_data = [
            Row(id=1, name="Alice"),
            Row(id=2, name="Bob"),
        ]
        df1 = spark.createDataFrame(initial_data)

        df1.write.format("delta").mode("overwrite").save(str(delta_path))

        new_data = [
            Row(id=3, name="Charlie", age=30),
            Row(id=4, name="Diana", age=25),
        ]
        df2 = spark.createDataFrame(new_data)

        df2.write.format("delta").mode("append").option("mergeSchema", "true").save(str(delta_path))

        result_df = spark.read.format("delta").load(delta_table_path).sort("id")

        expected_data = pd.DataFrame(
            {"id": [1, 2, 3, 4], "name": ["Alice", "Bob", "Charlie", "Diana"], "age": [None, None, 30, 25]}
        ).astype({"id": "int32", "name": "string", "age": "Int32"})

        from pandas.testing import assert_frame_equal

        assert_frame_equal(
            result_df.toPandas(), expected_data.sort_values("id").reset_index(drop=True), check_dtype=False
        )

    def test_delta_with_different_data_types(self, spark, tmp_path):
        """Test Delta Lake support for different data types"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"
        complex_data = [
            Row(
                id=1,
                name="Alice",
                age=30,
                salary=50000.50,
                is_active=True,
                birth_date=date(1993, 5, 15),
                created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=UTC),
                tags=["python", "spark"],
                metadata={"department": "engineering", "level": "senior"},
            ),
            Row(
                id=2,
                name="Bob",
                age=25,
                salary=45000.75,
                is_active=False,
                birth_date=date(1998, 8, 22),
                created_at=datetime(2025, 2, 1, 14, 45, 0, tzinfo=UTC),
                tags=["java", "scala"],
                metadata={"department": "product", "level": "junior"},
            ),
        ]

        df = spark.createDataFrame(complex_data)

        df.write.format("delta").mode("overwrite").save(str(delta_path))

        result_df = spark.read.format("delta").load(delta_table_path).sort("id")

        result_pandas = result_df.toPandas()

        expected = 2
        assert len(result_pandas) == expected
        assert result_pandas["id"].tolist() == [1, 2]
        assert result_pandas["name"].tolist() == ["Alice", "Bob"]
        assert result_pandas["age"].tolist() == [30, 25]
        assert result_pandas["is_active"].tolist() == [True, False]

    def test_delta_query_with_custom_schema(self, spark, tmp_path):
        """Test with a custom schema and filter conditions."""
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        delta_path = tmp_path / "delta_custom_schema"
        delta_table_path = f"file://{delta_path}"

        # User-defined schema
        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
                StructField("score", IntegerType(), True),
            ]
        )

        data = [(1, "Alice", 90), (2, "Bob", None), (3, "Charlie", 85)]
        spark.createDataFrame(data, schema=schema).write.format("delta").mode("overwrite").save(str(delta_path))

        result_df = spark.read.format("delta").load(delta_table_path).filter("name IS NOT NULL AND score > 80")
        result = result_df.collect()
        assert len(result) == 2  # noqa: PLR2004
        assert {row.name for row in result} == {"Alice", "Charlie"}

        loaded_schema = result_df.schema
        assert loaded_schema == schema, f"Schema mismatch: expected {schema}, got {loaded_schema}"
