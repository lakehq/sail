"""Tests for colRegex column selection functionality."""


class TestColRegex:
    """Tests for DataFrame.colRegex() method."""

    def test_select_columns_not_starting_with_col1(self, spark):
        """Select columns using regex pattern that excludes Col1."""
        df = spark.createDataFrame(
            [("a", 1), ("b", 2), ("c", 3)],
            ["Col1", "Col2"],
        )
        result = df.select(df.colRegex("`Col[^1]`"))

        assert result.columns == ["Col2"]
        assert result.collect() == [(1,), (2,), (3,)]

    def test_select_columns_starting_with_col(self, spark):
        """Select columns starting with 'col' pattern."""
        df = spark.createDataFrame(
            [(1, 2, 3, 4)],
            ["col1", "col2", "col3", "other"],
        )
        result = df.select(df.colRegex("`col.*`"))

        assert result.columns == ["col1", "col2", "col3"]
        assert result.collect() == [(1, 2, 3)]

    def test_select_columns_ending_with_name(self, spark):
        """Select columns ending with 'name' pattern."""
        df = spark.createDataFrame(
            [("Alice", "Smith", 25), ("Bob", "Jones", 30)],
            ["firstname", "lastname", "age"],
        )
        result = df.select(df.colRegex("`.*name$`"))

        assert result.columns == ["firstname", "lastname"]
        rows = result.collect()
        assert rows[0] == ("Alice", "Smith")
        assert rows[1] == ("Bob", "Jones")

    def test_select_single_column(self, spark):
        """Select a single column using exact match regex."""
        df = spark.createDataFrame(
            [("Alice", 25)],
            ["name", "age"],
        )
        result = df.select(df.colRegex("`age`"))

        assert result.columns == ["age"]
        assert result.collect() == [(25,)]

    def test_select_columns_with_numeric_pattern(self, spark):
        """Select columns matching numeric pattern."""
        df = spark.createDataFrame(
            [(1, 2, 3, 4)],
            ["col1", "col2", "col10", "column"],
        )
        result = df.select(df.colRegex("`col[0-9]+`"))

        assert result.columns == ["col1", "col2", "col10"]
        assert result.collect() == [(1, 2, 3)]

    def test_select_all_columns_wildcard(self, spark):
        """Select all columns using wildcard regex."""
        df = spark.createDataFrame(
            [(1, 2, 3)],
            ["a", "b", "c"],
        )
        result = df.select(df.colRegex("`.*`"))

        assert result.columns == ["a", "b", "c"]
        assert result.collect() == [(1, 2, 3)]

    def test_no_columns_match_returns_empty(self, spark):
        """When no columns match, return DataFrame with no columns."""
        df = spark.createDataFrame(
            [(1, 2, 3)],
            ["col1", "col2", "col3"],
        )
        result = df.select(df.colRegex("`nonexistent.*`"))

        assert len(result.columns) == 0

    def test_no_columns_match_preserves_row_count(self, spark):
        """When no columns match, verify row count is preserved."""
        data = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        df = spark.createDataFrame(data, ["col1", "col2", "col3"])
        result = df.select(df.colRegex("`nonexistent.*`"))

        assert len(result.columns) == 0
        assert result.count() == len(data)
