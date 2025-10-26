"""Unit tests for JDBC options normalization and validation."""

import pytest

from pysail.jdbc.exceptions import InvalidOptionsError
from pysail.jdbc.jdbc_options import NormalizedJDBCOptions, normalize_session_init


class TestNormalizedJDBCOptions:
    """Test JDBC options normalization and validation."""

    def test_basic_options(self):
        """Test basic option normalization."""
        options = {
            "url": "jdbc:postgresql://localhost:5432/mydb",
            "dbtable": "orders",
        }

        opts = NormalizedJDBCOptions.from_spark_options(options)
        opts.validate()

        assert opts.url == "jdbc:postgresql://localhost:5432/mydb"
        assert opts.dbtable == "orders"
        assert opts.query is None
        assert opts.engine == "connectorx"  # default
        assert opts.num_partitions == 1  # default

    def test_case_insensitive_options(self):
        """Test that options are case-insensitive."""
        password = "secret"  # noqa: S105
        options = {
            "URL": "jdbc:postgresql://localhost:5432/mydb",
            "DbTable": "orders",
            "User": "admin",
            "Password": password,
            "Engine": "ADBC",
        }

        opts = NormalizedJDBCOptions.from_spark_options(options)

        assert opts.url == "jdbc:postgresql://localhost:5432/mydb"
        assert opts.dbtable == "orders"
        assert opts.user == "admin"
        assert opts.password == password
        assert opts.engine == "adbc"

    def test_missing_url_raises_error(self):
        """Test that missing URL raises error."""
        options = {
            "dbtable": "orders",
        }

        with pytest.raises(InvalidOptionsError, match="'url' option is required"):
            NormalizedJDBCOptions.from_spark_options(options)

    def test_missing_dbtable_and_query_raises_error(self):
        """Test that missing both dbtable and query raises error."""
        options = {
            "url": "jdbc:postgresql://localhost:5432/mydb",
        }

        with pytest.raises(
            InvalidOptionsError,
            match="Either 'dbtable' or 'query' option must be specified",
        ):
            NormalizedJDBCOptions.from_spark_options(options)

    def test_both_dbtable_and_query_raises_error(self):
        """Test that specifying both dbtable and query raises error."""
        options = {
            "url": "jdbc:postgresql://localhost:5432/mydb",
            "dbtable": "orders",
            "query": "SELECT * FROM orders",
        }

        with pytest.raises(InvalidOptionsError, match="Cannot specify both 'dbtable' and 'query'"):
            NormalizedJDBCOptions.from_spark_options(options)

    def test_partition_options(self):
        """Test partition option normalization."""
        lower_bound = 1
        upper_bound = 1_000
        partitions = 10
        options = {
            "url": "jdbc:postgresql://localhost:5432/mydb",
            "dbtable": "orders",
            "partitionColumn": "order_id",
            "lowerBound": str(lower_bound),
            "upperBound": str(upper_bound),
            "numPartitions": str(partitions),
        }

        opts = NormalizedJDBCOptions.from_spark_options(options)
        opts.validate()

        assert opts.partition_column == "order_id"
        assert opts.lower_bound == lower_bound
        assert opts.upper_bound == upper_bound
        assert opts.num_partitions == partitions

    def test_partition_column_without_bounds_fails_validation(self):
        """Test that partitionColumn without bounds fails validation."""
        options = {
            "url": "jdbc:postgresql://localhost:5432/mydb",
            "dbtable": "orders",
            "partitionColumn": "order_id",
        }

        opts = NormalizedJDBCOptions.from_spark_options(options)

        with pytest.raises(
            InvalidOptionsError,
            match="partitionColumn requires lowerBound and upperBound",
        ):
            opts.validate()

    def test_invalid_bounds_fails_validation(self):
        """Test that invalid bounds (lower >= upper) fails validation."""
        options = {
            "url": "jdbc:postgresql://localhost:5432/mydb",
            "dbtable": "orders",
            "partitionColumn": "order_id",
            "lowerBound": str(1_000),
            "upperBound": str(100),
            "numPartitions": str(10),
        }

        opts = NormalizedJDBCOptions.from_spark_options(options)

        with pytest.raises(InvalidOptionsError, match="lowerBound .* must be less than upperBound"):
            opts.validate()

    def test_explicit_predicates(self):
        """Test explicit predicates option."""
        options = {
            "url": "jdbc:postgresql://localhost:5432/mydb",
            "dbtable": "orders",
            "predicates": "status='active',status='pending',status='completed'",
        }

        opts = NormalizedJDBCOptions.from_spark_options(options)
        opts.validate()

        assert opts.predicates == [
            "status='active'",
            "status='pending'",
            "status='completed'",
        ]

    def test_predicates_with_partition_column_fails_validation(self):
        """Test that predicates with partitionColumn fails validation."""
        options = {
            "url": "jdbc:postgresql://localhost:5432/mydb",
            "dbtable": "orders",
            "predicates": "status='active',status='pending'",
            "partitionColumn": "order_id",
            "lowerBound": "1",
            "upperBound": "1000",
        }

        opts = NormalizedJDBCOptions.from_spark_options(options)

        with pytest.raises(
            InvalidOptionsError,
            match="Cannot specify both predicates list and partitionColumn",
        ):
            opts.validate()

    def test_invalid_engine_raises_error(self):
        """Test that invalid engine raises error."""
        options = {
            "url": "jdbc:postgresql://localhost:5432/mydb",
            "dbtable": "orders",
            "engine": "invalid_engine",
        }

        with pytest.raises(InvalidOptionsError, match="Invalid engine: invalid_engine"):
            NormalizedJDBCOptions.from_spark_options(options)

    def test_fetch_size_option(self):
        """Test fetch size option normalization."""
        options = {
            "url": "jdbc:postgresql://localhost:5432/mydb",
            "dbtable": "orders",
            "fetchsize": str(5_000),
        }

        opts = NormalizedJDBCOptions.from_spark_options(options)
        assert opts.fetch_size == int(options["fetchsize"])

    def test_invalid_fetch_size_raises_error(self):
        """Test that invalid fetch size raises error."""
        options = {
            "url": "jdbc:postgresql://localhost:5432/mydb",
            "dbtable": "orders",
            "fetchsize": "not_a_number",
        }

        with pytest.raises(InvalidOptionsError, match="fetchsize must be an integer"):
            NormalizedJDBCOptions.from_spark_options(options)

    def test_invalid_num_partitions_raises_error(self):
        """Test that invalid numPartitions raises error."""
        options = {
            "url": "jdbc:postgresql://localhost:5432/mydb",
            "dbtable": "orders",
            "numPartitions": "not_a_number",
        }

        with pytest.raises(InvalidOptionsError, match="numPartitions must be an integer"):
            NormalizedJDBCOptions.from_spark_options(options)

    def test_zero_num_partitions_fails_validation(self):
        """Test that numPartitions = 0 fails validation."""
        options = {
            "url": "jdbc:postgresql://localhost:5432/mydb",
            "dbtable": "orders",
            "numPartitions": "0",
        }

        opts = NormalizedJDBCOptions.from_spark_options(options)

        with pytest.raises(InvalidOptionsError, match="numPartitions must be >= 1"):
            opts.validate()


class TestSessionInitNormalization:
    """Test session initialization statement normalization."""

    def test_allow_set_statement(self):
        """Test that SET statements are allowed."""
        stmt = "SET timezone='UTC'"
        result = normalize_session_init(stmt)
        assert result == stmt

    def test_allow_create_temporary(self):
        """Test that CREATE TEMPORARY is allowed."""
        stmt = "CREATE TEMPORARY TABLE temp AS SELECT 1"
        result = normalize_session_init(stmt)
        assert result == stmt

    def test_reject_insert_statement(self):
        """Test that INSERT statements are rejected."""
        stmt = "INSERT INTO table VALUES (1, 2, 3)"

        with pytest.raises(InvalidOptionsError, match="sessionInitStatement cannot start with INSERT"):
            normalize_session_init(stmt)

    def test_reject_update_statement(self):
        """Test that UPDATE statements are rejected."""
        stmt = "UPDATE table SET col=1"

        with pytest.raises(InvalidOptionsError, match="sessionInitStatement cannot start with UPDATE"):
            normalize_session_init(stmt)

    def test_reject_delete_statement(self):
        """Test that DELETE statements are rejected."""
        stmt = "DELETE FROM table"

        with pytest.raises(InvalidOptionsError, match="sessionInitStatement cannot start with DELETE"):
            normalize_session_init(stmt)

    def test_reject_drop_statement(self):
        """Test that DROP statements are rejected."""
        stmt = "DROP TABLE table"

        with pytest.raises(InvalidOptionsError, match="sessionInitStatement cannot start with DROP"):
            normalize_session_init(stmt)

    def test_none_returns_none(self):
        """Test that None input returns None."""
        result = normalize_session_init(None)
        assert result is None

    def test_case_insensitive_rejection(self):
        """Test that dangerous keywords are rejected case-insensitively."""
        stmt = "insert into table values (1)"

        with pytest.raises(InvalidOptionsError, match="sessionInitStatement cannot start with INSERT"):
            normalize_session_init(stmt)
