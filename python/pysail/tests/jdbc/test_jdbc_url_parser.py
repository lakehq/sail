"""Unit tests for JDBC URL parser."""

import pytest

from pysail.jdbc.exceptions import InvalidJDBCUrlError
from pysail.jdbc.jdbc_url_parser import parse_jdbc_url


class TestJDBCUrlParser:
    """Test JDBC URL parsing."""

    def test_parse_postgresql_url(self):
        """Test parsing PostgreSQL JDBC URL."""
        url = "jdbc:postgresql://localhost:5432/mydb"
        result = parse_jdbc_url(url)

        assert result.driver == "postgresql"
        assert result.connection_string == "postgresql://localhost:5432/mydb"
        assert result.user is None
        assert result.password is None

    def test_parse_postgresql_url_with_credentials(self):
        """Test parsing PostgreSQL JDBC URL with embedded credentials."""
        embedded_user = "user"
        embedded_password = "pass"  # noqa: S105
        url = f"jdbc:postgresql://{embedded_user}:{embedded_password}@localhost:5432/mydb"
        result = parse_jdbc_url(url)

        assert result.driver == "postgresql"
        assert f"{embedded_user}:{embedded_password}" in result.connection_string
        assert result.user == embedded_user
        assert result.password == embedded_password

    def test_parse_mysql_url(self):
        """Test parsing MySQL JDBC URL."""
        url = "jdbc:mysql://localhost:3306/mydb"
        result = parse_jdbc_url(url)

        assert result.driver == "mysql"
        assert result.connection_string == "mysql://localhost:3306/mydb"

    def test_parse_sqlite_url(self):
        """Test parsing SQLite JDBC URL."""
        url = "jdbc:sqlite:/path/to/database.db"
        result = parse_jdbc_url(url)

        assert result.driver == "sqlite"
        assert "sqlite" in result.connection_string

    def test_parse_sqlserver_url(self):
        """Test parsing SQL Server JDBC URL (non-standard format)."""
        url = "jdbc:sqlserver://localhost:1433;database=mydb"
        result = parse_jdbc_url(url)

        assert result.driver == "sqlserver"
        assert "sqlserver" in result.connection_string

    def test_parse_url_with_credential_override(self):
        """Test parsing URL with credential override from options."""
        url = "jdbc:postgresql://localhost:5432/mydb"
        override_user = "admin"
        override_password = "secret"  # noqa: S105
        result = parse_jdbc_url(url, user=override_user, password=override_password)

        assert result.driver == "postgresql"
        assert f"{override_user}:{override_password}" in result.connection_string
        assert result.user == override_user
        assert result.password == override_password

    def test_parse_url_with_partial_credential_override(self):
        """Test parsing URL with embedded user but password override."""
        url_user = "user"
        new_password = "newsecret"  # noqa: S105
        url = f"jdbc:postgresql://{url_user}@localhost:5432/mydb"
        result = parse_jdbc_url(url, password=new_password)

        assert result.driver == "postgresql"
        assert result.user == url_user
        assert result.password == new_password

    def test_invalid_url_missing_jdbc_prefix(self):
        """Test that URLs without jdbc: prefix are rejected."""
        url = "postgresql://localhost:5432/mydb"

        with pytest.raises(InvalidJDBCUrlError, match="must start with 'jdbc:'"):
            parse_jdbc_url(url)

    def test_invalid_url_malformed(self):
        """Test that malformed JDBC URLs are rejected."""
        url = "jdbc:"

        with pytest.raises(InvalidJDBCUrlError, match="Invalid JDBC URL format"):
            parse_jdbc_url(url)

    def test_oracle_thin_driver(self):
        """Test parsing Oracle JDBC URL with thin driver."""
        url = "jdbc:oracle:thin:@localhost:1521:orcl"
        result = parse_jdbc_url(url)

        assert result.driver == "oracle"
        assert "oracle" in result.connection_string

    def test_url_with_query_parameters(self):
        """Test parsing URL with query parameters."""
        url = "jdbc:postgresql://localhost:5432/mydb?ssl=true&timeout=30"
        result = parse_jdbc_url(url)

        assert result.driver == "postgresql"
        assert "ssl=true" in result.connection_string
        assert "timeout=30" in result.connection_string

    def test_url_credential_extraction(self):
        """Test that credentials are properly extracted from URL."""
        embedded_user = "myuser"
        embedded_password = "mypassword"  # noqa: S105
        url = f"jdbc:postgresql://{embedded_user}:{embedded_password}@localhost:5432/mydb"
        result = parse_jdbc_url(url)

        assert result.user == embedded_user
        assert result.password == embedded_password

    def test_url_credential_with_special_characters(self):
        """Test parsing URL with special characters in credentials."""
        # Note: In real usage, these should be URL-encoded
        url = "jdbc:postgresql://user:p%40ss@localhost:5432/mydb"
        result = parse_jdbc_url(url)

        assert result.driver == "postgresql"
        assert result.user == "user"
