"""Unit tests for utility functions."""

from pysail.jdbc.utils import mask_credentials


class TestCredentialMasking:
    """Test credential masking utility."""

    def test_mask_postgresql_url(self):
        """Test masking credentials in PostgreSQL URL."""
        url = "postgresql://user:password@localhost:5432/mydb"
        masked = mask_credentials(url)

        assert masked == "postgresql://***:***@localhost:5432/mydb"
        assert "user" not in masked
        assert "password" not in masked

    def test_mask_mysql_url(self):
        """Test masking credentials in MySQL URL."""
        url = "mysql://root:secret@host:3306/database"
        masked = mask_credentials(url)

        assert masked == "mysql://***:***@host:3306/database"
        assert "root" not in masked
        assert "secret" not in masked

    def test_mask_jdbc_url(self):
        """Test masking credentials in JDBC URL."""
        url = "jdbc:postgresql://admin:pass123@localhost:5432/db"
        masked = mask_credentials(url)

        assert masked == "jdbc:postgresql://***:***@localhost:5432/db"
        assert "admin" not in masked
        assert "pass123" not in masked

    def test_mask_complex_password(self):
        """Test masking credentials with special characters."""
        url = "postgresql://user:p@ssw0rd!@localhost/db"
        masked = mask_credentials(url)

        assert masked == "postgresql://***:***@localhost/db"
        assert "p@ssw0rd!" not in masked

    def test_no_credentials_unchanged(self):
        """Test that URLs without credentials remain unchanged."""
        url = "postgresql://localhost:5432/mydb"
        masked = mask_credentials(url)

        assert masked == url

    def test_mask_url_with_query_params(self):
        """Test masking URL with query parameters."""
        url = "postgresql://user:pass@localhost/db?ssl=true"
        masked = mask_credentials(url)

        assert masked == "postgresql://***:***@localhost/db?ssl=true"
        assert "user" not in masked
        assert "pass" not in masked
        assert "ssl=true" in masked

    def test_mask_multiple_occurrences(self):
        """Test that only the credential part is masked."""
        url = "postgresql://user:user@localhost/user_db"
        masked = mask_credentials(url)

        # Only the credentials should be masked, not the database name
        assert masked == "postgresql://***:***@localhost/user_db"
        assert "user_db" in masked  # Database name preserved

    def test_mask_empty_password(self):
        """Test masking URL with empty password."""
        url = "postgresql://user:@localhost/db"
        masked = mask_credentials(url)

        assert masked == "postgresql://***:***@localhost/db"

    def test_mask_only_username(self):
        """Test URL with only username (no password)."""
        # Note: Without colon separator, this won't match the pattern
        url = "postgresql://user@localhost/db"
        masked = mask_credentials(url)

        # No colon, so pattern won't match
        assert masked == url

    def test_mask_with_port(self):
        """Test masking URL with explicit port."""
        url = "postgresql://admin:secret@localhost:5432/mydb"
        masked = mask_credentials(url)

        assert masked == "postgresql://***:***@localhost:5432/mydb"
        assert ":5432" in masked  # Port preserved
