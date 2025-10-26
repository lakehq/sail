"""JDBC URL parsing with minimal conversion strategy.

This module extracts the driver name and delegates the rest to backends.
ConnectorX and ADBC already understand their dialects; we don't reinvent it.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from urllib.parse import urlparse

from pysail.jdbc.exceptions import InvalidJDBCUrlError


@dataclass
class ParsedJDBCUrl:
    """Minimal JDBC URL parse result (delegate to backend)."""

    driver: str  # e.g., 'postgresql', 'mysql', 'oracle'
    connection_string: str  # Full connection string for backend
    user: str | None = None  # Optional override from URL
    password: str | None = None  # Optional override from URL


def parse_jdbc_url(jdbc_url: str, user: str | None = None, password: str | None = None) -> ParsedJDBCUrl:
    """
    Extract driver and delegate rest to backend.

    Examples:
    - jdbc:postgresql://localhost/mydb → driver='postgresql', connection_string='postgresql://localhost/mydb'
    - jdbc:mysql://root@host/db → driver='mysql', connection_string='mysql://root@host/db'
    - jdbc:sqlserver://host;database=db → driver='sqlserver', connection_string='sqlserver://host;database=db'

    Args:
        jdbc_url: JDBC URL string starting with jdbc:
        user: Optional user to override/inject into URL
        password: Optional password to override/inject into URL

    Returns:
        ParsedJDBCUrl with driver, connection_string, and optional credentials

    Raises:
        InvalidJDBCUrlError: If URL format is invalid
    """
    if not jdbc_url.startswith("jdbc:"):
        msg = f"Invalid JDBC URL: must start with 'jdbc:', got: {jdbc_url}"
        raise InvalidJDBCUrlError(msg)

    # Extract driver (e.g., 'postgresql', 'mysql:thin')
    match = re.match(r"jdbc:([^:]+(?::[^/:]+)?)(.*)", jdbc_url)
    if not match:
        msg = f"Invalid JDBC URL format: {jdbc_url}"
        raise InvalidJDBCUrlError(msg)

    driver_part = match.group(1)
    rest = match.group(2)

    # Normalize driver to base name (strip subtype like ':thin')
    driver = driver_part.split(":")[0].lower()

    # Build URI by replacing 'jdbc:driver' with just 'driver'
    # For most backends, we can strip the jdbc: prefix
    connection_string = f"{driver}{rest}"

    # Special handling for SQLite: ensure absolute paths have three slashes
    # jdbc:sqlite:/path/to/file.db -> sqlite:///path/to/file.db
    if driver == "sqlite" and rest.startswith(":/") and not rest.startswith("://"):
        connection_string = f"{driver}://{rest[1:]}"  # sqlite:///path/to/file.db

    # Extract credentials from URL if present
    url_user = None
    url_password = None

    if "://" in connection_string:
        parsed = urlparse(connection_string)
        url_user = parsed.username
        url_password = parsed.password

        # If user/password provided as parameters, inject them into URL
        if user or password:
            final_user = user or url_user or ""
            final_password = password or url_password or ""

            # Rebuild URL with injected credentials
            if final_user or final_password:
                # Remove existing credentials from netloc
                netloc = parsed.hostname or ""
                if parsed.port:
                    netloc = f"{netloc}:{parsed.port}"

                # Add credentials
                if final_password:
                    netloc = f"{final_user}:{final_password}@{netloc}"
                elif final_user:
                    netloc = f"{final_user}@{netloc}"

                connection_string = f"{parsed.scheme}://{netloc}{parsed.path}"
                if parsed.query:
                    connection_string += f"?{parsed.query}"
                if parsed.fragment:
                    connection_string += f"#{parsed.fragment}"

    # Use provided credentials or extracted ones
    final_user = user or url_user
    final_password = password or url_password

    return ParsedJDBCUrl(
        driver=driver,
        connection_string=connection_string,
        user=final_user,
        password=final_password,
    )


def validate_driver_supported(driver: str) -> None:
    """
    Warn if driver may not be supported by any enabled backend.

    Note: This is minimal validation; backends will fail explicitly if unsupported.

    Args:
        driver: Database driver name (e.g., 'postgresql', 'mysql')
    """
    # Common supported drivers
    supported_drivers = {
        "postgresql",
        "postgres",
        "mysql",
        "sqlite",
        "sqlserver",
        "mssql",
        "oracle",
        "snowflake",
        "redshift",
        "clickhouse",
    }

    if driver.lower() not in supported_drivers:
        logger = logging.getLogger("lakesail.jdbc")
        supported = ", ".join(sorted(supported_drivers))
        logger.warning(
            "Driver '%s' may not be supported by ConnectorX or ADBC backends. Supported drivers: %s",
            driver,
            supported,
        )
