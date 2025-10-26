"""JDBC options normalization and validation."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pysail.jdbc.exceptions import InvalidOptionsError
from pysail.jdbc.query_builder import validate_identifier

logger = logging.getLogger("lakesail.jdbc")

# Maximum lengths to prevent DoS via huge strings
MAX_URL_LENGTH = 2048
MAX_QUERY_LENGTH = 1_000_000  # 1MB for complex queries
MAX_TABLE_NAME_LENGTH = 256
MAX_OPTION_LENGTH = 4096


def normalize_session_init(session_init_statement: str | None) -> str | None:
    """
    Validate sessionInitStatement is DDL-safe.

    Allow: SET, CREATE TEMPORARY, etc.
    Deny: INSERT, UPDATE, DELETE (no DML allowed)

    Args:
        session_init_statement: SQL statement to execute before reading

    Returns:
        Normalized statement or None

    Raises:
        InvalidOptionsError: If statement contains dangerous keywords
    """
    if not session_init_statement:
        return None

    dangerous_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE"]
    stmt_upper = session_init_statement.upper().strip()

    for keyword in dangerous_keywords:
        if stmt_upper.startswith(keyword):
            msg = f"sessionInitStatement cannot start with {keyword}. " f"Only SET and read-only configuration allowed."
            raise InvalidOptionsError(msg)

    return session_init_statement


@dataclass
class NormalizedJDBCOptions:
    """Normalized & validated JDBC options."""

    url: str
    dbtable: str | None = None
    query: str | None = None
    user: str | None = None
    password: str | None = None
    engine: str = "connectorx"
    partition_column: str | None = None
    lower_bound: int | None = None
    upper_bound: int | None = None
    num_partitions: int = 1
    predicates: list[str] | None = None
    fetch_size: int = 10000
    session_init_statement: str | None = None
    isolation_level: str = "READ_COMMITTED"

    @classmethod
    def from_spark_options(cls, options: dict[str, str]) -> NormalizedJDBCOptions:
        """
        Convert Spark option dict → normalized options.

        Handles case-insensitive option names and validates consistency.

        Args:
            options: Dictionary of JDBC options from Spark

        Returns:
            NormalizedJDBCOptions instance

        Raises:
            InvalidOptionsError: If options are missing or inconsistent
        """
        # Normalize keys to lowercase (Spark passes camelCase, handle both)
        norm_opts = {k.lower(): v for k, v in options.items()}

        # Extract required
        url = norm_opts.get("url")
        if not url:
            msg = "'url' option is required"
            raise InvalidOptionsError(msg)
        if len(url) > MAX_URL_LENGTH:
            msg = f"URL exceeds maximum length of {MAX_URL_LENGTH} characters"
            raise InvalidOptionsError(msg)

        # Extract dbtable/query (exactly one required)
        dbtable = norm_opts.get("dbtable")
        query = norm_opts.get("query")

        if not dbtable and not query:
            msg = "Either 'dbtable' or 'query' option must be specified"
            raise InvalidOptionsError(msg)
        if dbtable and query:
            msg = "Cannot specify both 'dbtable' and 'query'"
            raise InvalidOptionsError(msg)

        # Validate lengths
        if dbtable and len(dbtable) > MAX_TABLE_NAME_LENGTH:
            msg = f"dbtable exceeds maximum length of {MAX_TABLE_NAME_LENGTH} characters"
            raise InvalidOptionsError(msg)
        if query and len(query) > MAX_QUERY_LENGTH:
            msg = f"query exceeds maximum length of {MAX_QUERY_LENGTH} characters"
            raise InvalidOptionsError(msg)

        # Extract user/password (may be in URL or as options)
        user = norm_opts.get("user")
        password = norm_opts.get("password")

        # Engine selection
        engine = norm_opts.get("engine", "connectorx").lower()
        if engine not in ("connectorx", "adbc", "fallback"):
            msg = f"Invalid engine: {engine}. Must be one of: connectorx, adbc, fallback"
            raise InvalidOptionsError(msg)

        # Partitioning
        partition_column = norm_opts.get("partitioncolumn")
        lower_bound = None
        upper_bound = None
        num_partitions = 1

        if "lowerbound" in norm_opts:
            try:
                lower_bound = int(norm_opts["lowerbound"])
            except ValueError as err:
                msg = f"lowerBound must be an integer, got: {norm_opts['lowerbound']}"
                raise InvalidOptionsError(msg) from err

        if "upperbound" in norm_opts:
            try:
                upper_bound = int(norm_opts["upperbound"])
            except ValueError as err:
                msg = f"upperBound must be an integer, got: {norm_opts['upperbound']}"
                raise InvalidOptionsError(msg) from err

        if "numpartitions" in norm_opts:
            try:
                num_partitions = int(norm_opts["numpartitions"])
            except ValueError as err:
                msg = f"numPartitions must be an integer, got: {norm_opts['numpartitions']}"
                raise InvalidOptionsError(msg) from err

        # Predicates
        predicates = None
        if "predicates" in norm_opts:
            predicates = [p.strip() for p in norm_opts["predicates"].split(",") if p.strip()]

        # Fetch size
        fetch_size = 10000
        if "fetchsize" in norm_opts:
            try:
                fetch_size = int(norm_opts["fetchsize"])
            except ValueError as err:
                msg = f"fetchsize must be an integer, got: {norm_opts['fetchsize']}"
                raise InvalidOptionsError(msg) from err

        # Session init
        session_init = norm_opts.get("sessioninitstatement")
        if session_init:
            session_init = normalize_session_init(session_init)

        # Isolation level
        isolation_level = norm_opts.get("isolationlevel", "READ_COMMITTED").upper()

        return cls(
            url=url,
            dbtable=dbtable,
            query=query,
            user=user,
            password=password,
            engine=engine,
            partition_column=partition_column,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            num_partitions=num_partitions,
            predicates=predicates,
            fetch_size=fetch_size,
            session_init_statement=session_init,
            isolation_level=isolation_level,
        )

    def validate(self) -> None:
        """
        Validate option consistency.

        Raises:
            InvalidOptionsError: If options are inconsistent
        """
        # Validate partition column identifier to prevent SQL injection
        if self.partition_column:
            try:
                validate_identifier(self.partition_column)
            except InvalidOptionsError as err:
                message = f"Invalid partitionColumn: {err}"
                raise InvalidOptionsError(message) from err

        # Partitioning validation
        if self.partition_column:
            if self.lower_bound is None or self.upper_bound is None:
                msg = "partitionColumn requires lowerBound and upperBound"
                raise InvalidOptionsError(msg)
            if self.lower_bound >= self.upper_bound:
                msg = f"lowerBound ({self.lower_bound}) must be less than upperBound ({self.upper_bound})"
                raise InvalidOptionsError(msg)
            if self.num_partitions < 1:
                msg = "numPartitions must be >= 1"
                raise InvalidOptionsError(msg)

        # Predicate validation
        if self.predicates and self.partition_column:
            msg = "Cannot specify both predicates list and partitionColumn"
            raise InvalidOptionsError(msg)

        # Num partitions validation
        if self.num_partitions < 1:
            msg = f"numPartitions must be >= 1, got: {self.num_partitions}"
            raise InvalidOptionsError(msg)

        # Fetch size validation
        if self.fetch_size < 1:
            msg = f"fetchsize must be >= 1, got: {self.fetch_size}"
            raise InvalidOptionsError(msg)
