"""Utility functions for JDBC reader."""


def mask_credentials(connection_string: str) -> str:
    """
    Mask username:password in connection strings for safe logging.

    Examples:
    - postgresql://user:pass@localhost:5432/db → postgresql://***:***@localhost:5432/db
    - mysql://root:secret@host/mydb → mysql://***:***@host/mydb
    - jdbc:sqlserver://user:pass@host → jdbc:sqlserver://***:***@host

    Args:
        connection_string: The connection string to mask

    Returns:
        Connection string with credentials replaced by ***:***
    """
    if "://" not in connection_string:
        return connection_string

    prefix, remainder = connection_string.split("://", 1)

    at_index = remainder.rfind("@")
    if at_index == -1:
        return connection_string

    credential_section = remainder[:at_index]
    colon_index = credential_section.find(":")
    if colon_index == -1:
        return connection_string

    masked_remainder = f"***:***@{remainder[at_index + 1 :]}"
    return f"{prefix}://{masked_remainder}"
