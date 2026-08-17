"""Subprocess probe for Kerberos Hive Metastore integration tests."""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import time

_DATABASE_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_ITEM_COLUMNS = [("id", "int64", True, None), ("value", "utf8", True, None)]
_HMS_CONTAINER_TMP = "/tmp"  # noqa: S108


def _hms_catalog(uri: str, service_principal: str):
    from pysail import _native  # noqa: PLC0415

    return _native._catalog._hms.HmsCatalogProvider(  # noqa: SLF001
        "kerberos-hms-test",
        [uri],
        auth="kerberos",
        kerberos_service_principal=service_principal,
    )


def _wait_for_catalog(catalog, timeout: float = 240) -> None:
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            catalog.list_databases()
        except Exception as error:  # noqa: BLE001
            last_error = error
            time.sleep(1)
        else:
            return
    message = f"Kerberos HMS catalog did not become queryable: {last_error}"
    raise TimeoutError(message)


def _database_and_table_round_trip(database: str, uri: str, service_principal: str) -> None:
    if _DATABASE_PATTERN.fullmatch(database) is None:
        message = f"invalid test database name: {database!r}"
        raise ValueError(message)

    catalog = _hms_catalog(uri, service_principal)
    namespace = [database]
    _wait_for_catalog(catalog)
    catalog.drop_database(namespace, if_exists=True, cascade=True)
    try:
        created_database = catalog.create_database(namespace)
        assert created_database.database == namespace
        fetched_database = catalog.get_database(namespace)
        assert fetched_database.database == namespace

        created_table = catalog.create_table(
            namespace,
            "items",
            _ITEM_COLUMNS,
            location=f"{_HMS_CONTAINER_TMP}/{database}_items",
        )
        assert created_table.name == "items"
        assert created_table.kind == "table"
        assert created_table.format == "parquet"
        assert [(column.name, column.data_type) for column in created_table.columns] == [
            ("id", "Int64"),
            ("value", "Utf8"),
        ]

        fetched = catalog.get_table(namespace, "items")
        assert fetched.name == "items"
        assert [status.name for status in catalog.list_tables(namespace)] == ["items"]

        catalog.drop_table(namespace, "items")
        assert catalog.list_tables(namespace) == []
    finally:
        catalog.drop_database(namespace, if_exists=True, cascade=True)


def _assert_missing_credentials_rejected(uri: str, service_principal: str) -> None:
    catalog = _hms_catalog(uri, service_principal)
    try:
        catalog.list_databases()
    except Exception as error:
        message = str(error).lower()
        if any(token in message for token in ("kerberos", "gssapi", "sasl", "credential")):
            return
        message = f"expected Kerberos-related failure, got: {error}"
        raise AssertionError(message) from error
    message = "Kerberos HMS query unexpectedly succeeded without credentials"
    raise AssertionError(message)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("case", choices=("round-trip", "missing-credentials"))
    parser.add_argument("--database")
    parser.add_argument("--keytab")
    parser.add_argument("--principal")
    parser.add_argument("--uri", required=True)
    parser.add_argument("--service-principal", required=True)
    args = parser.parse_args()

    if args.case == "round-trip":
        if not args.database or not args.keytab or not args.principal:
            parser.error("round-trip requires --database, --keytab, and --principal")
        kinit = shutil.which("kinit")
        if kinit is None:
            parser.error("kinit is required for the Kerberos HMS integration test")
        subprocess.run([kinit, "-kt", args.keytab, args.principal], check=True)
        _database_and_table_round_trip(args.database, args.uri, args.service_principal)
    else:
        _assert_missing_credentials_rejected(args.uri, args.service_principal)


if __name__ == "__main__":
    main()
