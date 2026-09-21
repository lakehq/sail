"""Kerberos-authenticated native Hive Metastore integration tests."""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from pysail.testing.containers.hms import KerberosHmsService

_PROBE_MODULE = "pysail.testing.containers.hms_probe"


def _run_probe(
    service: KerberosHmsService,
    credential_cache: Path,
    *arguments: str,
) -> None:
    env = {
        **os.environ,
        "KRB5_CONFIG": str(service.krb5_config),
        "KRB5CCNAME": f"FILE:{credential_cache}",
    }
    env.pop("SPARK_REMOTE", None)
    env.pop("SAIL_CATALOG__LIST", None)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            _PROBE_MODULE,
            *arguments,
            "--uri",
            f"{service.host}:{service.port}",
            "--service-principal",
            service.service_principal,
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert result.returncode == 0, (
        f"Kerberos HMS probe failed with exit code {result.returncode}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )


def test_kerberos_hms_database_and_table_round_trip(
    kerberos_hms_service: KerberosHmsService,
    tmp_path: Path,
) -> None:
    database = f"kerberos_hms_{uuid.uuid4().hex}"
    _run_probe(
        kerberos_hms_service,
        tmp_path / "krb5cc-round-trip",
        "round-trip",
        "--database",
        database,
        "--keytab",
        str(kerberos_hms_service.client_keytab),
        "--principal",
        kerberos_hms_service.client_principal,
    )


def test_kerberos_hms_rejects_missing_credentials(
    kerberos_hms_service: KerberosHmsService,
    tmp_path: Path,
) -> None:
    _run_probe(
        kerberos_hms_service,
        tmp_path / "krb5cc-missing",
        "missing-credentials",
    )
