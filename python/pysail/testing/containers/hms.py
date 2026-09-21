"""Hive Metastore container fixtures."""

from __future__ import annotations

import os
import socket
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.image import DockerImage
from testcontainers.core.network import Network
from testcontainers.core.waiting_utils import wait_for_logs

if TYPE_CHECKING:
    from collections.abc import Generator

_HMS_IMAGE = "apache/hive:4.0.0"
_HMS_PORT = 9083
_KDC_PORT = 88
_KDC_TCP_PORT = f"{_KDC_PORT}/tcp"
_KDC_UDP_PORT = f"{_KDC_PORT}/udp"
_KERBEROS_REALM = "SAIL.TEST"
_KDC_HOSTNAME = "sail-kerberos-kdc"
_HMS_HOSTNAME = "sail-kerberos-hms"
_HMS_KEYTAB_PATH = "/opt/hive/conf/hms.service.keytab"
_KRB5_CONFIG_PATH = "/etc/krb5.conf"
_HIVE_SITE_PATH = "/opt/hive/conf/hive-site.xml"
_CORE_SITE_PATH = "/opt/hadoop/etc/hadoop/core-site.xml"
_FIXTURE_PATH = Path(__file__).with_name("kerberos-kdc")


@dataclass(frozen=True)
class HmsService:
    """Host-visible Hive Metastore endpoint."""

    host: str
    port: int

    @property
    def endpoint(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass(frozen=True)
class KerberosHmsService:
    """Host-visible Kerberos HMS endpoint and client credentials."""

    host: str
    port: int
    krb5_config: Path
    client_keytab: Path
    client_principal: str
    service_principal: str


def _wait_for_port(host: str, port: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except OSError:
            time.sleep(1)
    message = f"Hive Metastore did not accept connections on {host}:{port}"
    raise TimeoutError(message)


def _published_host(container: DockerContainer) -> str:
    host = container.get_container_host_ip()
    return "127.0.0.1" if host in {"localhost", "::1"} else host


def _exec_checked(container: DockerContainer, command: list[str], operation: str) -> None:
    result = container.exec(command)
    if result.exit_code != 0:
        output = result.output.decode(errors="replace")
        message = f"{operation} failed with exit code {result.exit_code}: {output}"
        raise RuntimeError(message)


def _wait_for_nonempty_file(container: DockerContainer, path: str) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        result = container.exec(["sh", "-lc", f"test -s {path}"])
        if result.exit_code == 0:
            return
        time.sleep(1)
    message = f"Kerberos fixture file was not created: {path}"
    raise TimeoutError(message)


def _host_krb5_config(kdc_host: str, kdc_tcp_port: int, kdc_udp_port: int) -> str:
    return f"""[libdefaults]
 default_realm = {_KERBEROS_REALM}
 dns_lookup_kdc = false
 dns_lookup_realm = false
 rdns = false
 dns_canonicalize_hostname = false
 qualify_shortname = ""
 ignore_acceptor_hostname = true
 udp_preference_limit = 1
 ticket_lifetime = 24h
 forwardable = true

[realms]
 {_KERBEROS_REALM} = {{
  kdc = {kdc_host}:{kdc_udp_port}
  kdc = {kdc_host}:{kdc_tcp_port}
 }}

[domain_realm]
 localhost = {_KERBEROS_REALM}
 .localhost = {_KERBEROS_REALM}
 .local = {_KERBEROS_REALM}
 .internal.cloudapp.net = {_KERBEROS_REALM}
 .cloudapp.net = {_KERBEROS_REALM}
"""


def _hive_site_config(service_principal: str) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=/tmp/sail-kerberos-metastore/metastore_db;create=true</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.apache.derby.jdbc.EmbeddedDriver</value>
  </property>
  <property>
    <name>datanucleus.autoCreateSchema</name>
    <value>true</value>
  </property>
  <property>
    <name>datanucleus.schema.autoCreateTables</name>
    <value>true</value>
  </property>
  <property>
    <name>datanucleus.schema.autoCreateColumns</name>
    <value>true</value>
  </property>
  <property>
    <name>datanucleus.schema.autoCreateConstraints</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.metastore.sasl.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.metastore.kerberos.principal</name>
    <value>{service_principal}</value>
  </property>
  <property>
    <name>hive.metastore.kerberos.keytab.file</name>
    <value>{_HMS_KEYTAB_PATH}</value>
  </property>
  <property>
    <name>hadoop.rpc.protection</name>
    <value>authentication</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/tmp/sail-kerberos-metastore/warehouse</value>
  </property>
</configuration>
"""


def _core_site_config() -> str:
    return """<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>hadoop.security.authentication</name>
    <value>kerberos</value>
  </property>
  <property>
    <name>hadoop.security.authorization</name>
    <value>true</value>
  </property>
</configuration>
"""


@pytest.fixture(scope="session")
def hms_service() -> Generator[HmsService, None, None]:
    """Start an unauthenticated Hive Metastore for native provider tests."""
    hms = (
        DockerContainer(_HMS_IMAGE)
        .with_env("SERVICE_NAME", "metastore")
        .with_env("VERBOSE", "true")
        .with_exposed_ports(_HMS_PORT)
    )
    with hms:
        host = _published_host(hms)
        port = int(hms.get_exposed_port(_HMS_PORT))
        _wait_for_port(host, port, timeout=180)
        time.sleep(10)
        yield HmsService(host=host, port=port)


@pytest.fixture(scope="session")
def kerberos_hms_service(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[KerberosHmsService, None, None]:
    """Start a KDC and a Kerberos-protected Hive Metastore."""
    fixture_dir = tmp_path_factory.mktemp("kerberos_hms")
    image_tag = f"pysail-kerberos-kdc:test-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    service_principal = f"hive-metastore/localhost@{_KERBEROS_REALM}"
    client_principal = f"sail-test-client@{_KERBEROS_REALM}"

    with DockerImage(path=_FIXTURE_PATH, tag=image_tag) as kdc_image, Network() as network:
        kdc = (
            DockerContainer(str(kdc_image))
            .with_kwargs(hostname=_KDC_HOSTNAME)
            .with_env("KERBEROS_REALM", _KERBEROS_REALM)
            .with_env("KDC_HOSTNAME", _KDC_HOSTNAME)
            .with_exposed_ports(_KDC_TCP_PORT, _KDC_UDP_PORT)
            .with_network(network)
            .with_network_aliases(_KDC_HOSTNAME)
        )
        with kdc:
            wait_for_logs(kdc, "KDC ready", timeout=120)
            kdc_host = _published_host(kdc)
            kdc_tcp_port = int(kdc.get_exposed_port(_KDC_TCP_PORT))  # type: ignore[arg-type]
            kdc_udp_port = int(kdc.get_exposed_port(_KDC_UDP_PORT))  # type: ignore[arg-type]
            _wait_for_port(kdc_host, kdc_tcp_port, timeout=60)

            _exec_checked(
                kdc,
                [
                    "/usr/local/bin/bootstrap-kdc.sh",
                    service_principal,
                    client_principal,
                    "/artifacts/hms.service.keytab",
                    "/artifacts/client.keytab",
                ],
                "bootstrap Kerberos principals",
            )
            for path in (
                "/artifacts/hms.service.keytab",
                "/artifacts/client.keytab",
                "/artifacts/krb5.conf",
            ):
                _wait_for_nonempty_file(kdc, path)

            hms_keytab = fixture_dir / "hms.service.keytab"
            client_keytab = fixture_dir / "client.keytab"
            hms_krb5_config = fixture_dir / "krb5-hms.conf"
            kdc.copy_from_container("/artifacts/hms.service.keytab", hms_keytab)
            kdc.copy_from_container("/artifacts/client.keytab", client_keytab)
            kdc.copy_from_container("/artifacts/krb5.conf", hms_krb5_config)
            hms_keytab.chmod(0o644)
            client_keytab.chmod(0o600)

            host_krb5_config = fixture_dir / "krb5-host.conf"
            host_krb5_config.write_text(
                _host_krb5_config(kdc_host, kdc_tcp_port, kdc_udp_port),
                encoding="utf-8",
            )
            hive_site = fixture_dir / "hive-site.xml"
            hive_site.write_text(_hive_site_config(service_principal), encoding="utf-8")
            core_site = fixture_dir / "core-site.xml"
            core_site.write_text(_core_site_config(), encoding="utf-8")

            hms = (
                DockerContainer(_HMS_IMAGE)
                .with_kwargs(hostname=_HMS_HOSTNAME)
                .with_env("SERVICE_NAME", "metastore")
                .with_env("VERBOSE", "true")
                .with_env("HIVE_CONF_DIR", "/opt/hive/conf")
                .with_env("HADOOP_CONF_DIR", "/opt/hadoop/etc/hadoop")
                .with_env(
                    "SERVICE_OPTS",
                    f"-Djava.security.krb5.conf={_KRB5_CONFIG_PATH} "
                    "-Dsun.security.krb5.debug=true -Dsun.security.jgss.debug=true",
                )
                .with_exposed_ports(_HMS_PORT)
                .with_network(network)
                .with_network_aliases(_HMS_HOSTNAME)
                .with_copy_into_container(hms_krb5_config, _KRB5_CONFIG_PATH)
                .with_copy_into_container(hive_site, _HIVE_SITE_PATH)
                .with_copy_into_container(core_site, _CORE_SITE_PATH)
                .with_copy_into_container(hms_keytab, _HMS_KEYTAB_PATH)
            )
            with hms:
                hms_host = _published_host(hms)
                hms_port = int(hms.get_exposed_port(_HMS_PORT))
                _wait_for_port(hms_host, hms_port, timeout=240)
                time.sleep(10)
                yield KerberosHmsService(
                    host=hms_host,
                    port=hms_port,
                    krb5_config=host_krb5_config,
                    client_keytab=client_keytab,
                    client_principal=client_principal,
                    service_principal=service_principal,
                )
