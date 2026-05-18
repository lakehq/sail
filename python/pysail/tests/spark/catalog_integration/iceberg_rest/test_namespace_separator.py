from __future__ import annotations

import contextlib
import json
import urllib.parse
import urllib.request
from typing import TYPE_CHECKING

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from pysail.tests.spark.catalog_integration.conftest import (
    create_spark_session,
    start_sail_server,
    stop_sail_server,
)

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    from pyspark.sql import SparkSession
    from testcontainers.core.network import Network


def _quote(value: str) -> str:
    return urllib.parse.quote(value, safe="")


def _load_config(iceberg_rest_endpoint: str) -> dict[str, object]:
    url = f"{iceberg_rest_endpoint}/v1/config"
    # Safe: the URL is built from the controlled Iceberg REST fixture endpoint.
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
        return json.load(response)


def _nessie_iceberg_endpoint(container: DockerContainer) -> str:
    host = container.get_container_host_ip()
    port = container.get_exposed_port(19120)
    return f"http://{host}:{port}/iceberg"


def _load_namespace(
    iceberg_rest_endpoint: str,
    namespace_parts: list[str],
    separator: str,
    *,
    prefix: str | None = None,
) -> dict[str, object]:
    namespace = _quote(separator.join(namespace_parts))
    prefix_path = f"/{_quote(prefix)}" if prefix is not None else ""
    url = f"{iceberg_rest_endpoint}/v1{prefix_path}/namespaces/{namespace}"
    # Safe: the URL is built from the controlled Iceberg REST fixture endpoint.
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
        return json.load(response)


def _assert_rest_config_namespace_separator(config: dict[str, object], separator: str) -> None:
    defaults = config.get("defaults", {})
    overrides = config.get("overrides", {})
    assert isinstance(defaults, dict), config
    assert isinstance(overrides, dict), config
    actual = overrides.get("namespace-separator", defaults.get("namespace-separator"))
    assert actual == separator, config


def _rest_config_prefix(config: dict[str, object]) -> str | None:
    defaults = config.get("defaults", {})
    overrides = config.get("overrides", {})
    assert isinstance(defaults, dict), config
    assert isinstance(overrides, dict), config
    prefix = overrides.get("prefix", defaults.get("prefix"))
    assert prefix is None or isinstance(prefix, str), config
    return prefix


@contextlib.contextmanager
def _nessie_rest_container(
    docker_network: Network,
    seaweedfs_internal_endpoint: str,
    tmp_path: Path,
    separator: str,
) -> Generator[DockerContainer, None, None]:
    config_path = tmp_path / "nessie-application.properties"
    config_path.write_text(
        f"nessie.catalog.iceberg-config-defaults.namespace-separator={separator}\n"
    )
    container = (
        DockerContainer("ghcr.io/projectnessie/nessie:0.107.5")
        .with_exposed_ports(19120)
        .with_env("NESSIE_CATALOG_DEFAULT_WAREHOUSE", "warehouse")
        .with_env("NESSIE_CATALOG_WAREHOUSES_WAREHOUSE_LOCATION", "s3://icebergdata/nessie")
        .with_env("NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ENDPOINT", seaweedfs_internal_endpoint)
        .with_env("NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_REGION", "us-east-1")
        .with_env("NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_PATH_STYLE_ACCESS", "true")
        .with_env("NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_AUTH_TYPE", "STATIC")
        .with_env(
            "NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ACCESS_KEY",
            "urn:nessie-secret:quarkus:nessie.catalog.secrets.s3default",
        )
        .with_env("NESSIE_CATALOG_SECRETS_S3DEFAULT_NAME", "admin")
        .with_env("NESSIE_CATALOG_SECRETS_S3DEFAULT_SECRET", "password")
        .with_volume_mapping(
            str(config_path),
            "/tmp/nessie-application.properties",
            "ro",
        )
        .with_env("QUARKUS_CONFIG_LOCATIONS", "file:/tmp/nessie-application.properties")
        .with_network(docker_network)
        .with_network_aliases("nessie")
    )
    container.start()
    try:
        wait_for_logs(container, "Listening on:", timeout=120)
        yield container
    finally:
        container.stop()


@contextlib.contextmanager
def _spark_with_iceberg_rest(
    iceberg_rest_endpoint: str,
    catalog_options: str,
    app_name: str,
) -> Generator[SparkSession, None, None]:
    catalog_config = f'[{{name="sail", type="iceberg-rest", uri="{iceberg_rest_endpoint}", {catalog_options}}}]'
    server, remote, saved_env = start_sail_server(catalog_list=catalog_config)
    spark = create_spark_session(remote, app_name, new_session=True)
    try:
        yield spark
    finally:
        with contextlib.suppress(Exception):
            spark.stop()
        stop_sail_server(server, saved_env)


def _assert_namespace_visible(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
    namespace_parts: list[str],
    separator: str,
    *,
    prefix: str | None = None,
) -> None:
    namespace = ".".join(namespace_parts)

    rows = {row.info_name: row.info_value for row in spark.sql(f"DESCRIBE DATABASE {namespace}").collect()}
    assert rows["Namespace Name"] == namespace

    rest_namespace = _load_namespace(iceberg_rest_endpoint, namespace_parts, separator, prefix=prefix)["namespace"]
    assert rest_namespace == namespace_parts


def _exercise_namespace_separator(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
    *,
    namespace_id: str,
    separator: str,
    prefix: str | None = None,
) -> None:
    parent = f"namespace_separator_{namespace_id}"
    child = "child"
    namespace = f"{parent}.{child}"

    spark.sql(f"DROP DATABASE IF EXISTS {parent} CASCADE")
    try:
        spark.sql(f"CREATE DATABASE {parent}")
        spark.sql(f"CREATE DATABASE {namespace}")
        _assert_namespace_visible(
            spark,
            iceberg_rest_endpoint,
            [parent, child],
            separator,
            prefix=prefix,
        )
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS {namespace}")
        spark.sql(f"DROP DATABASE IF EXISTS {parent} CASCADE")


@pytest.mark.parametrize(
    ("config_key", "namespace_id"),
    [
        ("namespace_separator", "snake_case"),
        ("namespace-separator", "kebab_case"),
        ("namespaceSeparator", "camel_case"),
        ("namespaceseparator", "flat_case"),
    ],
)
@pytest.mark.usefixtures("_create_s3_bucket")
def test_namespace_separator_config_aliases(
    docker_network: Network,
    seaweedfs_internal_endpoint: str,
    tmp_path: Path,
    config_key: str,
    namespace_id: str,
) -> None:
    separator = "::"

    with _nessie_rest_container(
        docker_network,
        seaweedfs_internal_endpoint,
        tmp_path,
        separator,
    ) as nessie:
        iceberg_rest_endpoint = _nessie_iceberg_endpoint(nessie)
        config = _load_config(iceberg_rest_endpoint)
        _assert_rest_config_namespace_separator(config, separator)

        with _spark_with_iceberg_rest(
            iceberg_rest_endpoint,
            f'{config_key}="{separator}"',
            f"iceberg_rest_namespace_separator_{namespace_id}",
        ) as spark:
            _exercise_namespace_separator(
                spark,
                iceberg_rest_endpoint,
                namespace_id=namespace_id,
                separator=separator,
                prefix=_rest_config_prefix(config),
            )


@pytest.mark.parametrize(
    ("separator", "namespace_id"),
    [
        ("::", "colon_colon"),
        ("__", "underscore_underscore"),
    ],
)
@pytest.mark.usefixtures("_create_s3_bucket")
def test_namespace_separator_custom_values(
    docker_network: Network,
    seaweedfs_internal_endpoint: str,
    tmp_path: Path,
    separator: str,
    namespace_id: str,
) -> None:
    with _nessie_rest_container(
        docker_network,
        seaweedfs_internal_endpoint,
        tmp_path,
        separator,
    ) as nessie:
        iceberg_rest_endpoint = _nessie_iceberg_endpoint(nessie)
        config = _load_config(iceberg_rest_endpoint)
        _assert_rest_config_namespace_separator(config, separator)

        with _spark_with_iceberg_rest(
            iceberg_rest_endpoint,
            f'namespace_separator="{separator}"',
            f"iceberg_rest_namespace_separator_{namespace_id}",
        ) as spark:
            _exercise_namespace_separator(
                spark,
                iceberg_rest_endpoint,
                namespace_id=namespace_id,
                separator=separator,
                prefix=_rest_config_prefix(config),
            )
