from __future__ import annotations

import contextlib
import json
import time
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

    from pyspark.sql import SparkSession
    from testcontainers.core.network import Network


def _quote(value: str) -> str:
    return urllib.parse.quote(value, safe="")


def _load_config(iceberg_rest_endpoint: str) -> dict[str, object]:
    url = f"{iceberg_rest_endpoint}/v1/config"
    # Safe: the URL is built from the controlled Iceberg REST fixture endpoint.
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
        return json.load(response)


def _load_namespace(
    iceberg_rest_endpoint: str,
    namespace_parts: list[str],
    separator: str,
) -> dict[str, object]:
    namespace = _quote(separator.join(namespace_parts))
    url = f"{iceberg_rest_endpoint}/v1/namespaces/{namespace}"
    # Safe: the URL is built from the controlled Iceberg REST fixture endpoint.
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
        return json.load(response)


@contextlib.contextmanager
def _custom_separator_rest_container(
    docker_network: Network,
    seaweedfs_internal_endpoint: str,
    separator: str,
) -> Generator[DockerContainer, None, None]:
    encoded_separator = _quote(separator)
    container = (
        DockerContainer("apache/iceberg-rest-fixture:1.10.1")
        .with_exposed_ports(8181)
        .with_env("AWS_ACCESS_KEY_ID", "admin")
        .with_env("AWS_SECRET_ACCESS_KEY", "password")
        .with_env("AWS_REGION", "us-east-1")
        .with_env("CATALOG_CATALOG__IMPL", "org.apache.iceberg.jdbc.JdbcCatalog")
        .with_env("CATALOG_URI", f"jdbc:sqlite:file:/tmp/iceberg_rest_ns_separator_{int(time.time_ns())}")
        .with_env("CATALOG_WAREHOUSE", "s3://icebergdata/demo")
        .with_env("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
        .with_env("CATALOG_S3_ENDPOINT", seaweedfs_internal_endpoint)
        .with_env("CATALOG_S3_PATH__STYLE__ACCESS", "true")
        .with_env("CATALOG_NAMESPACE__SEPARATOR", encoded_separator)
        .with_network(docker_network)
    )
    container.start()
    try:
        wait_for_logs(container, "INFO org.eclipse.jetty.server.Server - Started ", timeout=120)
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
    spark = create_spark_session(remote, app_name)
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
) -> None:
    namespace = ".".join(namespace_parts)

    rows = {row.info_name: row.info_value for row in spark.sql(f"DESCRIBE DATABASE {namespace}").collect()}
    assert rows["Namespace Name"] == namespace

    listed_namespaces = {row.name for row in spark.sql(f"SHOW DATABASES LIKE '{namespace}'").collect()}
    assert namespace in listed_namespaces

    rest_namespace = _load_namespace(iceberg_rest_endpoint, namespace_parts, separator)["namespace"]
    assert rest_namespace == namespace_parts


def _exercise_namespace_separator(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
    *,
    namespace_id: str,
    separator: str,
) -> None:
    parent = f"namespace_separator_{namespace_id}"
    child = "child"
    namespace = f"{parent}.{child}"

    spark.sql(f"DROP DATABASE IF EXISTS {parent} CASCADE")
    try:
        spark.sql(f"CREATE DATABASE {parent}")
        spark.sql(f"CREATE DATABASE {namespace}")
        _assert_namespace_visible(spark, iceberg_rest_endpoint, [parent, child], separator)
    finally:
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
    config_key: str,
    namespace_id: str,
) -> None:
    separator = "::"

    with _custom_separator_rest_container(
        docker_network,
        seaweedfs_internal_endpoint,
        separator,
    ) as rest:
        iceberg_rest_endpoint = f"http://{rest.get_container_host_ip()}:{rest.get_exposed_port(8181)}"
        config = _load_config(iceberg_rest_endpoint)
        assert config["defaults"]["namespace-separator"] == _quote(separator)

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
    separator: str,
    namespace_id: str,
) -> None:
    with _custom_separator_rest_container(
        docker_network,
        seaweedfs_internal_endpoint,
        separator,
    ) as rest:
        iceberg_rest_endpoint = f"http://{rest.get_container_host_ip()}:{rest.get_exposed_port(8181)}"
        config = _load_config(iceberg_rest_endpoint)
        assert config["defaults"]["namespace-separator"] == _quote(separator)

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
            )
