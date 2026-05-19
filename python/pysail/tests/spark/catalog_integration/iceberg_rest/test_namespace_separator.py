from __future__ import annotations

import contextlib
import json
import urllib.parse
import urllib.request
from typing import TYPE_CHECKING

import pytest
from testcontainers.core.waiting_utils import wait_for_logs

from pysail.tests.spark.catalog_integration.conftest import (
    create_spark_session,
    start_sail_server,
    stop_sail_server,
)
from pysail.tests.spark.catalog_integration.iceberg_rest.conftest import make_nessie_container

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    from pyspark.sql import SparkSession
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.network import Network


@pytest.fixture(autouse=True)
def spark_doctest() -> None:
    """Override the module-autouse doctest fixture.

    These tests create their own Sail server with a custom Nessie REST catalog
    configuration. The shared doctest fixture would otherwise start the default
    iceberg_spark fixture for this module too.
    """


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


def _list_namespaces(
    iceberg_rest_endpoint: str,
    separator: str,
    *,
    prefix: str | None = None,
    parent: list[str] | None = None,
) -> dict[str, object]:
    return _list_namespaces_with_parent_separator(
        iceberg_rest_endpoint,
        separator,
        prefix=prefix,
        parent=parent,
        parent_separator=separator,
    )


def _list_namespaces_with_parent_separator(
    iceberg_rest_endpoint: str,
    separator: str,
    *,
    prefix: str | None = None,
    parent: list[str] | None = None,
    parent_separator: str,
) -> dict[str, object]:
    prefix_path = f"/{_quote(prefix)}" if prefix is not None else ""
    query = ""
    if parent is not None:
        parent_namespace = parent_separator.join(parent)
        query = f"?parent={_quote(parent_namespace)}"
    url = f"{iceberg_rest_endpoint}/v1{prefix_path}/namespaces{query}"
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
    container = make_nessie_container(
        docker_network,
        seaweedfs_internal_endpoint,
        config_path=config_path,
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


def _assert_namespace_listed(
    iceberg_rest_endpoint: str,
    namespace_parts: list[str],
    separator: str,
    *,
    prefix: str | None = None,
) -> None:
    root_namespaces = _list_namespaces(iceberg_rest_endpoint, separator, prefix=prefix)
    assert namespace_parts[:1] in root_namespaces["namespaces"]

    for depth in range(1, len(namespace_parts)):
        parent = namespace_parts[:depth]
        child = namespace_parts[: depth + 1]
        child_namespaces = _list_namespaces(
            iceberg_rest_endpoint,
            separator,
            prefix=prefix,
            parent=parent,
        )
        if child not in child_namespaces["namespaces"] and len(parent) > 1:
            child_namespaces = _list_namespaces_with_parent_separator(
                iceberg_rest_endpoint,
                separator,
                prefix=prefix,
                parent=parent,
                parent_separator="\x1f",
            )
        assert child in child_namespaces["namespaces"]


def _exercise_namespace_separator(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
    *,
    namespace_id: str,
    separator: str,
    prefix: str | None = None,
) -> None:
    namespace_parts = [f"namespace_separator_{namespace_id}", "child", "grandchild"]
    root_namespace = namespace_parts[0]
    nested_namespace = ".".join(namespace_parts[:2])
    deeply_nested_namespace = ".".join(namespace_parts)

    spark.sql(f"DROP DATABASE IF EXISTS {root_namespace} CASCADE").collect()
    try:
        spark.sql(f"CREATE DATABASE {root_namespace}").collect()
        spark.sql(f"CREATE DATABASE {nested_namespace}").collect()
        spark.sql(f"CREATE DATABASE {deeply_nested_namespace}").collect()
        _assert_namespace_listed(
            iceberg_rest_endpoint,
            namespace_parts,
            separator,
            prefix=prefix,
        )
    finally:
        with contextlib.suppress(Exception):
            spark.sql(f"DROP DATABASE IF EXISTS {deeply_nested_namespace} CASCADE").collect()
        with contextlib.suppress(Exception):
            spark.sql(f"DROP DATABASE IF EXISTS {root_namespace} CASCADE").collect()


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
    separator = "-"

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
