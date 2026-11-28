# SPDX-License-Identifier: Apache-2.0

"""Fixtures shared by the Kafka data source tests."""

from __future__ import annotations

import pytest

from pysail.tests._dev.spark.datasource.kafka.fakes import FakeAdminClient


@pytest.fixture(scope="module", autouse=True)
def require_confluent_kafka():
    pytest.importorskip("confluent_kafka")


@pytest.fixture(scope="module", autouse=True)
def spark_doctest():
    """Override the parent fixture: tests here start Spark only by requesting the ``spark`` fixture."""


@pytest.fixture
def consumer_factory(monkeypatch):
    """Install a fake Consumer and AdminClient backed by one broker state."""
    from pysail.spark.datasource.kafka import client as kafka_client

    holder = {}

    def install(consumer):
        holder["consumer"] = consumer
        holder["admin"] = FakeAdminClient(consumer)
        monkeypatch.setattr(kafka_client, "Consumer", lambda _config: holder["consumer"])
        monkeypatch.setattr(kafka_client, "AdminClient", lambda _config: holder["admin"])
        consumer.admin = holder["admin"]
        return consumer

    return install


@pytest.fixture(scope="module")
def kafka_options():
    from pysail.spark.datasource.kafka import options

    return options


@pytest.fixture(scope="module")
def kafka_models():
    from pysail.spark.datasource.kafka import models

    return models


@pytest.fixture(scope="module")
def kafka_client():
    from pysail.spark.datasource.kafka import client

    return client


@pytest.fixture(scope="module")
def kafka_planning():
    from pysail.spark.datasource.kafka import planning

    return planning


@pytest.fixture(scope="module")
def case_insensitive():
    from pyspark.sql.datasource import CaseInsensitiveDict

    return CaseInsensitiveDict


@pytest.fixture(scope="module")
def parse_read_options(kafka_options):
    def parse(**extra):
        return kafka_options.parse_options({"kafka.bootstrap.servers": "localhost:9092", "subscribe": "t", **extra})

    return parse


@pytest.fixture(scope="module")
def partition_of_t(kafka_models):
    def partition(partition_id):
        return kafka_models.TopicPartitionKey("t", partition_id)

    return partition


@pytest.fixture(scope="module")
def range_of_t(kafka_models, partition_of_t):
    def offset_range(partition, start, end):
        return kafka_models.OffsetRange(partition_of_t(partition), start, end)

    return offset_range
