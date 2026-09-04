from __future__ import annotations

import contextlib
import json
import re
import shutil
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow.parquet as pq
import pyspark
import pytest

from pysail.testing.spark.utils.jvm import delta_spark_maven_coordinate
from pysail.testing.spark.utils.sql import escape_sql_identifier

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from pyspark.sql import SparkSession


pytestmark = pytest.mark.integration

_CHECKPOINT_VERSION = 1
_CHECKPOINT_ACTIONS_PER_PART = 2
_EXPECTED_CHECKPOINT_PARTS = 2
_EXPECTED_DATA_FILES = 4
_JSON_FILE_RE = re.compile(r"^(?P<version>\d{20})\.json$")
_CHECKSUM_FILE_RE = re.compile(r"^(?P<version>\d{20})\.crc$")


@dataclass(frozen=True)
class DeltaSparkMultipartTable:
    path: Path
    expected_ids: tuple[int, ...]
    checkpoint_parts: tuple[Path, ...]


def _spark_conf_value(spark: SparkSession, key: str) -> str | None:
    try:
        return spark.conf.get(key)
    except Exception:  # noqa: BLE001
        return None


@contextlib.contextmanager
def _spark_conf_overrides(spark: SparkSession, values: dict[str, str]) -> Iterator[None]:
    previous = {key: _spark_conf_value(spark, key) for key in values}
    try:
        for key, value in values.items():
            spark.conf.set(key, value)
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                with contextlib.suppress(Exception):
                    spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


def _local_checksum_path(path: Path) -> Path:
    return path.with_name(f".{path.name}.crc")


def _delete_file_and_local_checksum(path: Path) -> None:
    path.unlink()
    with contextlib.suppress(FileNotFoundError):
        _local_checksum_path(path).unlink()


def _copy_table(source: Path, destination: Path) -> None:
    shutil.copytree(source, destination)


def _delete_pre_checkpoint_history(table_path: Path) -> None:
    log_path = table_path / "_delta_log"
    for path in log_path.iterdir():
        json_match = _JSON_FILE_RE.fullmatch(path.name)
        checksum_match = _CHECKSUM_FILE_RE.fullmatch(path.name)
        if json_match is not None and int(json_match.group("version")) < _CHECKPOINT_VERSION:
            _delete_file_and_local_checksum(path)
        elif checksum_match is not None and int(checksum_match.group("version")) <= _CHECKPOINT_VERSION:
            _delete_file_and_local_checksum(path)


def _delta_spark_ids(spark: SparkSession, table_path: Path) -> list[int]:
    table_identifier = f"delta.`{escape_sql_identifier(str(table_path))}`"
    return [row.id for row in spark.sql(f"SELECT id FROM {table_identifier} ORDER BY id").collect()]  # noqa: S608


def _sail_ids(spark: SparkSession, table_path: Path, *, metadata_as_data: bool) -> list[int]:
    frame = spark.read.format("delta").option("metadataAsDataRead", str(metadata_as_data).lower()).load(str(table_path))
    return [row.id for row in frame.select("id").orderBy("id").collect()]


@pytest.fixture(scope="module")
def delta_spark_multipart_table(
    delta_jvm_spark: SparkSession,
    tmp_path_factory: pytest.TempPathFactory,
) -> DeltaSparkMultipartTable:
    table_path = tmp_path_factory.mktemp("delta-spark-multipart") / "table"
    table_identifier = f"delta.`{escape_sql_identifier(str(table_path))}`"
    expected_ids = tuple(range(32))

    with _spark_conf_overrides(
        delta_jvm_spark,
        {
            "spark.databricks.delta.checkpoint.partSize": str(_CHECKPOINT_ACTIONS_PER_PART),
            "spark.databricks.delta.checkpoint.exceptionThrowing.enabled": "true",
            "spark.databricks.delta.snapshotPartitions": "1",
        },
    ):
        delta_jvm_spark.sql(
            f"""
            CREATE TABLE {table_identifier} (id BIGINT)
            USING DELTA
            TBLPROPERTIES (
                'delta.checkpointInterval' = '1',
                'delta.checkpointPolicy' = 'classic'
            )
            """
        )
        (
            delta_jvm_spark.range(len(expected_ids))
            .repartition(_EXPECTED_DATA_FILES)
            .write.format("delta")
            .mode("append")
            .save(str(table_path))
        )

    checkpoint_parts = tuple(
        sorted((table_path / "_delta_log").glob(f"{_CHECKPOINT_VERSION:020d}.checkpoint.*.parquet"))
    )
    return DeltaSparkMultipartTable(
        path=table_path,
        expected_ids=expected_ids,
        checkpoint_parts=checkpoint_parts,
    )


def test_delta_spark_writes_complete_multipart_checkpoint(
    delta_jvm_spark: SparkSession,
    delta_spark_multipart_table: DeltaSparkMultipartTable,
) -> None:
    _, artifact, version = delta_spark_maven_coordinate(pyspark.__version__).split(":")
    expected_jar = f"{artifact}-{version}.jar"
    class_loader = delta_jvm_spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
    delta_log_class = class_loader.loadClass("org.apache.spark.sql.delta.DeltaLog")
    delta_jar = str(delta_log_class.getProtectionDomain().getCodeSource().getLocation())
    assert expected_jar in delta_jar

    expected_names = [
        f"{_CHECKPOINT_VERSION:020d}.checkpoint.{part:010d}.{_EXPECTED_CHECKPOINT_PARTS:010d}.parquet"
        for part in range(1, _EXPECTED_CHECKPOINT_PARTS + 1)
    ]
    assert [path.name for path in delta_spark_multipart_table.checkpoint_parts] == expected_names

    log_path = delta_spark_multipart_table.path / "_delta_log"
    with (log_path / "_last_checkpoint").open(encoding="utf-8") as handle:
        hint = json.load(handle)
    assert hint["version"] == _CHECKPOINT_VERSION
    assert hint["parts"] == _EXPECTED_CHECKPOINT_PARTS

    checkpoint_tables = [pq.read_table(path) for path in delta_spark_multipart_table.checkpoint_parts]
    assert sum(table.num_rows for table in checkpoint_tables) == hint["size"]
    assert (
        sum(value is not None for table in checkpoint_tables for value in table["add"].to_pylist())
        == _EXPECTED_DATA_FILES
    )
    assert _delta_spark_ids(delta_jvm_spark, delta_spark_multipart_table.path) == list(
        delta_spark_multipart_table.expected_ids
    )


def test_sail_reads_delta_spark_multipart_checkpoint(
    delta_jvm_spark: SparkSession,
    delta_spark_multipart_table: DeltaSparkMultipartTable,
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    table_path = tmp_path / "complete-checkpoint"
    _copy_table(delta_spark_multipart_table.path, table_path)
    _delete_pre_checkpoint_history(table_path)

    remaining_json = sorted(path.name for path in (table_path / "_delta_log").glob("*.json"))
    assert remaining_json == [f"{_CHECKPOINT_VERSION:020d}.json"]
    expected_ids = list(delta_spark_multipart_table.expected_ids)
    assert _delta_spark_ids(delta_jvm_spark, table_path) == expected_ids
    for metadata_as_data in (False, True):
        assert _sail_ids(spark, table_path, metadata_as_data=metadata_as_data) == expected_ids


def test_sail_ignores_incomplete_delta_spark_multipart_checkpoint(
    delta_jvm_spark: SparkSession,
    delta_spark_multipart_table: DeltaSparkMultipartTable,
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    table_path = tmp_path / "incomplete-checkpoint"
    _copy_table(delta_spark_multipart_table.path, table_path)
    missing_part = table_path / "_delta_log" / delta_spark_multipart_table.checkpoint_parts[-1].name
    _delete_file_and_local_checksum(missing_part)

    expected_ids = list(delta_spark_multipart_table.expected_ids)
    assert _delta_spark_ids(delta_jvm_spark, table_path) == expected_ids
    for metadata_as_data in (False, True):
        assert _sail_ids(spark, table_path, metadata_as_data=metadata_as_data) == expected_ids
