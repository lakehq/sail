"""Identity MERGE invariants.

Assertions avoid assumptions about partition-stride numbering and row-to-ID ordering.
Explicit BY DEFAULT values retain Sail's existing watermark policy.
"""

import json
import os
import subprocess
import sys
import time
import uuid

import pyarrow.parquet as pq
import pytest

from pysail.testing.spark.session import spark_session_factory
from pysail.testing.spark.utils.sql import escape_sql_string_literal


@pytest.fixture
def identity_table(spark, tmp_path):
    tables = []

    def create(*, mode="ALWAYS", start=10, step=3, schema=None, properties=""):
        name = "identity_merge_" + uuid.uuid4().hex
        path = tmp_path / name
        schema = (
            schema or f"id BIGINT GENERATED {mode} AS IDENTITY (START WITH {start} INCREMENT BY {step}), k INT, v INT"
        )
        spark.sql(
            f"CREATE TABLE {name} ({schema}) USING DELTA LOCATION '{escape_sql_string_literal(str(path))}' {properties}"
        ).collect()
        tables.append(name)
        return name, path

    yield create
    for name in tables:
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


@pytest.fixture
def single_partition_spark(remote):
    with spark_session_factory(remote) as sessions:
        session = sessions.create()
        # Spark's shuffle setting alone does not change DataFusion's window partitioning.
        session.sql("SET datafusion.execution.target_partitions = 1").collect()
        yield session


def _log(path):
    return {file.name: file.read_bytes() for file in sorted((path / "_delta_log").glob("*.json"))}


def _metadata(path):
    metadata = None
    for contents in _log(path).values():
        for line in contents.splitlines():
            metadata = json.loads(line).get("metaData", metadata)
    assert metadata is not None
    return {field["name"]: field["metadata"] for field in json.loads(metadata["schemaString"])["fields"]}


def _watermark(path, column="id"):
    return _metadata(path)[column].get("delta.identity.highWaterMark")


def _state(spark, name, path):
    frame = spark.table(name)
    return frame.orderBy("k").collect(), frame.schema.json(), _log(path)


@pytest.mark.parametrize("mode", ["ALWAYS", "BY DEFAULT"])
@pytest.mark.parametrize(("start", "step"), [(10, 3), (0, -2)])
def test_merge_identity_lifecycle(spark, identity_table, mode, start, step):
    name, path = identity_table(mode=mode, start=start, step=step)
    spark.sql(f"INSERT INTO {name} (k,v) VALUES (1,10)").collect()  # noqa: S608
    before = _watermark(path)
    spark.sql(f"""
        MERGE INTO {name} t USING (SELECT 1 AS k,11 AS v UNION ALL SELECT 2,22) s ON t.k=s.k
        WHEN MATCHED THEN UPDATE SET v=s.v
        WHEN NOT MATCHED THEN INSERT (k,v) VALUES (s.k,s.v)
    """).collect()  # noqa: S608
    rows = spark.table(name).orderBy("k").collect()
    assert rows[0] == (start, 1, 11)
    generated = rows[1].id
    assert (generated - start) % step == 0
    assert (generated - before) * step > 0
    assert _watermark(path) == generated
    merge_commit = [json.loads(line) for line in list(_log(path).values())[-1].splitlines()]
    assert any("add" in action for action in merge_commit)
    assert any("metaData" in action for action in merge_commit)
    spark.sql(f"INSERT INTO {name} (k,v) VALUES (3,33)").collect()  # noqa: S608
    reopened = spark.read.format("delta").load(str(path)).orderBy("k").collect()
    expected_keys = [1, 2, 3]
    assert [row.k for row in reopened] == expected_keys
    assert len({row.id for row in reopened}) == len(expected_keys)
    assert (reopened[2].id - generated) * step > 0
    assert _watermark(path) == reopened[2].id
    assert _metadata(path)["id"]["delta.identity.allowExplicitInsert"] == (mode == "BY DEFAULT")
    for file in path.glob("*.parquet"):
        assert pq.ParquetFile(file).schema_arrow.names == ["id", "k", "v"]


def test_merge_by_default_explicit_and_null_are_not_generated(spark, identity_table):
    name, path = identity_table(mode="BY DEFAULT")
    spark.sql(f"""
        MERGE INTO {name} t USING (SELECT 1 AS k UNION ALL SELECT 2) s ON t.k=s.k
        WHEN NOT MATCHED AND s.k=1 THEN INSERT (id,k,v) VALUES (42,s.k,10)
        WHEN NOT MATCHED THEN INSERT (id,k,v) VALUES (NULL,s.k,20)
    """).collect()  # noqa: S608
    assert spark.table(name).orderBy("k").collect() == [(42, 1, 10), (None, 2, 20)]
    expected_watermark = 43  # Existing Sail policy: round explicit values forward.
    assert _watermark(path) == expected_watermark
    spark.sql(f"INSERT INTO {name} (k,v) VALUES (3,30)").collect()  # noqa: S608
    expected_next_id = 46
    assert spark.table(name).where("k=3").first().id == expected_next_id


@pytest.mark.parametrize("matched", [False, True], ids=["fast-append", "copy-on-write"])
def test_merge_nullable_clause_conditions_do_not_generate(spark, identity_table, matched):
    name, path = identity_table(mode="BY DEFAULT", start=1, step=1)
    source = "identity_source_" + uuid.uuid4().hex
    spark.sql(f"INSERT INTO {name} (k,v) VALUES (0,0)").collect()  # noqa: S608
    spark.createDataFrame([(1, True), (2, False), (3, None)], "k int, c boolean").createOrReplaceTempView(source)
    try:
        update = "WHEN MATCHED THEN UPDATE SET v=1" if matched else ""
        spark.sql(f"""
            MERGE INTO {name} t USING {source} s ON t.k=s.k {update}
            WHEN NOT MATCHED AND s.c THEN INSERT (k,v) VALUES (s.k,1)
            WHEN NOT MATCHED THEN INSERT (id,k,v) VALUES (NULL,s.k,2)
        """).collect()  # noqa: S608
        assert spark.table(name).orderBy("k").collect() == [(1, 0, 0), (2, 1, 1), (None, 2, 2), (None, 3, 2)]
        expected_watermark = 2  # Only the true clause allocates an identity.
        assert _watermark(path) == expected_watermark
    finally:
        spark.catalog.dropTempView(source)


@pytest.mark.parametrize("mode", ["ALWAYS", "BY DEFAULT"])
@pytest.mark.parametrize(
    ("clause", "error"),
    [
        ("WHEN MATCHED THEN UPDATE SET id=t.id", "UPDATE cannot assign Delta identity"),
        ("WHEN MATCHED THEN UPDATE SET *", "UPDATE cannot assign Delta identity"),
        ("WHEN NOT MATCHED BY SOURCE THEN UPDATE SET id=t.id", "UPDATE cannot assign Delta identity"),
        ("WHEN NOT MATCHED THEN INSERT (id,k,v) VALUES (DEFAULT,s.k,s.v)", "DEFAULT for an identity"),
    ],
)
def test_merge_identity_assignments_rejected(spark, identity_table, mode, clause, error):
    name, path = identity_table(mode=mode)
    spark.sql(f"INSERT INTO {name} (k,v) VALUES (1,10)").collect()  # noqa: S608
    before = _state(spark, name, path)
    with pytest.raises(Exception, match=error):
        spark.sql(f"MERGE INTO {name} t USING (SELECT 1 AS k,10 AS v,100L AS id) s ON t.k=s.k {clause}").collect()
    assert _state(spark, name, path) == before


@pytest.mark.parametrize("value", ["100L", "NULL"])
def test_merge_always_rejects_supply_even_in_unreachable_clause(spark, identity_table, value):
    name, path = identity_table()
    before = _state(spark, name, path)
    with pytest.raises(Exception, match="GENERATED ALWAYS AS IDENTITY"):
        spark.sql(f"""
            MERGE INTO {name} t USING (SELECT 1 AS k,10 AS v) s ON t.k=s.k
            WHEN NOT MATCHED AND false THEN INSERT (id,k,v) VALUES ({value},s.k,s.v)
        """).collect()  # noqa: S608
    assert _state(spark, name, path) == before


@pytest.mark.parametrize("evolution", [False, True])
def test_merge_identity_star_omission_and_evolution_fail_closed(spark, identity_table, evolution):
    name, path = identity_table(mode="BY DEFAULT")
    before = _state(spark, name, path)
    with pytest.raises(Exception, match=r"schema evolution with identity|INSERT \* omitting identity"):
        spark.sql(f"""
            MERGE {"WITH SCHEMA EVOLUTION" if evolution else ""} INTO {name} t
            USING (SELECT 1 AS k,10 AS v) s ON t.k=s.k WHEN NOT MATCHED THEN INSERT *
        """).collect()
    assert _state(spark, name, path) == before


@pytest.mark.parametrize("matched", [False, True], ids=["fast-append", "copy-on-write"])
@pytest.mark.parametrize("equal_masks", [False, True], ids=["different-masks", "equal-masks"])
@pytest.mark.parametrize("partitions", [1, 8])
def test_merge_multiple_insert_clauses_share_allocation(spark, identity_table, matched, equal_masks, partitions):
    identity_start = 5
    name, path = identity_table(
        schema=f"""
        id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH {identity_start} INCREMENT BY 5),
        decreasing BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 0 INCREMENT BY -3),
        k INT, v INT
    """
    )
    spark.sql(f"INSERT INTO {name} (k,v) VALUES (0,100)").collect()  # noqa: S608
    source = "identity_source_" + uuid.uuid4().hex
    row_count = 129
    spark.sql(f"SET datafusion.execution.target_partitions = {partitions}").collect()
    spark.range(0, row_count, numPartitions=8).selectExpr("CAST(id AS INT) AS k").repartition(
        8
    ).createOrReplaceTempView(source)
    try:
        update = "WHEN MATCHED THEN UPDATE SET v=101" if matched else ""
        first_insert = "(k,v) VALUES (s.k,1)" if equal_masks else "(decreasing,k,v) VALUES (1000L+s.k,s.k,1)"
        spark.sql(f"""
            MERGE INTO {name} t USING {source} s ON t.k=s.k {update}
            WHEN NOT MATCHED AND s.k % 3 = 0 THEN INSERT {first_insert}
            WHEN NOT MATCHED AND s.k % 2 = 0 THEN INSERT (k,v) VALUES (s.k,2)
            WHEN NOT MATCHED THEN INSERT (k,v) VALUES (s.k,3)
        """).collect()  # noqa: S608
        rows = spark.table(name).orderBy("k").collect()
        assert len(rows) == row_count
        assert len({row.id for row in rows}) == row_count
        assert all((row.id - identity_start) % 5 == 0 for row in rows)
        assert rows[0].id == identity_start
        assert rows[0].decreasing == 0
        generated = [row.decreasing for row in rows if row.k and (equal_masks or row.k % 3 != 0)]
        assert len(set(generated)) == len(generated)
        assert all(value < 0 and value % -3 == 0 for value in generated)
        if not equal_masks:
            assert all(row.decreasing == 1000 + row.k for row in rows if row.k and row.k % 3 == 0)
        assert all(row.v == (1 if row.k % 3 == 0 else 2 if row.k % 2 == 0 else 3) for row in rows[1:])
        assert _watermark(path) == max(row.id for row in rows)
        assert _watermark(path, "decreasing") == min(generated)
    finally:
        spark.sql("RESET datafusion.execution.target_partitions").collect()
        spark.catalog.dropTempView(source)


@pytest.mark.parametrize(("start", "step"), [(9223372036854775807, 1), (-9223372036854775808, -1)])
@pytest.mark.parametrize(
    ("source", "condition"),
    [
        ("SELECT 1 AS k,11 AS v", "true"),
        ("SELECT 2 AS k,22 AS v", "false"),
        ("SELECT 2 AS k,22 AS v WHERE false", "true"),
    ],
)
def test_merge_without_generated_rows_allows_exhausted_watermark(spark, identity_table, start, step, source, condition):
    name, path = identity_table(start=start, step=step)
    spark.sql(f"INSERT INTO {name} (k,v) VALUES (1,10)").collect()  # noqa: S608
    spark.sql(f"""
        MERGE INTO {name} t USING ({source}) s ON t.k=s.k
        WHEN MATCHED THEN UPDATE SET v=s.v
        WHEN NOT MATCHED AND {condition} THEN INSERT (k,v) VALUES (s.k,s.v)
    """).collect()  # noqa: S608
    assert spark.table(name).count() == 1
    assert spark.table(name).first().id == start
    assert _watermark(path) == start


@pytest.mark.parametrize("kind", ["cdf", "stats-disabled", "stats-excluded", "beyond-prefix"])
def test_merge_missing_watermark_evidence_fails_without_commit(spark, identity_table, kind):
    properties = {
        "cdf": "TBLPROPERTIES ('delta.enableChangeDataFeed'='true')",
        "stats-disabled": "TBLPROPERTIES ('delta.dataSkippingNumIndexedCols'='0')",
        "stats-excluded": "TBLPROPERTIES ('delta.dataSkippingStatsColumns'='k,v')",
        "beyond-prefix": "",
    }[kind]
    schema = None
    if kind == "beyond-prefix":
        schema = ",".join(
            [f"c{i} INT" for i in range(32)] + ["id BIGINT GENERATED ALWAYS AS IDENTITY", "k INT", "v INT"]
        )
    name, path = identity_table(properties=properties, schema=schema)
    before = _state(spark, name, path)
    files_before = set(path.rglob("*.parquet"))
    with pytest.raises(Exception, match=r"change data feed|min/max statistics"):
        spark.sql(f"""
            MERGE INTO {name} t USING (SELECT 1 AS k,10 AS v) s ON t.k=s.k
            WHEN NOT MATCHED THEN INSERT (k,v) VALUES (s.k,s.v)
        """).collect()  # noqa: S608
    assert _state(spark, name, path) == before
    assert set(path.rglob("*.parquet")) == files_before


@pytest.mark.parametrize(("start", "step"), [(1, 1), (0, -2)])
def test_merge_missing_identity_row_group_bounds_rejects_atomically(single_partition_spark, tmp_path, start, step):
    spark = single_partition_spark
    # The default Parquet row-group size makes an all-NULL group followed by one generated row.
    limit = 1_048_576
    name = "identity_merge_" + uuid.uuid4().hex
    source = "identity_source_" + uuid.uuid4().hex
    path = tmp_path / name
    spark.sql(f"""
        CREATE TABLE {name} (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH {start} INCREMENT BY {step}), k BIGINT
        ) USING DELTA LOCATION '{escape_sql_string_literal(str(path))}'
    """).collect()
    try:
        before = _state(spark, name, path)
        assert _watermark(path) is None
        spark.range(limit + 1, numPartitions=1).selectExpr("id AS k").createOrReplaceTempView(source)
        with pytest.raises(Exception, match=r"Delta identity MERGE requires usable.*statistics"):
            spark.sql(f"""
                MERGE INTO {name} t USING {source} s ON t.k=s.k
                WHEN NOT MATCHED AND s.k < {limit} THEN INSERT (id,k) VALUES (NULL,s.k)
                WHEN NOT MATCHED THEN INSERT (k) VALUES (s.k)
            """).collect()  # noqa: S608
        assert _state(spark, name, path) == before
        assert _watermark(path) is None
        assert spark.read.format("delta").load(str(path)).collect() == []

        # Cluster retries may leave several uncommitted attempts. Every file must have
        # the actual two-group layout; none may appear in the committed table.
        files = list(path.glob("*.parquet"))
        assert files
        expected_row_groups = 2
        for file in files:
            parquet = pq.ParquetFile(file)
            assert parquet.metadata.num_row_groups == expected_row_groups
            null_group = parquet.metadata.row_group(0)
            value_group = parquet.metadata.row_group(1)
            assert null_group.num_rows == limit
            assert null_group.column(0).statistics.null_count == limit
            assert not null_group.column(0).statistics.has_min_max
            assert value_group.num_rows == 1
            assert value_group.column(0).statistics.null_count == 0
            assert value_group.column(0).statistics.min == value_group.column(0).statistics.max == start

        for offset in range(2):
            spark.sql(f"INSERT INTO {name} (k) VALUES ({limit + 1 + offset})").collect()  # noqa: S608
            assert _watermark(path) == start + offset * step
        assert spark.read.format("delta").load(str(path)).orderBy("k").collect() == [
            (start, limit + 1),
            (start + step, limit + 2),
        ]
        assert len(_log(path)) == len(before[2]) + 2
        for contents in list(_log(path).values())[len(before[2]) :]:
            commit = [json.loads(line) for line in contents.splitlines()]
            assert next(action["commitInfo"]["operation"] for action in commit if "commitInfo" in action) == "WRITE"
    finally:
        spark.catalog.dropTempView(source)
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


@pytest.mark.parametrize(("start", "step"), [(1, 1), (0, -2)])
def test_merge_all_null_and_empty_inputs_do_not_require_identity_bounds(spark, identity_table, start, step):
    name, path = identity_table(mode="BY DEFAULT", start=start, step=step)
    before = _state(spark, name, path)
    spark.sql(f"""
        MERGE INTO {name} t USING (SELECT 1 AS k WHERE false) s ON t.k=s.k
        WHEN NOT MATCHED THEN INSERT (k) VALUES (s.k)
    """).collect()  # noqa: S608
    assert _state(spark, name, path) == before
    spark.sql(f"""
        MERGE INTO {name} t USING (SELECT 1 AS k UNION ALL SELECT 2) s ON t.k=s.k
        WHEN NOT MATCHED AND s.k < 3 THEN INSERT (id,k) VALUES (NULL,s.k)
        WHEN NOT MATCHED THEN INSERT (k) VALUES (s.k)
    """).collect()  # noqa: S608
    assert spark.table(name).orderBy("k").collect() == [(None, 1, None), (None, 2, None)]
    assert _watermark(path) is None
    adds = [json.loads(line)["add"] for line in list(_log(path).values())[-1].splitlines() if "add" in json.loads(line)]
    assert adds
    for add in adds:
        stats = json.loads(add["stats"])
        assert stats["numRecords"] > 0
        assert stats["nullCount"]["id"] == stats["numRecords"]
        assert "id" not in stats.get("minValues", {})
        assert "id" not in stats.get("maxValues", {})
        assert stats.get("tightBounds", True) is True
        assert add.get("deletionVector") is None
    spark.sql(f"INSERT INTO {name} (k) VALUES (3)").collect()  # noqa: S608
    assert spark.table(name).where("k=3").first().id == start
    assert _watermark(path) == start


@pytest.mark.parametrize("matched", [False, True], ids=["fast-append", "copy-on-write"])
@pytest.mark.parametrize(("start", "step"), [(9223372036854775806, 1), (-9223372036854775807, -1)])
def test_merge_generation_failure_does_not_commit(spark, identity_table, matched, start, step):
    name, path = identity_table(start=start, step=step)
    spark.sql(f"INSERT INTO {name} (k,v) VALUES (0,0)").collect()  # noqa: S608
    before = _state(spark, name, path)
    update = "WHEN MATCHED THEN UPDATE SET v=s.v" if matched else ""
    with pytest.raises(Exception, match="DELTA_IDENTITY_COLUMN_VALUE_OVERFLOW"):
        spark.sql(f"""
            MERGE INTO {name} t USING (SELECT 0 AS k,100 AS v UNION ALL SELECT 1,10 UNION ALL SELECT 2,20) s
            ON t.k=s.k {update}
            WHEN NOT MATCHED THEN INSERT (k,v) VALUES (s.k,s.v)
        """).collect()  # noqa: S608
    assert _state(spark, name, path) == before


@pytest.mark.parametrize("volatile_value", [False, True], ids=["source-alias", "insert-value"])
def test_merge_identity_helpers_do_not_shadow_user_values(spark, identity_table, volatile_value):
    name, path = identity_table(
        schema="""
            id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, k BIGINT, v DOUBLE,
            __sail_src_identity_generate INT, __sail_src_identity_row_number INT,
            __sail_src_identity_partition_key INT
        """
    )
    spark.sql(f"""
        INSERT INTO {name} (k,v,__sail_src_identity_generate,__sail_src_identity_row_number,__sail_src_identity_partition_key)
        VALUES (0,0.0,7,8,9)
    """).collect()  # noqa: S608
    value = "rand()" if volatile_value else "s.r"
    row_count = 257
    threshold = 0.5
    first_marker = 10
    spark.sql(f"""
        MERGE INTO {name} t USING (SELECT id AS k, rand() AS r FROM range({row_count})) s ON t.k=s.k
        WHEN MATCHED THEN UPDATE SET v=s.r
        WHEN NOT MATCHED AND s.r < {threshold} THEN
          INSERT (k,v,__sail_src_identity_generate,__sail_src_identity_row_number,__sail_src_identity_partition_key)
          VALUES (s.k,{value},{first_marker},20,30)
        WHEN NOT MATCHED THEN
          INSERT (k,v,__sail_src_identity_generate,__sail_src_identity_row_number,__sail_src_identity_partition_key)
          VALUES (s.k,{value},11,21,31)
    """).collect()  # noqa: S608
    frame = spark.table(name)
    rows = frame.orderBy("k").collect()
    assert len(rows) == len({row.id for row in rows}) == row_count
    assert not frame.schema["id"].nullable
    assert all(0 <= row.v < 1 for row in rows)
    assert tuple(rows[0][3:]) == (7, 8, 9)
    for row in rows[1:]:
        assert tuple(row[3:]) in {(10, 20, 30), (11, 21, 31)}
        if not volatile_value:
            assert (row.v < threshold) == (row[3] == first_marker)
    assert _watermark(path) == max(row.id for row in rows)
    for file in path.glob("*.parquet"):
        assert pq.ParquetFile(file).schema_arrow.names == frame.columns


def _allocation_window_rows(spark, session_id, jobs_before, expected_rows):
    deadline = time.monotonic() + 20
    expected_windows = 2
    jobs = {}
    while time.monotonic() < deadline:
        samples = (
            spark.sql("""
            SELECT attributes, to_json(value) AS value FROM system.telemetry.metrics
            WHERE name = 'execution.output_row_count'
        """)
            .toArrow()
            .to_pylist()
        )
        for sample in samples:
            attrs = dict(sample["attributes"])
            if (
                attrs.get("session.id") != session_id
                or attrs.get("execution.job.id") in jobs_before
                or attrs.get("execution.operator.name") != "BoundedWindowAggExec"
            ):
                continue
            # Job IDs are session-local. Inspector queries also create jobs, so
            # identify the MERGE by its two allocation windows, not min(new_ids).
            windows = jobs.setdefault(attrs["execution.job.id"], {})
            key = (attrs["execution.stage"], attrs["execution.operator.id"])
            partition = int(attrs["execution.partition"])
            value = json.loads(sample["value"])["gauge"]
            rows = windows.setdefault(key, {})
            rows[partition] = max(rows.get(partition, 0), value)
        complete = [
            windows
            for windows in jobs.values()
            if len(windows) == expected_windows
            and all(sum(rows.values()) == expected_rows for rows in windows.values())
        ]
        if complete:
            assert len(complete) == 1, jobs
            return complete[0]
    pytest.fail(f"incomplete allocation window metrics for session {session_id}: {jobs}")


@pytest.mark.parametrize("layout", ["eight-files", "single-file", "source-explicit"])
def test_merge_distributes_non_generating_allocation_rows(spark, tmp_path, layout, record_property):
    if os.environ.get("SPARK_REMOTE"):
        pytest.skip("allocation metrics require a locally configured Sail server")
    mode = os.environ.get("SAIL_MODE", "local")
    if mode == "local":
        record_property("allocation_metrics", "not applicable: local mode has no distributed jobs")
        spark.sql("SET datafusion.execution.target_partitions = 8").collect()
        try:
            _check_non_generating_allocation_rows(spark, tmp_path, layout, distributed=False)
        finally:
            spark.sql("RESET datafusion.execution.target_partitions").collect()
        return
    assert mode == "local-cluster", f"unsupported allocation metrics test mode: {mode}"
    # Telemetry is process-global and may already be initialized by other tests.
    # Start this scenario in a fresh interpreter with metrics enabled at startup.
    env = {
        **os.environ,
        "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "2",
        "SAIL_CLUSTER__WORKER_MAX_COUNT": "2",
        "SAIL_CLUSTER__WORKER_TASK_SLOTS": "64",
        "SAIL_TELEMETRY__EXPORT_METRICS": "true",
        "SAIL_TELEMETRY__EXPORTER__SYSTEM__ENABLED": "true",
        "SAIL_TELEMETRY__METRICS_EXPORT_INTERVAL_SECS": "1",
        "SAIL_TELEMETRY__METRICS_COLLECTION_INTERVAL_SECS": "1",
    }
    code = """
import runpy
import sys
from pathlib import Path
from pysail.testing.spark.session import spark_connect_server, spark_session_factory
checks = runpy.run_path(sys.argv[1])
with spark_connect_server() as server, spark_session_factory(server.remote) as sessions:
    session = sessions.create()
    session.sql("SET datafusion.execution.target_partitions = 8").collect()
    checks["_check_non_generating_allocation_rows"](session, Path(sys.argv[2]), sys.argv[3], distributed=True)
"""
    result = subprocess.run(
        [sys.executable, "-c", code, __file__, str(tmp_path), layout],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _check_non_generating_allocation_rows(spark, tmp_path, layout, *, distributed):
    name = "identity_allocation_" + uuid.uuid4().hex
    source = "identity_source_" + uuid.uuid4().hex
    path = tmp_path / name
    count = 8192
    spark.sql(f"""
        CREATE TABLE {name} (id BIGINT GENERATED BY DEFAULT AS IDENTITY, k BIGINT, v BIGINT)
        USING DELTA LOCATION '{escape_sql_string_literal(str(path))}'
        TBLPROPERTIES ('delta.enableDeletionVectors'='false')
    """).collect()
    try:
        if layout == "source-explicit":
            spark.sql(f"INSERT INTO {name} (id,k,v) VALUES (1,-1,7)").collect()  # noqa: S608
            # Touch the target row so the COW branch consumes its allocation
            # window too; an empty touched-file set bypasses that window.
            spark.range(-1, count + 1, numPartitions=8).selectExpr(
                "id AS k", "CASE WHEN id = -1 THEN 7L ELSE id*7 END AS v"
            ).createOrReplaceTempView(source)
            insert = f"""
                WHEN NOT MATCHED AND s.k < {count} THEN INSERT (id,k,v)
                  VALUES (CASE WHEN s.k % 2 = 0 THEN NULL ELSE 0L END,s.k,s.v)
                WHEN NOT MATCHED THEN INSERT (k,v) VALUES (s.k,s.v)
            """  # noqa: S608
            false_rows = count + 1
        else:
            partitions = 8 if layout == "eight-files" else 1
            spark.range(count, numPartitions=partitions).selectExpr(
                "id+1 AS id", "id AS k", "id*7 AS v"
            ).createOrReplaceTempView(source)
            spark.sql(f"SET datafusion.execution.target_partitions = {partitions}").collect()
            spark.sql(f"INSERT INTO {name} (id,k,v) SELECT id,k,v FROM {source}").collect()  # noqa: S608
            spark.sql("SET datafusion.execution.target_partitions = 8").collect()
            spark.createDataFrame([(0, -1), (count, -2)], "k long, v long").createOrReplaceTempView(source)
            insert = "WHEN NOT MATCHED THEN INSERT (k,v) VALUES (s.k,s.v)"
            false_rows = count
        before = _log(path)
        seed_files = [
            json.loads(line)["add"]
            for data in before.values()
            for line in data.splitlines()
            if "add" in json.loads(line)
        ]
        assert len(seed_files) == (8 if layout == "eight-files" else 1)
        if distributed:
            sessions = spark.sql(
                "SELECT session_id FROM system.session.sessions WHERE client_session_id = :id",
                args={"id": spark.session_id},
            ).collect()
            assert len(sessions) == 1
            session_id = sessions[0].session_id
            jobs_before = {
                str(row["job_id"])
                for row in spark.sql(
                    "SELECT job_id FROM system.execution.jobs WHERE session_id = :id", args={"id": session_id}
                )
                .toArrow()
                .to_pylist()
            }
        spark.sql(f"""
            MERGE INTO {name} t USING {source} s ON t.k=s.k
            WHEN MATCHED THEN UPDATE SET v=s.v {insert}
        """).collect()
        if distributed:
            windows = _allocation_window_rows(spark, session_id, jobs_before, false_rows + 1)
            for rows in windows.values():
                assert len([n for n in rows.values() if n]) > 1
                assert max(rows.values()) <= false_rows // 2 + 1
        result = spark.table(name).orderBy("k").collect()
        assert len(result) == false_rows + 1
        if layout == "source-explicit":
            assert result[0] == (1, -1, 7)
            assert result[-1] == (2, count, count * 7)
            assert all(row.id == (None if row.k % 2 == 0 else 0) for row in result[1:-1])
            expected_watermark = 2
        else:
            assert result[0] == (1, 0, -1)
            assert result[-1] == (count + 1, count, -2)
            assert all(row == (row.k + 1, row.k, row.k * 7) for row in result[1:-1])
            expected_watermark = count + 1
        assert _watermark(path) == expected_watermark
        changes = [
            json.loads(line)
            for version, contents in _log(path).items()
            if version not in before
            for line in contents.splitlines()
        ]
        assert len([action for action in changes if "remove" in action]) == 1
    finally:
        spark.catalog.dropTempView(source)
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()
