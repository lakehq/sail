"""Identity MERGE invariants.

Assertions avoid assumptions about partition-stride numbering and row-to-ID ordering.
Explicit BY DEFAULT values retain Sail's existing watermark policy.
"""

import json
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
@pytest.mark.parametrize("start,step", [(10, 3), (0, -2)])
def test_merge_identity_lifecycle(spark, identity_table, mode, start, step):
    name, path = identity_table(mode=mode, start=start, step=step)
    spark.sql(f"INSERT INTO {name} (k,v) VALUES (1,10)").collect()
    before = _watermark(path)
    spark.sql(f"""
        MERGE INTO {name} t USING (SELECT 1 AS k,11 AS v UNION ALL SELECT 2,22) s ON t.k=s.k
        WHEN MATCHED THEN UPDATE SET v=s.v
        WHEN NOT MATCHED THEN INSERT (k,v) VALUES (s.k,s.v)
    """).collect()
    rows = spark.table(name).orderBy("k").collect()
    assert rows[0] == (start, 1, 11)
    generated = rows[1].id
    assert (generated - start) % step == 0 and (generated - before) * step > 0
    assert _watermark(path) == generated
    merge_commit = [json.loads(line) for line in list(_log(path).values())[-1].splitlines()]
    assert any("add" in action for action in merge_commit)
    assert any("metaData" in action for action in merge_commit)
    spark.sql(f"INSERT INTO {name} (k,v) VALUES (3,33)").collect()
    reopened = spark.read.format("delta").load(str(path)).orderBy("k").collect()
    assert [row.k for row in reopened] == [1, 2, 3]
    assert len({row.id for row in reopened}) == 3
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
    """).collect()
    assert spark.table(name).orderBy("k").collect() == [(42, 1, 10), (None, 2, 20)]
    assert _watermark(path) == 43  # Existing Sail policy: round explicit values forward.
    spark.sql(f"INSERT INTO {name} (k,v) VALUES (3,30)").collect()
    assert spark.table(name).where("k=3").first().id == 46


@pytest.mark.parametrize("mode", ["ALWAYS", "BY DEFAULT"])
@pytest.mark.parametrize(
    "clause,error",
    [
        ("WHEN MATCHED THEN UPDATE SET id=t.id", "UPDATE cannot assign Delta identity"),
        ("WHEN MATCHED THEN UPDATE SET *", "UPDATE cannot assign Delta identity"),
        ("WHEN NOT MATCHED BY SOURCE THEN UPDATE SET id=t.id", "UPDATE cannot assign Delta identity"),
        ("WHEN NOT MATCHED THEN INSERT (id,k,v) VALUES (DEFAULT,s.k,s.v)", "DEFAULT for an identity"),
    ],
)
def test_merge_identity_assignments_rejected(spark, identity_table, mode, clause, error):
    name, path = identity_table(mode=mode)
    spark.sql(f"INSERT INTO {name} (k,v) VALUES (1,10)").collect()
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
        """).collect()
    assert _state(spark, name, path) == before


@pytest.mark.parametrize("evolution", [False, True])
def test_merge_identity_star_omission_and_evolution_fail_closed(spark, identity_table, evolution):
    name, path = identity_table(mode="BY DEFAULT")
    before = _state(spark, name, path)
    with pytest.raises(Exception, match="schema evolution with identity|INSERT \\* omitting identity"):
        spark.sql(f"""
            MERGE {"WITH SCHEMA EVOLUTION" if evolution else ""} INTO {name} t
            USING (SELECT 1 AS k,10 AS v) s ON t.k=s.k WHEN NOT MATCHED THEN INSERT *
        """).collect()
    assert _state(spark, name, path) == before


@pytest.mark.parametrize("matched", [False, True], ids=["fast-append", "copy-on-write"])
def test_merge_multiple_insert_clauses_share_allocation(spark, identity_table, matched):
    name, path = identity_table(
        schema="""
        id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 5 INCREMENT BY 5),
        decreasing BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 0 INCREMENT BY -3),
        k INT, v INT
    """
    )
    spark.sql(f"INSERT INTO {name} (k,v) VALUES (0,100)").collect()
    source = "identity_source_" + uuid.uuid4().hex
    spark.range(0, 129, numPartitions=8).selectExpr("CAST(id AS INT) AS k").repartition(8).createOrReplaceTempView(
        source
    )
    try:
        update = "WHEN MATCHED THEN UPDATE SET v=101" if matched else ""
        spark.sql(f"""
            MERGE INTO {name} t USING {source} s ON t.k=s.k {update}
            WHEN NOT MATCHED AND s.k % 3 = 0 THEN INSERT (decreasing,k,v) VALUES (1000L+s.k,s.k,1)
            WHEN NOT MATCHED AND s.k % 2 = 0 THEN INSERT (k,v) VALUES (s.k,2)
            WHEN NOT MATCHED THEN INSERT (k,v) VALUES (s.k,3)
        """).collect()
        rows = spark.table(name).orderBy("k").collect()
        assert len(rows) == 129
        assert len({row.id for row in rows}) == 129
        assert all((row.id - 5) % 5 == 0 for row in rows)
        assert rows[0].id == 5 and rows[0].decreasing == 0
        generated = [row.decreasing for row in rows if row.k % 3 != 0]
        assert len(set(generated)) == len(generated)
        assert all(value < 0 and value % -3 == 0 for value in generated)
        assert all(row.decreasing == 1000 + row.k for row in rows if row.k and row.k % 3 == 0)
        assert all(row.v == (1 if row.k % 3 == 0 else 2 if row.k % 2 == 0 else 3) for row in rows[1:])
        assert _watermark(path) == max(row.id for row in rows)
        assert _watermark(path, "decreasing") == min(generated)
    finally:
        spark.catalog.dropTempView(source)


@pytest.mark.parametrize("start,step", [(9223372036854775807, 1), (-9223372036854775808, -1)])
@pytest.mark.parametrize(
    "source,condition",
    [
        ("SELECT 1 AS k,11 AS v", "true"),
        ("SELECT 2 AS k,22 AS v", "false"),
        ("SELECT 2 AS k,22 AS v WHERE false", "true"),
    ],
)
def test_merge_without_generated_rows_allows_exhausted_watermark(spark, identity_table, start, step, source, condition):
    name, path = identity_table(start=start, step=step)
    spark.sql(f"INSERT INTO {name} (k,v) VALUES (1,10)").collect()
    spark.sql(f"""
        MERGE INTO {name} t USING ({source}) s ON t.k=s.k
        WHEN MATCHED THEN UPDATE SET v=s.v
        WHEN NOT MATCHED AND {condition} THEN INSERT (k,v) VALUES (s.k,s.v)
    """).collect()
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
    with pytest.raises(Exception, match="change data feed|min/max statistics"):
        spark.sql(f"""
            MERGE INTO {name} t USING (SELECT 1 AS k,10 AS v) s ON t.k=s.k
            WHEN NOT MATCHED THEN INSERT (k,v) VALUES (s.k,s.v)
        """).collect()
    assert _state(spark, name, path) == before
    assert set(path.rglob("*.parquet")) == files_before


@pytest.mark.parametrize("start,step", [(1, 1), (0, -2)])
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
        with pytest.raises(Exception, match="Delta identity MERGE requires usable.*statistics"):
            spark.sql(f"""
                MERGE INTO {name} t USING {source} s ON t.k=s.k
                WHEN NOT MATCHED AND s.k < {limit} THEN INSERT (id,k) VALUES (NULL,s.k)
                WHEN NOT MATCHED THEN INSERT (k) VALUES (s.k)
            """).collect()
        assert _state(spark, name, path) == before
        assert _watermark(path) is None
        assert spark.read.format("delta").load(str(path)).collect() == []

        # Cluster retries may leave several uncommitted attempts. Every file must have
        # the actual two-group layout; none may appear in the committed table.
        files = list(path.glob("*.parquet"))
        assert files
        for file in files:
            parquet = pq.ParquetFile(file)
            assert parquet.metadata.num_row_groups == 2
            null_group = parquet.metadata.row_group(0)
            value_group = parquet.metadata.row_group(1)
            assert null_group.num_rows == limit
            assert null_group.column(0).statistics.null_count == limit
            assert not null_group.column(0).statistics.has_min_max
            assert value_group.num_rows == 1
            assert value_group.column(0).statistics.null_count == 0
            assert value_group.column(0).statistics.min == value_group.column(0).statistics.max == start

        for offset in range(2):
            spark.sql(f"INSERT INTO {name} (k) VALUES ({limit + 1 + offset})").collect()
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


@pytest.mark.parametrize("start,step", [(1, 1), (0, -2)])
def test_merge_all_null_and_empty_inputs_do_not_require_identity_bounds(spark, identity_table, start, step):
    name, path = identity_table(mode="BY DEFAULT", start=start, step=step)
    before = _state(spark, name, path)
    spark.sql(f"""
        MERGE INTO {name} t USING (SELECT 1 AS k WHERE false) s ON t.k=s.k
        WHEN NOT MATCHED THEN INSERT (k) VALUES (s.k)
    """).collect()
    assert _state(spark, name, path) == before
    spark.sql(f"""
        MERGE INTO {name} t USING (SELECT 1 AS k UNION ALL SELECT 2) s ON t.k=s.k
        WHEN NOT MATCHED AND s.k < 3 THEN INSERT (id,k) VALUES (NULL,s.k)
        WHEN NOT MATCHED THEN INSERT (k) VALUES (s.k)
    """).collect()
    assert spark.table(name).orderBy("k").collect() == [(None, 1, None), (None, 2, None)]
    assert _watermark(path) is None
    adds = [json.loads(line)["add"] for line in list(_log(path).values())[-1].splitlines() if "add" in json.loads(line)]
    assert adds
    for add in adds:
        stats = json.loads(add["stats"])
        assert stats["numRecords"] > 0
        assert stats["nullCount"]["id"] == stats["numRecords"]
        assert "id" not in stats.get("minValues", {}) and "id" not in stats.get("maxValues", {})
        assert stats.get("tightBounds", True) is True
        assert add.get("deletionVector") is None
    spark.sql(f"INSERT INTO {name} (k) VALUES (3)").collect()
    assert spark.table(name).where("k=3").first().id == start
    assert _watermark(path) == start


def test_merge_generation_failure_does_not_commit(spark, identity_table):
    name, path = identity_table(start=9223372036854775806, step=1)
    spark.sql(f"INSERT INTO {name} (k,v) VALUES (0,0)").collect()
    before = _state(spark, name, path)
    with pytest.raises(Exception, match="DELTA_IDENTITY_COLUMN_VALUE_OVERFLOW"):
        spark.sql(f"""
            MERGE INTO {name} t USING (SELECT 1 AS k,10 AS v UNION ALL SELECT 2,20) s ON t.k=s.k
            WHEN NOT MATCHED THEN INSERT (k,v) VALUES (s.k,s.v)
        """).collect()
    assert _state(spark, name, path) == before
