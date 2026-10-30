import json
from contextlib import nullcontext
from datetime import UTC, date, datetime

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import Row

from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.testing.spark.utils.files import assert_file_lifecycle, get_data_files
from pysail.testing.spark.utils.jvm import classic_spark_mode
from pysail.testing.spark.utils.sql import escape_sql_identifier, escape_sql_string_literal


@pytest.fixture(scope="module")
def delta_test_data():
    """Test data"""
    return [
        Row(id=10, event="A", score=0.98),
        Row(id=11, event="B", score=0.54),
        Row(id=12, event="A", score=0.76),
    ]


@pytest.fixture(scope="module")
def expected_pandas_df():
    """Expected pandas DataFrame"""
    return pd.DataFrame({"id": [10, 11, 12], "event": ["A", "B", "A"], "score": [0.98, 0.54, 0.76]}).astype(
        {"id": "int32", "event": "string", "score": "float64"}
    )


def test_delta_io_write_with_input_partitions(spark, tmp_path):
    delta_path = tmp_path / "delta_table"

    spark.range(1).write.format("delta").save(str(delta_path))
    assert spark.read.format("delta").load(f"{delta_path}").count() == 1

    spark.range(1, 101, 1, 10).write.format("delta").mode("overwrite").save(str(delta_path))
    assert spark.read.format("delta").load(f"{delta_path}").count() == 100  # noqa: PLR2004


def test_delta_io_basic_overwrite_and_read(spark, delta_test_data, expected_pandas_df, tmp_path):
    """Test basic Delta Lake write and read operations"""
    delta_path = tmp_path / "delta_table"

    df = spark.createDataFrame(delta_test_data)

    df.write.format("delta").mode("overwrite").save(str(delta_path))

    result_df = spark.read.format("delta").load(f"{delta_path}").sort("id")

    assert_frame_equal(
        result_df.toPandas(), expected_pandas_df.sort_values("id").reset_index(drop=True), check_dtype=False
    )


def test_delta_io_create_table_with_sql(spark, delta_test_data, expected_pandas_df, tmp_path):
    """Test creating Delta table with SQL and querying"""
    delta_path = tmp_path / "delta_table"
    delta_table_path = f"{delta_path}"

    df = spark.createDataFrame(delta_test_data)

    df.write.format("delta").mode("overwrite").save(str(delta_path))

    spark.sql(f"CREATE TABLE my_delta USING delta LOCATION '{escape_sql_string_literal(delta_table_path)}'")

    try:
        result_df = spark.sql("SELECT * FROM my_delta").sort("id")

        assert_frame_equal(
            result_df.toPandas(), expected_pandas_df.sort_values("id").reset_index(drop=True), check_dtype=False
        )
    finally:
        spark.sql("DROP TABLE IF EXISTS my_delta")


def test_delta_io_create_table_materializes_empty_log(spark, tmp_path):
    delta_path = tmp_path / "delta_empty_table"
    table_name = "delta_empty_materialized_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (
              id INT,
              name STRING
            )
            USING DELTA
            LOCATION '{escape_sql_string_literal(str(delta_path))}'
            """
        )

        commit0 = delta_path / "_delta_log" / "00000000000000000000.json"
        assert commit0.exists()
        actions = [json.loads(line) for line in commit0.read_text(encoding="utf-8").splitlines()]
        assert any("protocol" in action for action in actions)
        assert any("metaData" in action for action in actions)
        assert not any("add" in action for action in actions)
        assert spark.sql(f"SELECT id, name FROM {table_name} ORDER BY id").collect() == []  # noqa: S608

        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'one')")  # noqa: S608
        commit1 = delta_path / "_delta_log" / "00000000000000000001.json"
        assert commit1.exists()
        assert spark.sql(f"SELECT id, name FROM {table_name} ORDER BY id").collect() == [  # noqa: S608
            Row(id=1, name="one")
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_delta_io_create_table_if_not_exists_does_not_materialize_new_location(spark, tmp_path):
    delta_path = tmp_path / "delta_if_not_exists_table"
    alternate_path = tmp_path / "delta_if_not_exists_alternate"
    table_name = "delta_if_not_exists_materialized_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (
              id INT,
              name STRING
            )
            USING DELTA
            LOCATION '{escape_sql_string_literal(str(delta_path))}'
            """
        )
        assert (delta_path / "_delta_log" / "00000000000000000000.json").exists()

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
              id INT,
              name STRING
            )
            USING DELTA
            LOCATION '{escape_sql_string_literal(str(alternate_path))}'
            """
        )

        assert not (alternate_path / "_delta_log").exists()
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_delta_io_create_table_rejects_mismatched_existing_log_schema(spark, tmp_path):
    delta_path = tmp_path / "delta_existing_schema"
    table_name = "delta_existing_schema_mismatch_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.createDataFrame([Row(id=1, name="one")]).write.format("delta").save(str(delta_path))
    try:
        with pytest.raises(Exception, match="different schema"):
            spark.sql(
                f"""
                CREATE TABLE {table_name} (
                  id STRING,
                  name STRING
                )
                USING DELTA
                LOCATION '{escape_sql_string_literal(str(delta_path))}'
                """
            )
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_delta_io_create_table_rejects_mismatched_existing_log_nullability(spark, tmp_path):
    delta_path = tmp_path / "delta_existing_nullability"
    table_name = "delta_existing_nullability_mismatch_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.createDataFrame([Row(id=1, name="one")]).write.format("delta").save(str(delta_path))
    try:
        with pytest.raises(Exception, match=r"different schema.*nullable"):
            spark.sql(
                f"""
                CREATE TABLE {table_name} (
                  id BIGINT NOT NULL,
                  name STRING
                )
                USING DELTA
                LOCATION '{escape_sql_string_literal(str(delta_path))}'
                """
            )
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_delta_io_create_or_replace_existing_table_with_same_schema_clears_rows(spark, tmp_path):
    delta_path = tmp_path / "delta_replace_existing"
    table_name = "delta_create_or_replace_existing_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (
              id BIGINT,
              name STRING
            )
            USING DELTA
            LOCATION '{escape_sql_string_literal(str(delta_path))}'
            """
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'one')")  # noqa: S608

        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {table_name} (
              id BIGINT,
              name STRING
            )
            USING DELTA
            LOCATION '{escape_sql_string_literal(str(delta_path))}'
            """
        )

        rows = spark.sql(f"SELECT id, name FROM {table_name} ORDER BY id").collect()  # noqa: S608
        assert rows == []
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_delta_io_create_or_replace_existing_table_replaces_metadata_and_clears_rows(spark, tmp_path):
    delta_path = tmp_path / "delta_replace_existing_clears_rows"
    table_name = "delta_create_or_replace_existing_clear_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (
              id BIGINT,
              name STRING
            )
            USING DELTA
            LOCATION '{escape_sql_string_literal(str(delta_path))}'
            """
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'one')")  # noqa: S608

        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {table_name} (
              replacement STRING
            )
            USING DELTA
            LOCATION '{escape_sql_string_literal(str(delta_path))}'
            """
        )

        rows = spark.sql(f"SELECT replacement FROM {table_name}").collect()  # noqa: S608
        assert rows == []

        latest_log = sorted((delta_path / "_delta_log").glob("*.json"))[-1]
        latest_actions = [json.loads(line) for line in latest_log.read_text(encoding="utf-8").splitlines()]
        metadata_actions = [action["metaData"] for action in latest_actions if "metaData" in action]
        remove_actions = [action["remove"] for action in latest_actions if "remove" in action]
        assert len(metadata_actions) == 1
        assert json.loads(metadata_actions[0]["schemaString"])["fields"][0]["name"] == "replacement"
        assert [action["path"] for action in remove_actions]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_delta_io_create_or_replace_existing_table_requires_schema(spark, tmp_path):
    delta_path = tmp_path / "delta_replace_existing_no_schema"
    table_name = "delta_create_or_replace_no_schema_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (
              id BIGINT
            )
            USING DELTA
            LOCATION '{escape_sql_string_literal(str(delta_path))}'
            """
        )

        with pytest.raises(Exception, match="schema is not provided"):
            spark.sql(
                f"""
                CREATE OR REPLACE TABLE {table_name}
                USING DELTA
                LOCATION '{escape_sql_string_literal(str(delta_path))}'
                """
            )
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_delta_io_replace_table_missing_errors_without_materializing_log(spark, tmp_path):
    delta_path = tmp_path / "delta_replace_missing"
    table_name = "delta_replace_missing_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        with pytest.raises(Exception, match=r"(?i)(not found|does not exist|table_or_view_not_found)"):
            spark.sql(
                f"""
                REPLACE TABLE {table_name} (
                  id BIGINT
                )
                USING DELTA
                LOCATION '{escape_sql_string_literal(str(delta_path))}'
                """
            )

        assert not (delta_path / "_delta_log").exists()
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_delta_io_replace_table_as_select_missing_errors_without_materializing_log(spark, tmp_path):
    delta_path = tmp_path / "delta_replace_as_select_missing"
    table_name = "delta_replace_as_select_missing_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        with pytest.raises(Exception, match=r"(?i)(not found|does not exist|table_or_view_not_found)"):
            spark.sql(
                f"""
                REPLACE TABLE {table_name}
                USING DELTA
                LOCATION '{escape_sql_string_literal(str(delta_path))}'
                AS SELECT 1 AS id
                """
            )

        assert not (delta_path / "_delta_log").exists()
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_delta_io_append_mode(spark, delta_test_data, tmp_path):
    """Test Delta Lake append mode"""
    delta_path = tmp_path / "delta_table"
    delta_table_path = f"{delta_path}"

    df1 = spark.createDataFrame(delta_test_data)

    df1.write.format("delta").mode("overwrite").save(str(delta_path))

    # Keep lifecycle assertion in Python tests (BDD covers structural layout separately).
    files_v0 = set(get_data_files(str(delta_path)))
    assert len(files_v0) > 0, "Initial write should create files"

    append_data = [
        Row(id=13, event="C", score=0.89),
        Row(id=14, event="D", score=0.67),
    ]
    df2 = spark.createDataFrame(append_data)

    df2.write.format("delta").mode("append").save(str(delta_path))

    files_v1 = set(get_data_files(str(delta_path)))
    assert_file_lifecycle(files_v0, files_v1, "append")

    result_df = spark.read.format("delta").load(delta_table_path).sort("id")

    expected_data = pd.DataFrame(
        {
            "id": [10, 11, 12, 13, 14],
            "event": ["A", "B", "A", "C", "D"],
            "score": [0.98, 0.54, 0.76, 0.89, 0.67],
        }
    ).astype({"id": "int32", "event": "string", "score": "float64"})

    assert_frame_equal(result_df.toPandas(), expected_data.sort_values("id").reset_index(drop=True), check_dtype=False)


def test_delta_io_overwrite_mode(spark, delta_test_data, tmp_path):
    """Test Delta Lake overwrite mode"""
    delta_path = tmp_path / "delta_table"
    delta_table_path = f"{delta_path}"

    df1 = spark.createDataFrame(delta_test_data)

    df1.write.format("delta").mode("overwrite").save(str(delta_path))

    files_v0 = set(get_data_files(str(delta_path)))
    assert len(files_v0) > 0, "Initial write should create files"

    new_data = [
        Row(id=20, event="X", score=0.95),
        Row(id=21, event="Y", score=0.88),
    ]
    df2 = spark.createDataFrame(new_data)

    df2.write.format("delta").mode("overwrite").save(str(delta_path))

    files_v1 = set(get_data_files(str(delta_path)))
    assert len(files_v1) > len(files_v0), "Overwrite should create new data files"

    result_df = spark.read.format("delta").load(delta_table_path).sort("id")

    expected_data = pd.DataFrame({"id": [20, 21], "event": ["X", "Y"], "score": [0.95, 0.88]}).astype(
        {"id": "int32", "event": "string", "score": "float64"}
    )

    assert_frame_equal(result_df.toPandas(), expected_data.sort_values("id").reset_index(drop=True), check_dtype=False)


REPLACE_WHERE_CHECK = "spark.databricks.delta.replaceWhere.constraintCheck.enabled"


@pytest.fixture
def replace_where_check(spark, request):
    previous = spark.conf.get(REPLACE_WHERE_CHECK, None)
    value = getattr(request, "param", None)
    if value is None:
        spark.conf.unset(REPLACE_WHERE_CHECK)
    else:
        spark.conf.set(REPLACE_WHERE_CHECK, value)
    try:
        yield
    finally:
        if previous is None:
            spark.conf.unset(REPLACE_WHERE_CHECK)
        else:
            spark.conf.set(REPLACE_WHERE_CHECK, previous)


def _delta_commits(path):
    return {file.name: file.read_bytes() for file in sorted((path / "_delta_log").glob("*.json"))}


def _delta_metadata(path):
    return next(
        action["metaData"]
        for commit in reversed(list(_delta_commits(path).values()))
        for line in commit.splitlines()
        if "metaData" in (action := json.loads(line))
    )


def _conditional_overwrite(spark, frame, path, table, api, predicate):
    writer = frame.write.format("delta").mode("overwrite").option("replaceWhere", predicate)
    if api == "path":
        writer.save(str(path))
    elif api == "sink":
        writer.option("path", str(path)).save()
    elif api == "table":
        writer.saveAsTable(table)
    elif api == "v2":
        frame.writeTo(table).overwrite(F.expr(predicate))
    else:
        view = f"{table}_input"
        frame.createOrReplaceTempView(view)
        try:
            spark.sql(f"INSERT INTO {table} REPLACE WHERE {predicate} SELECT * FROM {view}")  # noqa: S608
        finally:
            spark.catalog.dropTempView(view)


@pytest.mark.parametrize("api", ["table", "path", "sink", "v2", "sql"])
@pytest.mark.parametrize("partitioned", [False, True], ids=["data", "partition"])
@pytest.mark.usefixtures("replace_where_check")
def test_delta_io_conditional_overwrite_validates_input(spark, tmp_path, api, partitioned):
    path = tmp_path / "conditional_input"
    table = "delta_conditional_input"
    partition = "PARTITIONED BY (p)" if partitioned else ""
    spark.sql(
        f"CREATE TABLE {table} (p INT, v INT) USING DELTA {partition} LOCATION '{escape_sql_string_literal(str(path))}'"
    )
    try:
        spark.sql("INSERT INTO delta_conditional_input VALUES (NULL, 0), (1, 10), (2, 11)")
        before = _delta_commits(path)
        initial = spark.table(table).orderBy("v").collect()
        # A matching row must not hide an invalid row in the same input.
        invalid = spark.createDataFrame([(1, 20), (2, 21)], "p INT, v INT")
        with pytest.raises(Exception, match="DELTA_REPLACE_WHERE_MISMATCH"):
            _conditional_overwrite(spark, invalid, path, table, api, "p = 1")
        assert _delta_commits(path) == before
        assert spark.table(table).orderBy("v").collect() == initial

        valid = spark.createDataFrame([(1, 20)], "p INT, v INT")
        _conditional_overwrite(spark, valid, path, table, api, "p = 1")
        assert spark.table(table).orderBy("v").collect() == [(None, 0), (2, 11), (1, 20)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize(
    ("predicate", "incoming", "expected", "error"),
    [
        ("p = 1", [], [(None, 0), (2, 11)], None),
        ("p = 1", [(None, 20)], None, "DELTA_REPLACE_WHERE_MISMATCH"),
        ("p IS NULL", [(None, 20)], [(1, 10), (2, 11), (None, 20)], None),
        ("p = 1 OR NULL", [(2, 20)], None, "DELTA_REPLACE_WHERE_MISMATCH"),
        ("true", [(1, 20)], [(1, 20)], None),
        ("false", [(1, 20)], [(None, 0), (1, 10), (2, 11), (1, 20)], None),
        ("p = 1 AND false", [(1, 20)], [(None, 0), (1, 10), (2, 11), (1, 20)], None),
        ("false AND p = 1", [(2, 20)], None, "DELTA_REPLACE_WHERE_MISMATCH"),
        ("p = 1 AND NULL", [(1, 20)], [(None, 0), (1, 10), (2, 11), (1, 20)], None),
    ],
    ids=["empty", "null-input", "is-null", "or-null", "true", "false", "and-false", "and-invalid", "and-null"],
)
@pytest.mark.usefixtures("replace_where_check")
def test_delta_io_replace_where_constraint_selection(spark, tmp_path, predicate, incoming, expected, error):
    # Constraint extraction selects column-bearing conjuncts.
    path = tmp_path / "constraint_selection"
    initial = [(None, 0), (1, 10), (2, 11)]
    spark.createDataFrame(initial, "p INT, v INT").write.format("delta").save(str(path))
    before = _delta_commits(path)
    frame = spark.createDataFrame(incoming, "p INT, v INT")
    if error:
        with pytest.raises(Exception, match=error):
            _conditional_overwrite(spark, frame, path, None, "path", predicate)
        assert _delta_commits(path) == before
        expected = initial
    else:
        _conditional_overwrite(spark, frame, path, None, "path", predicate)
    assert spark.read.format("delta").load(str(path)).orderBy("v").collect() == expected


@pytest.mark.usefixtures("replace_where_check")
def test_delta_io_save_as_table_replace_where_preserves_metadata(spark, tmp_path):
    path = tmp_path / "table_identity"
    table = "delta_replace_where_identity"
    spark.sql(
        f"CREATE TABLE {table} (p INT NOT NULL, v INT) USING DELTA PARTITIONED BY (p) "
        f"LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES ('owner.tag' = 'retained')"
    )
    try:
        spark.sql("INSERT INTO delta_replace_where_identity VALUES (1, 10), (2, 11)")
        metadata = _delta_metadata(path)
        schema = spark.table(table).schema
        description = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
        # Reordered columns exercise saveAsTable's name-based matching.
        frame = spark.createDataFrame([(20, 1)], "v INT, p INT")
        frame.write.format("DELTA").mode("overwrite").option("RePlAcEwHeRe", "p = 1").saveAsTable(table)
        assert spark.table(table).orderBy("v").collect() == [(2, 11), (1, 20)]
        assert spark.table(table).schema == schema
        assert spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect() == description
        assert _delta_metadata(path) == metadata

        before = _delta_commits(path)
        for frame, options, partition in [
            (spark.createDataFrame([(None, 21)], "p INT, v INT"), {}, None),
            (spark.createDataFrame([(1, 21)], "p INT, v INT"), {}, "v"),
            (spark.createDataFrame([(1, 21)], "p INT, v INT"), {"partitionOverwriteMode": "dynamic"}, None),
        ]:
            writer = frame.write.format("delta").mode("overwrite").option("replaceWhere", "p = 1").options(**options)
            if partition:
                writer = writer.partitionBy(partition)
            with pytest.raises(Exception, match=r"(?i)(null|partition)"):
                writer.saveAsTable(table)
            assert _delta_commits(path) == before
            assert spark.table(table).schema == schema
            assert _delta_metadata(path) == metadata
        assert spark.table(table).orderBy("v").collect() == [(2, 11), (1, 20)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize(
    ("existing", "provider", "predicate", "method"),
    [
        (None, "delta", "p = 1", "saveAsTable"),
        ("parquet", "parquet", "p = 1", "saveAsTable"),
        ("delta", "parquet", "p = 1", "saveAsTable"),
        ("delta", "delta", None, "saveAsTable"),
        ("delta", "delta", "p = 1", "insertInto"),
    ],
    ids=["create", "non-delta", "provider-replacement", "ordinary-overwrite", "insert-into"],
)
def test_delta_io_v1_overwrite_intent_boundaries(spark, tmp_path, existing, provider, predicate, method):
    path = tmp_path / "overwrite_intent"
    table = "delta_overwrite_intent"
    try:
        if existing:
            spark.sql(
                f"CREATE TABLE {table} (p INT, v INT) USING {existing} LOCATION '{escape_sql_string_literal(str(path))}'"
            )
            spark.sql("INSERT INTO delta_overwrite_intent VALUES (1, 10), (2, 11)")
        writer = spark.createDataFrame([(2, 20)], "p INT, v INT").write.format(provider).mode("overwrite")
        if predicate:
            writer = writer.option("replaceWhere", predicate)
        if not existing:
            writer = writer.option("path", str(path))
        getattr(writer, method)(table)
        assert spark.table(table).collect() == [(2, 20)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.usefixtures("replace_where_check")
def test_delta_io_replace_where_check_switch(spark, tmp_path):
    path = tmp_path / "check_switch"
    table = "delta_replace_where_check_switch"
    spark.sql(
        f"CREATE TABLE {table} (p INT NOT NULL, v INT) USING DELTA LOCATION '{escape_sql_string_literal(str(path))}'"
    )
    try:
        spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT positive CHECK (v >= 0)")
        spark.sql("INSERT INTO delta_replace_where_check_switch VALUES (1, 10), (2, 11)")
        outside = spark.createDataFrame([(2, 20)], "p INT, v INT")
        for setting in [None, "true", "false", "true"]:
            if setting is None:
                spark.conf.unset(REPLACE_WHERE_CHECK)
            else:
                spark.conf.set(REPLACE_WHERE_CHECK, setting)
            writer = outside.write.format("delta").mode("overwrite").option("replaceWhere", "p = 1")
            # This is a session setting, not a per-write option.
            writer = writer.option(REPLACE_WHERE_CHECK, "false")
            before = _delta_commits(path)
            if setting == "false":
                writer.saveAsTable(table)
                assert spark.table(table).orderBy("v").collect() == [(2, 11), (2, 20)]
                for rows, schema, error in [
                    ([(None, 30)], "p INT, v INT", "NOT_NULL"),
                    ([(2, -1)], "p INT, v INT", "CONSTRAINT"),
                    ([(2,)], "p INT", "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES"),
                ]:
                    before_failure = _delta_commits(path)
                    with pytest.raises(Exception, match=error):
                        _conditional_overwrite(
                            spark, spark.createDataFrame(rows, schema), path, table, "table", "p = 1"
                        )
                    assert _delta_commits(path) == before_failure
            else:
                with pytest.raises(Exception, match="DELTA_REPLACE_WHERE_MISMATCH"):
                    writer.saveAsTable(table)
                assert _delta_commits(path) == before
        assert REPLACE_WHERE_CHECK not in _delta_metadata(path)["configuration"]
        before = _delta_commits(path)

        def overwrite_with_invalid_setting():
            spark.conf.set(REPLACE_WHERE_CHECK, "not-a-boolean")
            _conditional_overwrite(spark, outside, path, table, "v2", "p = 1")

        with pytest.raises(Exception, match="boolean"):
            overwrite_with_invalid_setting()
        assert _delta_commits(path) == before
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("replace_where_check", [None, "false"], indirect=True)
@pytest.mark.usefixtures("replace_where_check")
def test_delta_io_save_as_table_replace_where_rejects_extra_columns(spark, tmp_path):
    path = tmp_path / "extra_columns"
    table = "delta_replace_where_extra_columns"
    spark.sql(
        f"CREATE TABLE {table} (p INT NOT NULL, v INT) USING DELTA LOCATION '{escape_sql_string_literal(str(path))}'"
    )
    try:
        spark.sql("INSERT INTO delta_replace_where_extra_columns VALUES (1, 10), (2, 11)")
        before = _delta_commits(path)
        with pytest.raises(Exception, match=r"(?i)(schema|column)"):
            spark.createDataFrame([(1, 20, 30)], "p INT, v INT, extra INT").write.format("delta").mode(
                "overwrite"
            ).option("replaceWhere", "p = 1").option("overwriteSchema", "true").saveAsTable(table)
        assert _delta_commits(path) == before
        assert spark.table(table).orderBy("v").collect() == [(1, 10), (2, 11)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("api", ["path", "table"])
@pytest.mark.usefixtures("replace_where_check")
def test_delta_io_replace_where_late_failure_does_not_commit(spark, tmp_path, api):
    path = tmp_path / "late_failure"
    table = "delta_replace_where_late_failure"
    spark.sql(f"CREATE TABLE {table} (p INT, v INT) USING DELTA LOCATION '{escape_sql_string_literal(str(path))}'")
    try:
        spark.sql("INSERT INTO delta_replace_where_late_failure VALUES (1, 10), (2, 11)")
        before = _delta_commits(path)
        active_files = {file: file.read_bytes() for file in path.rglob("*.parquet")}
        frame = spark.range(0, 50000, 1, 4).selectExpr(
            "CASE WHEN id = 49999 THEN 2 ELSE 1 END AS p", "CAST(id AS INT) AS v"
        )
        with pytest.raises(Exception, match="DELTA_REPLACE_WHERE_MISMATCH"):
            _conditional_overwrite(spark, frame, path, table, api, "p = 1")
        assert _delta_commits(path) == before
        assert all(file.read_bytes() == contents for file, contents in active_files.items())
        assert spark.read.format("delta").load(str(path)).orderBy("v").collect() == [(1, 10), (2, 11)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.integration
@pytest.mark.usefixtures("replace_where_check")
def test_delta_io_replace_where_cross_engine_lifecycle(delta_jvm_spark, spark, tmp_path):
    path = tmp_path / "mapped_replace_where"
    table = "delta_mapped_replace_where"
    jvm_table = f"delta.`{escape_sql_identifier(str(path))}`"
    delta_jvm_spark.sql(
        f"CREATE TABLE {jvm_table} (p INT, v INT) USING DELTA PARTITIONED BY (p) "
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableChangeDataFeed' = 'true')"
    )
    # The generated path in jvm_table is escaped as a SQL identifier above.
    delta_jvm_spark.sql(f"INSERT INTO {jvm_table} VALUES (NULL, 0), (1, 10), (2, 11)")  # noqa: S608
    spark.sql(f"CREATE TABLE {table} USING DELTA LOCATION '{escape_sql_string_literal(str(path))}'")
    try:
        metadata = _delta_metadata(path)
        incoming = spark.createDataFrame([(1, 20)], "p INT, v INT")
        _conditional_overwrite(spark, incoming, path, table, "table", "p = 1")
        with classic_spark_mode():
            assert delta_jvm_spark.read.format("delta").load(str(path)).orderBy("v").collect() == [
                (None, 0),
                (2, 11),
                (1, 20),
            ]
            delta_jvm_spark.createDataFrame([(2, 21)], "p INT, v INT").writeTo(jvm_table).overwrite(F.expr("p = 2"))
        expected = [(None, 0), (1, 20), (2, 21)]
        for engine in [spark, delta_jvm_spark]:
            with classic_spark_mode() if engine is delta_jvm_spark else nullcontext():
                assert engine.read.format("delta").load(str(path)).orderBy("v").collect() == expected
                before = _delta_commits(path)
                with pytest.raises(Exception, match="DELTA_REPLACE_WHERE_MISMATCH"):
                    engine.createDataFrame([(1, 30), (None, 31)], "p INT, v INT").write.format("delta").mode(
                        "overwrite"
                    ).option("replaceWhere", "p = 1").save(str(path))
                assert _delta_commits(path) == before
                changes = (
                    engine.read.format("delta")
                    .option("readChangeFeed", "true")
                    .option("startingVersion", 2)
                    .option("endingVersion", 3)
                    .load(str(path))
                    .select("p", "v", "_change_type", "_commit_version")
                    .orderBy("_commit_version", "v")
                    .collect()
                )
                assert changes == [
                    (1, 10, "delete", 2),
                    (1, 20, "insert", 2),
                    (2, 11, "delete", 3),
                    (2, 21, "insert", 3),
                ]
        assert _delta_metadata(path) == metadata
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_delta_io_overwrite_partitions_with_replace_where(spark, tmp_path):
    """Test Delta Lake overwrite with replaceWhere option."""
    delta_path = tmp_path / "delta_replace_where"
    delta_table_path = f"{delta_path}"

    data = [
        Row(id=1, category="A", value=10),
        Row(id=2, category="B", value=20),
        Row(id=3, category="A", value=30),
        Row(id=4, category="B", value=40),
    ]
    df = spark.createDataFrame(data)
    df.write.format("delta").mode("overwrite").save(str(delta_path))

    new_data = [
        Row(id=5, category="A", value=100),
        Row(id=6, category="A", value=200),
    ]
    new_df = spark.createDataFrame(new_data)
    new_df.write.format("delta").mode("overwrite").option("replaceWhere", "category = 'A'").save(str(delta_path))

    result_df = spark.read.format("delta").load(delta_table_path).sort("id")
    result = result_df.collect()

    assert {row.id for row in result} == {2, 4, 5, 6}
    assert {row.category for row in result if row.category == "A"} == {"A"}
    assert {row.value for row in result if row.category == "A"} == {100, 200}
    assert {row.value for row in result if row.category == "B"} == {20, 40}


@pytest.mark.xfail(
    not is_jvm_spark(), reason="Sail accepts non-deterministic conditional overwrite predicates", strict=True
)
@pytest.mark.parametrize("api", ["replace_where", "v2"])
@pytest.mark.parametrize("predicate", ["rand(0) > p", "p + rand(0) < 1.0 AND v >= 0"])
def test_delta_io_conditional_overwrite_rejects_nondeterministic_predicates(spark, tmp_path, api, predicate):
    delta_path = tmp_path / "nondeterministic_overwrite"
    table_name = "delta_nondeterministic_overwrite"
    data = [Row(p=0.5, v=value) for value in range(4)]
    frame = spark.createDataFrame(data)
    frame.coalesce(1).write.format("delta").partitionBy("p").save(str(delta_path))
    replacement = spark.createDataFrame([], frame.schema)
    try:
        if api == "v2":
            spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{escape_sql_string_literal(str(delta_path))}'")
        files_before = {
            file.relative_to(delta_path): file.read_bytes() for file in delta_path.rglob("*") if file.is_file()
        }
        error_message = "Non-deterministic expressions are not allowed in OVERWRITE conditions"
        if api == "replace_where":
            with pytest.raises(AnalysisException, match=error_message):
                replacement.write.format("delta").mode("overwrite").option("replaceWhere", predicate).save(
                    str(delta_path)
                )
        else:
            with pytest.raises(AnalysisException, match=error_message):
                replacement.writeTo(table_name).overwrite(F.expr(predicate))
        assert spark.read.format("delta").load(str(delta_path)).select("p", "v").orderBy("v").collect() == data
        assert {
            file.relative_to(delta_path): file.read_bytes() for file in delta_path.rglob("*") if file.is_file()
        } == files_before
    finally:
        if api == "v2":
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.parametrize("replace_where_check", ["false"], indirect=True)
@pytest.mark.usefixtures("replace_where_check")
def test_delta_io_overwrite_partitions_with_v2_api(spark, tmp_path):
    """Test Delta Lake overwrite with a condition using the V2 API."""
    delta_path = tmp_path / "delta_condition_v2"
    delta_table_path = f"{delta_path}"
    table_name = "delta_v2_overwrite_test"

    table_columns = "(id bigint, category string, value bigint)"

    data = [
        Row(id=1, category="A", value=10),
        Row(id=2, category="B", value=20),
        Row(id=3, category="A", value=30),
        Row(id=4, category="B", value=40),
    ]
    df = spark.createDataFrame(data)
    df.write.format("delta").mode("overwrite").save(str(delta_path))

    spark.sql(
        f"CREATE TABLE {table_name} {table_columns} USING DELTA LOCATION '{escape_sql_string_literal(delta_table_path)}'"
    )

    try:
        new_data = [
            Row(id=5, category="A", value=100),
            Row(id=6, category="A", value=200),
            Row(id=7, category="A", value=10),
            Row(id=8, category="B", value=999),
        ]
        new_df = spark.createDataFrame(new_data)

        condition = (F.col("category") == "A") & (F.col("value") < F.lit(50).cast("bigint"))
        new_df.writeTo(table_name).overwrite(condition)

        result_df = spark.read.format("delta").load(delta_table_path).sort("id")
        result = result_df.collect()

        assert {row.id for row in result} == {2, 4, 5, 6, 8, 7}
        assert {row.value for row in result if row.category == "A" and row.value < 50} == {10}  # noqa: PLR2004
        assert {row.value for row in result if row.category == "A" and row.value >= 100} == {100, 200}  # noqa: PLR2004
        assert {row.value for row in result if row.category == "B"} == {20, 40, 999}
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.parametrize("replace_where_check", ["false"], indirect=True)
@pytest.mark.usefixtures("replace_where_check")
def test_delta_io_overwrite_partitions_with_sql_condition(spark, tmp_path):
    """Test Delta Lake overwrite with a complex condition using SQL REPLACE WHERE."""
    delta_path = tmp_path / "delta_condition_sql"
    delta_table_path = f"{delta_path}"
    table_name = "delta_sql_overwrite_test"
    table_columns = "(id bigint, category string, value bigint)"

    data = [
        Row(id=1, category="A", value=10),
        Row(id=2, category="B", value=20),
        Row(id=3, category="A", value=30),
        Row(id=4, category="B", value=40),
    ]
    df = spark.createDataFrame(data)
    df.write.format("delta").mode("overwrite").save(str(delta_path))

    spark.sql(
        f"CREATE TABLE {table_name} {table_columns} USING DELTA LOCATION '{escape_sql_string_literal(delta_table_path)}'"
    )

    try:
        spark.sql(
            f"INSERT INTO TABLE {table_name} REPLACE WHERE category = 'A' AND value < CAST(50 AS BIGINT) SELECT * FROM VALUES (5, 'A', 100), (6, 'A', 200), (7, 'A', 10), (8, 'B', 999) AS tab(id, category, value)"  # noqa: S608
        )

        result_df = spark.read.format("delta").load(delta_table_path).sort("id")
        result = result_df.collect()

        assert {row.id for row in result} == {2, 4, 5, 6, 8, 7}
        assert {row.value for row in result if row.category == "A" and row.value < 50} == {10}  # noqa: PLR2004
        assert {row.value for row in result if row.category == "A" and row.value >= 100} == {100, 200}  # noqa: PLR2004
        assert {row.value for row in result if row.category == "B"} == {20, 40, 999}
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_delta_io_all_data_types(spark, tmp_path):
    """Test Delta Lake support for different data types"""
    delta_path = tmp_path / "delta_table"
    delta_table_path = f"{delta_path}"
    complex_data = [
        Row(
            id=1,
            name="Alice",
            age=30,
            salary=50000.50,
            is_active=True,
            birth_date=date(1993, 5, 15),
            created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=UTC),
            tags=["python", "spark"],
            metadata={"department": "engineering", "level": "senior"},
        ),
        Row(
            id=2,
            name="Bob",
            age=25,
            salary=45000.75,
            is_active=False,
            birth_date=date(1998, 8, 22),
            created_at=datetime(2025, 2, 1, 14, 45, 0, tzinfo=UTC),
            tags=["java", "scala"],
            metadata={"department": "product", "level": "junior"},
        ),
    ]

    df = spark.createDataFrame(complex_data)

    df.write.format("delta").mode("overwrite").save(str(delta_path))

    result_df = spark.read.format("delta").load(delta_table_path).sort("id")

    result_pandas = result_df.toPandas()

    expected = 2
    assert len(result_pandas) == expected
    assert result_pandas["id"].tolist() == [1, 2]
    assert result_pandas["name"].tolist() == ["Alice", "Bob"]
    assert result_pandas["age"].tolist() == [30, 25]
    assert result_pandas["is_active"].tolist() == [True, False]


def test_delta_io_ignore_mode(spark, delta_test_data, tmp_path):
    """Test Delta Lake ignore mode (ignore if table exists)"""
    delta_path = tmp_path / "delta_table"
    delta_table_path = f"{delta_path}"

    # Create initial table
    df1 = spark.createDataFrame(delta_test_data)
    df1.write.format("delta").mode("overwrite").save(str(delta_path))

    # Read initial data
    initial_result = spark.read.format("delta").load(delta_table_path).sort("id")
    initial_data = initial_result.collect()
    assert len(initial_data) == 3  # noqa: PLR2004
    commit_files_before = sorted((delta_path / "_delta_log").glob("*.json"))
    files_before = {path.relative_to(delta_path): path.read_bytes() for path in delta_path.rglob("*") if path.is_file()}

    # Try to write new data with ignore mode - should be ignored since table exists
    new_data = [
        Row(id=20, event="X", score=0.95),
        Row(id=21, event="Y", score=0.88),
    ]
    df2 = spark.createDataFrame(new_data)
    df2.write.format("delta").mode("ignore").partitionBy("missing").save(str(delta_path))
    # Ignoring an existing path must not execute the input, even via the path option.
    spark.range(1).selectExpr("raise_error('ignored input was executed') AS id").write.format("delta").mode(
        "ignore"
    ).option("path", str(delta_path)).save()
    assert {
        path.relative_to(delta_path): path.read_bytes() for path in delta_path.rglob("*") if path.is_file()
    } == files_before

    # Read data again - should be unchanged
    result_df = spark.read.format("delta").load(delta_table_path).sort("id")
    result_data = result_df.collect()
    commit_files_after = sorted((delta_path / "_delta_log").glob("*.json"))

    # Data should remain the same as initial data
    assert commit_files_after == commit_files_before
    assert len(result_data) == 3  # noqa: PLR2004
    assert result_data[0].id == 10  # noqa: PLR2004
    assert result_data[1].id == 11  # noqa: PLR2004
    assert result_data[2].id == 12  # noqa: PLR2004
    assert result_data[0].event == "A"
    assert result_data[1].event == "B"
    assert result_data[2].event == "A"


def test_delta_io_ignore_mode_new_table(spark, tmp_path):
    """Test Delta Lake ignore mode when table doesn't exist (should create table)"""
    delta_path = tmp_path / "delta_table_new"
    delta_table_path = f"{delta_path}"

    # Write data with ignore mode to non-existent table - should create table
    new_data = [
        Row(id=30, event="Z", score=0.92),
        Row(id=31, event="W", score=0.85),
    ]
    df = spark.createDataFrame(new_data)
    df.write.format("delta").mode("ignore").save(str(delta_path))

    # Read data - should contain the new data
    result_df = spark.read.format("delta").load(delta_table_path).sort("id")
    result_data = result_df.collect()

    assert len(result_data) == 2  # noqa: PLR2004
    assert result_data[0].id == 30  # noqa: PLR2004
    assert result_data[1].id == 31  # noqa: PLR2004
    assert result_data[0].event == "Z"
    assert result_data[1].event == "W"
    assert result_data[0].score == 0.92  # noqa: PLR2004
    assert result_data[1].score == 0.85  # noqa: PLR2004


def test_delta_io_error_on_read_nonexistent_table(spark, tmp_path):
    """Test Delta Lake error handling"""
    delta_path = tmp_path / "delta_table"
    delta_table_path = f"{delta_path}"

    with pytest.raises(Exception, match=r".*"):
        spark.read.format("delta").load(delta_table_path).collect()

    # Create table and try again
    test_data = [Row(id=1, name="test")]
    df = spark.createDataFrame(test_data)
    df.write.format("delta").mode("overwrite").save(str(delta_path))

    result = spark.read.format("delta").load(delta_table_path).collect()
    assert result == [Row(id=1, name="test")]
