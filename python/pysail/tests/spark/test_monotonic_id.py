import pytest


def test_monotonically_increasing_id_smoke(spark):
    rows = spark.sql("SELECT monotonically_increasing_id() AS id").collect()
    assert len(rows) == 1
    value = rows[0]["id"]
    assert isinstance(value, int)
    assert value >= 0


def test_monotonically_increasing_id_increases_within_partition(spark):
    # 10 rows in one partition (most likely single partition for this small query),
    # ids should be strictly increasing in output order.
    rows = spark.sql(
        """
        SELECT monotonically_increasing_id() AS id
        FROM range(0, 10)
        ORDER BY id
        """
    ).collect()
    ids = [r["id"] for r in rows]
    assert ids == sorted(ids)
    assert len(set(ids)) == len(ids)


def test_monotonically_increasing_id_same_when_called_twice_in_select(spark):
    rows = spark.sql(
        """
        SELECT
          monotonically_increasing_id() AS id1,
          monotonically_increasing_id() AS id2
        FROM range(0, 10)
        """
    ).collect()
    assert rows, "expected non-empty result"
    for r in rows:
        assert r["id1"] == r["id2"]


@pytest.mark.skip(reason="Temporarily skipped until we have a fix for the issue")
def test_monotonically_increasing_id_in_aggregate_projection(spark):
    rows = spark.sql(
        """
        SELECT
          id,
          max(monotonically_increasing_id()) AS id1,
          max(monotonically_increasing_id()) AS id2
        FROM range(0, 10)
        GROUP BY id
        ORDER BY id
        """
    ).collect()
    assert len(rows) == 10  # noqa: PLR2004
    for index, row in enumerate(rows):
        assert row["id"] == index
        assert isinstance(row["id1"], int)
        assert row["id1"] >= 0
        assert row["id1"] == row["id2"]


def test_monotonically_increasing_id_in_grouping_expression_projection(spark):
    rows = spark.sql(
        """
        SELECT max(id), monotonically_increasing_id()
        FROM range(10)
        GROUP BY monotonically_increasing_id()
        ORDER BY monotonically_increasing_id()
        """
    ).collect()
    assert len(rows) == 10  # noqa: PLR2004

    max_ids = sorted(row[0] for row in rows)
    group_ids = [row[1] for row in rows]

    assert max_ids == list(range(10))
    assert group_ids == sorted(group_ids)
    assert len(set(group_ids)) == len(group_ids)


def test_monotonically_increasing_id_in_aggregate_with_group_by_id(spark):
    rows = spark.sql(
        """
        SELECT id, max(monotonically_increasing_id())
        FROM range(10)
        GROUP BY id
        ORDER BY id
        """
    ).collect()
    assert len(rows) == 10  # noqa: PLR2004

    for index, row in enumerate(rows):
        assert row[0] == index
        assert isinstance(row[1], int)
        assert row[1] >= 0


def test_explode_in_aggregate_with_group_by_id(spark):
    rows = spark.sql(
        """
        SELECT id, max(explode(sequence(id)))
        FROM range(10)
        GROUP BY id
        ORDER BY id
        """
    ).collect()
    assert len(rows) == 10  # noqa: PLR2004

    for index, row in enumerate(rows):
        assert row[0] == index
        assert row[1] is None or isinstance(row[1], int)


def test_explode_in_grouping_expression_projection(spark):
    rows = spark.sql(
        """
        SELECT max(id), explode(sequence(id))
        FROM range(10)
        GROUP BY explode(sequence(id))
        ORDER BY explode(sequence(id))
        """
    ).collect()
    assert rows

    group_values = [row[1] for row in rows]
    assert group_values == sorted(group_values)

    for row in rows:
        assert isinstance(row[0], int)
        assert isinstance(row[1], int)


def test_monotonically_increasing_id_inside_lateral_view_expression(spark):
    rows = spark.sql(
        """
        SELECT id, v
        FROM range(0, 10)
        LATERAL VIEW explode(array(monotonically_increasing_id())) AS v
        ORDER BY id
        """
    ).collect()
    assert len(rows) == 10  # noqa: PLR2004
    for index, row in enumerate(rows):
        assert row["id"] == index
        assert isinstance(row["v"], int)
        assert row["v"] >= 0
