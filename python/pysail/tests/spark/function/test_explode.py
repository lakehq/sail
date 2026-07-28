import pytest


@pytest.mark.skip(reason="Temporarily skipped until we have a fix for the issue")
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


@pytest.mark.skip(reason="Temporarily skipped until we have a fix for the issue")
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
