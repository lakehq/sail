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


