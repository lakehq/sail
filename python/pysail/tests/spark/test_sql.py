from pyspark.sql import Row


def test_recursive_expression_entry_points(spark):
    expression = """
        (SELECT max(r.id)
         FROM VALUES (1), (2) AS r(id)
         JOIN (VALUES (2) AS s(id) JOIN VALUES (2) AS t(id) ON s.id = t.id)
           ON r.id = s.id
         WHERE EXISTS (SELECT 1 FROM VALUES (2) AS u(id) WHERE u.id = r.id))
    """
    frame = spark.range(1, 4).filter(f"id = {expression}")
    assert frame.selectExpr("id", f"{expression} AS max_id").collect() == [Row(id=2, max_id=2)]
