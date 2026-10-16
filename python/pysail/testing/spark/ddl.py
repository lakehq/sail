# ruff: noqa: S608
"""Shared SQL assertions for catalog-backed lakehouse DDL."""


def exercise_lakehouse_alter(spark, table):
    """Exercise schema, default and property changes on a prepared three-column table."""
    spark.sql(f"INSERT INTO {table} VALUES (1, 'one', 'a')")
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('custom'='updated')")
    assert {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}["custom"] == "updated"
    assert [tuple(r) for r in spark.sql(f"SHOW TBLPROPERTIES {table} ('custom')").collect()] == [("custom", "updated")]
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN id TYPE BIGINT")
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN value SET DEFAULT 'fallback'")
    spark.sql(f"INSERT INTO {table} (id, part) VALUES (2, 'b')")
    assert spark.table(table).schema["id"].dataType.simpleString() == "bigint"
    assert [tuple(row) for row in spark.table(table).orderBy("id").collect()] == [
        (1, "one", "a"),
        (2, "fallback", "b"),
    ]
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN value DROP DEFAULT")
    spark.sql(f"INSERT INTO {table} VALUES (3, DEFAULT, 'c')")
    assert spark.table(table).where("id = 3").first().value is None
    spark.sql(f"ALTER TABLE {table} UNSET TBLPROPERTIES ('custom')")
    spark.sql(f"ALTER TABLE {table} UNSET TBLPROPERTIES IF EXISTS ('absent')")
    assert "custom" not in {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    assert [tuple(r) for r in spark.sql(f"SHOW TBLPROPERTIES {table} ('custom')").collect()] == [
        ("custom", f"Table {table} does not have property: custom")
    ]
