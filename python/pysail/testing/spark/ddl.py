# ruff: noqa: S608
"""Shared SQL assertions for catalog-backed lakehouse DDL."""


def exercise_lakehouse_alter(spark, table):
    """Exercise schema, default and property changes on a prepared three-column table."""
    spark.sql(f"INSERT INTO {table} VALUES (1, 'one', 'a')")
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('custom'='updated')")
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


def exercise_lakehouse_column_changes(spark, table):
    """Check column changes and subsequent DML against a table with id and value columns."""
    spark.sql(f"INSERT INTO {table} VALUES (1, 'one')")
    spark.sql(f"ALTER TABLE {table} ADD COLUMN extra BIGINT FIRST")
    spark.sql(f"ALTER TABLE {table} RENAME COLUMN value TO renamed")
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN renamed COMMENT 'renamed column'")
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN renamed AFTER extra")
    spark.sql(f"INSERT INTO {table} (id, renamed) VALUES (2, 'two')")
    spark.sql(f"UPDATE {table} SET extra = 10 WHERE id = 1")
    assert [tuple(row) for row in spark.table(table).orderBy("id").collect()] == [
        (10, "one", 1),
        (None, "two", 2),
    ]
    spark.sql(f"ALTER TABLE {table} DROP COLUMN extra")
    assert spark.table(table).columns == ["renamed", "id"]
    assert [tuple(row) for row in spark.table(table).orderBy("id").collect()] == [("one", 1), ("two", 2)]
