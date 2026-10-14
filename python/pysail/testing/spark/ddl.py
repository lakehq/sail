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


def exercise_lakehouse_replace_columns(spark, table, fmt):
    """Check the distinct field-identity semantics of Delta and Iceberg replacement."""
    spark.sql(f"INSERT INTO {table} VALUES (1, 'one')")
    spark.sql(f"ALTER TABLE {table} REPLACE COLUMNS (value STRING COMMENT '123', id INT, extra BIGINT)")
    old = ("one", 1, None) if fmt == "delta" else (None, None, None)
    assert [tuple(row) for row in spark.table(table).collect()] == [old]
    assert spark.table(table).columns == ["value", "id", "extra"]
    spark.sql(f"INSERT INTO {table} VALUES ('two', 2, 20)")
    spark.sql(f"UPDATE {table} SET extra = 21 WHERE id = 2")
    assert [tuple(row) for row in spark.table(table).orderBy("id").collect()] == [old, ("two", 2, 21)]
    return [old, ("two", 2, 21)]


def exercise_iceberg_write_order(spark, table):
    """Change native sort metadata and exercise writes under successive orders."""
    spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY id DESC NULLS LAST, value ASC")
    spark.sql(f"INSERT INTO {table} VALUES (3, 8), (1, 4), (NULL, 2), (3, 1)")
    spark.sql(f"UPDATE {table} SET value = 9 WHERE value = 8")
    assert [tuple(r) for r in spark.sql(f"SELECT * FROM {table} ORDER BY id DESC NULLS LAST, value").collect()] == [
        (3, 1), (3, 9), (1, 4), (None, 2)
    ]


def exercise_iceberg_partition_evolution(spark, table):
    """Write and mutate files across successive Iceberg partition specs."""
    spark.sql(f"INSERT INTO {table} VALUES (1, 'aa')")
    spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD bucket(4, id) AS id_bucket")
    spark.sql(f"INSERT INTO {table} VALUES (2, 'bb')")
    spark.sql(f"ALTER TABLE {table} REPLACE PARTITION FIELD part WITH truncate(2, part) AS part_prefix")
    spark.sql(f"INSERT INTO {table} VALUES (3, 'long')")
    spark.sql(f"ALTER TABLE {table} DROP PARTITION FIELD id_bucket")
    spark.sql(f"INSERT INTO {table} VALUES (4, 'longer')")
    spark.sql(f"UPDATE {table} SET part = 'zz' WHERE id = 1")
    spark.sql(f"DELETE FROM {table} WHERE id = 2")
    spark.sql(f"MERGE INTO {table} t USING (SELECT 3 AS id, 'updated' AS part) s ON t.id=s.id WHEN MATCHED THEN UPDATE SET part=s.part")
    assert [tuple(r) for r in spark.sql(f"SELECT * FROM {table} ORDER BY id").collect()] == [
        (1, "zz"), (3, "updated"), (4, "longer")
    ]
