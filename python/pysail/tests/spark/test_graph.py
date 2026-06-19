import pytest

from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.mark.skipif(is_jvm_spark(), reason="Sail-only Cypher graph extension")
def test_cypher_match_over_grust_tables(spark):
    spark.sql(
        """
        CREATE OR REPLACE TEMPORARY VIEW grust_nodes AS
        SELECT * FROM VALUES
          ('1', 'Person', '{"age":"42","name":"Alice"}'),
          ('2', 'Person', '{"age":"31","name":"Bob"}'),
          ('3', 'Document', '{"age":"42","name":"Paper"}')
        AS tab(id, label, props)
        """
    )
    spark.sql(
        """
        CREATE OR REPLACE TEMPORARY VIEW grust_edges AS
        SELECT * FROM VALUES
          ('edge-key-1', 'edge-1', '1', 'Person', '2', 'Person', 'KNOWS', '{"since":"2020"}'),
          ('edge-key-2', 'edge-2', '2', 'Person', '1', 'Person', 'LIKES', '{"since":"2021"}'),
          ('edge-key-3', 'edge-3', '3', 'Document', '2', 'Person', 'KNOWS', '{"since":"2022"}')
        AS tab(edge_key, id, src_id, src_label, dst_id, dst_label, edge_type, props)
        """
    )

    bracketed_rows = spark.sql(
        """
        MATCH (a:Person)-[e:KNOWS]->(b:Person)
        WHERE a.age = '42'
        RETURN a.id, e.id, e.edge_key, e.label, b.name
        ORDER BY b.name
        LIMIT 10
        """
    ).collect()
    shorthand_rows = spark.sql(
        """
        MATCH (a:Person)-->(b:Person)
        WHERE a.age = '42'
        RETURN b.name
        ORDER BY b.name
        LIMIT 10
        """
    ).collect()
    limit_all_rows = spark.sql(
        """
        MATCH (a:Person)-->(b:Person)
        RETURN b.name
        ORDER BY b.name
        SKIP 1
        LIMIT ALL
        """
    ).collect()
    property_rows = spark.sql(
        """
        MATCH (a:Person {age: '42'})-[e:KNOWS {since: '2020'}]->(b:Person {name: 'Bob'})
        RETURN a.id, e.id, b.name
        """
    ).collect()
    incoming_rows = spark.sql(
        """
        MATCH (a)<-[e]-(b)
        RETURN a.id, e.id, b.id
        ORDER BY e.id
        """
    ).collect()
    undirected_rows = spark.sql(
        """
        MATCH (a)-[e]-(b)
        RETURN a.id, e.id, b.id
        ORDER BY e.id, a.id
        """
    ).collect()

    assert bracketed_rows == [("1", "edge-1", "edge-key-1", "KNOWS", "Bob")]
    assert shorthand_rows == [("Bob",)]
    assert limit_all_rows == [("Bob",)]
    assert property_rows == [("1", "edge-1", "Bob")]
    assert incoming_rows == [
        ("2", "edge-1", "1"),
        ("1", "edge-2", "2"),
        ("2", "edge-3", "3"),
    ]
    assert undirected_rows == [
        ("1", "edge-1", "2"),
        ("2", "edge-1", "1"),
        ("1", "edge-2", "2"),
        ("2", "edge-2", "1"),
        ("2", "edge-3", "3"),
        ("3", "edge-3", "2"),
    ]
