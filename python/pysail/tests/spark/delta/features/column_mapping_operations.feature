Feature: Delta column mapping preserves values across SQL mutations
  # Expected results verified with JVM Spark 4.1.3 / Delta 4.3.1 for all four variants.

  Background:
    Given variable location for temporary directory column_mapping_operations
    Given final statement
      """
      DROP TABLE IF EXISTS column_mapping_operations
      """
    Given statement template
      """
      CREATE TABLE column_mapping_operations (
        id INT, grp STRING, dbl DOUBLE, name STRING,
        s STRUCT<a: INT, b: STRING, inner: STRUCT<c: INT, d: STRING>>,
        arr ARRAY<INT>, arr_s ARRAY<STRUCT<x: INT, y: STRING>>,
        m MAP<STRING, INT>, m_s MAP<STRING, STRUCT<p: INT, q: STRING>>
      ) USING DELTA <partition_clause> LOCATION {{ location.sql }}
      TBLPROPERTIES ('delta.columnMapping.mode' = '<mapping>')
      """
    Given statement
      """
      INSERT INTO column_mapping_operations VALUES
        (1, 'g1', 1.5, 'n1', named_struct('a', 1, 'b', 'x', 'inner', named_struct('c', 10, 'd', 'inner1')),
         array(1, 2), CAST(array(named_struct('x', 1, 'y', 'y1')) AS ARRAY<STRUCT<x: INT, y: STRING>>), map('k1', 1), map('k1', named_struct('p', 1, 'q', 'q1'))),
        (2, 'g1', NULL, NULL, named_struct('a', 2, 'b', 'y', 'inner', named_struct('c', 20, 'd', 'inner2')),
         array(2), CAST(array(named_struct('x', 2, 'y', 'y2')) AS ARRAY<STRUCT<x: INT, y: STRING>>), map('k1', 2), map('k1', named_struct('p', 2, 'q', 'q2'))),
        (3, 'g2', 3.5, 'n3', NULL, NULL, NULL, NULL, NULL),
        (4, NULL, 4.5, 'n4', named_struct('a', 4, 'b', 'w', 'inner', named_struct('c', 40, 'd', 'inner4')),
         array(4, NULL), CAST(array(named_struct('x', 4, 'y', 'y4')) AS ARRAY<STRUCT<x: INT, y: STRING>>), map('k1', NULL), map('k1', named_struct('p', 4, 'q', 'q4')))
      """

  Scenario Outline: Insert copies nested and null values
    Given statement
      """
      INSERT INTO column_mapping_operations
      SELECT * FROM column_mapping_operations WHERE id = 1 OR s IS NULL
      """
    When query
      """
      SELECT id, grp, dbl, name, s, arr, arr_s, m, m_s FROM column_mapping_operations ORDER BY id
      """
    Then query result ordered
      | id | grp  | dbl  | name | s                    | arr       | arr_s     | m            | m_s             |
      | 1  | g1   | 1.5  | n1   | {1, x, {10, inner1}} | [1, 2]    | [{1, y1}] | {k1 -> 1}    | {k1 -> {1, q1}} |
      | 1  | g1   | 1.5  | n1   | {1, x, {10, inner1}} | [1, 2]    | [{1, y1}] | {k1 -> 1}    | {k1 -> {1, q1}} |
      | 2  | g1   | NULL | NULL | {2, y, {20, inner2}} | [2]       | [{2, y2}] | {k1 -> 2}    | {k1 -> {2, q2}} |
      | 3  | g2   | 3.5  | n3   | NULL                 | NULL      | NULL      | NULL         | NULL            |
      | 3  | g2   | 3.5  | n3   | NULL                 | NULL      | NULL      | NULL         | NULL            |
      | 4  | NULL | 4.5  | n4   | {4, w, {40, inner4}} | [4, NULL] | [{4, y4}] | {k1 -> NULL} | {k1 -> {4, q4}} |

    Examples:
      | mapping | partition_clause     |
      | name    |                      |
      | name    | PARTITIONED BY (grp) |
      | id      |                      |
      | id      | PARTITIONED BY (grp) |

  Scenario Outline: Updates preserve untouched scalar and nested fields
    Given statement
      """
      UPDATE column_mapping_operations SET dbl = coalesce(dbl, 0) + 1, name = upper(name) WHERE s.a > 1
      """
    When query
      """
      SELECT id, grp, dbl, name, s, arr, arr_s, m, m_s FROM column_mapping_operations ORDER BY id
      """
    Then query result ordered
      | id | grp  | dbl | name | s                    | arr       | arr_s     | m            | m_s             |
      | 1  | g1   | 1.5 | n1   | {1, x, {10, inner1}} | [1, 2]    | [{1, y1}] | {k1 -> 1}    | {k1 -> {1, q1}} |
      | 2  | g1   | 1.0 | NULL | {2, y, {20, inner2}} | [2]       | [{2, y2}] | {k1 -> 2}    | {k1 -> {2, q2}} |
      | 3  | g2   | 3.5 | n3   | NULL                 | NULL      | NULL      | NULL         | NULL            |
      | 4  | NULL | 5.5 | N4   | {4, w, {40, inner4}} | [4, NULL] | [{4, y4}] | {k1 -> NULL} | {k1 -> {4, q4}} |
    Given statement
      """
      UPDATE column_mapping_operations
      SET s = named_struct('a', s.a + 10, 'b', s.b, 'inner', s.inner),
          arr_s = array(named_struct('x', -1, 'y', 'updated'))
      WHERE array_contains(arr, 2)
      """
    When query
      """
      SELECT id, grp, dbl, name, s, arr, arr_s, m, m_s FROM column_mapping_operations ORDER BY id
      """
    Then query result ordered
      | id | grp  | dbl | name | s                     | arr       | arr_s           | m            | m_s             |
      | 1  | g1   | 1.5 | n1   | {11, x, {10, inner1}} | [1, 2]    | [{-1, updated}] | {k1 -> 1}    | {k1 -> {1, q1}} |
      | 2  | g1   | 1.0 | NULL | {12, y, {20, inner2}} | [2]       | [{-1, updated}] | {k1 -> 2}    | {k1 -> {2, q2}} |
      | 3  | g2   | 3.5 | n3   | NULL                  | NULL      | NULL            | NULL         | NULL            |
      | 4  | NULL | 5.5 | N4   | {4, w, {40, inner4}}  | [4, NULL] | [{4, y4}]       | {k1 -> NULL} | {k1 -> {4, q4}} |
    Given statement
      """
      UPDATE column_mapping_operations SET m_s = map('k9', named_struct('p', 9, 'q', 'q9')) WHERE m['k1'] IS NULL
      """
    When query
      """
      SELECT id, grp, dbl, name, s, arr, arr_s, m, m_s FROM column_mapping_operations ORDER BY id
      """
    Then query result ordered
      | id | grp  | dbl | name | s                     | arr       | arr_s           | m            | m_s             |
      | 1  | g1   | 1.5 | n1   | {11, x, {10, inner1}} | [1, 2]    | [{-1, updated}] | {k1 -> 1}    | {k1 -> {1, q1}} |
      | 2  | g1   | 1.0 | NULL | {12, y, {20, inner2}} | [2]       | [{-1, updated}] | {k1 -> 2}    | {k1 -> {2, q2}} |
      | 3  | g2   | 3.5 | n3   | NULL                  | NULL      | NULL            | NULL         | {k9 -> {9, q9}} |
      | 4  | NULL | 5.5 | N4   | {4, w, {40, inner4}}  | [4, NULL] | [{4, y4}]       | {k1 -> NULL} | {k9 -> {9, q9}} |

    Examples:
      | mapping | partition_clause     |
      | name    |                      |
      | name    | PARTITIONED BY (grp) |
      | id      |                      |
      | id      | PARTITIONED BY (grp) |

  Scenario Outline: Deletes filter nested fields and nullable partitions
    Given statement
      """
      DELETE FROM column_mapping_operations WHERE s.inner.c = 20
      """
    When query
      """
      SELECT id, grp, dbl, name, s, arr, arr_s, m, m_s FROM column_mapping_operations ORDER BY id
      """
    Then query result ordered
      | id | grp  | dbl | name | s                    | arr       | arr_s     | m            | m_s             |
      | 1  | g1   | 1.5 | n1   | {1, x, {10, inner1}} | [1, 2]    | [{1, y1}] | {k1 -> 1}    | {k1 -> {1, q1}} |
      | 3  | g2   | 3.5 | n3   | NULL                 | NULL      | NULL      | NULL         | NULL            |
      | 4  | NULL | 4.5 | n4   | {4, w, {40, inner4}} | [4, NULL] | [{4, y4}] | {k1 -> NULL} | {k1 -> {4, q4}} |
    Given statement
      """
      DELETE FROM column_mapping_operations WHERE grp IS NULL OR exists(arr_s, e -> e.x = 1)
      """
    When query
      """
      SELECT id, grp, dbl, name, s, arr, arr_s, m, m_s FROM column_mapping_operations ORDER BY id
      """
    Then query result ordered
      | id | grp | dbl | name | s    | arr  | arr_s | m    | m_s  |
      | 3  | g2  | 3.5 | n3   | NULL | NULL | NULL  | NULL | NULL |

    Examples:
      | mapping | partition_clause     |
      | name    |                      |
      | name    | PARTITIONED BY (grp) |
      | id      |                      |
      | id      | PARTITIONED BY (grp) |

  Scenario Outline: Mutations match whole struct values
    Given statement
      """
      UPDATE column_mapping_operations SET name = 'matched'
      WHERE s = named_struct('a', 1, 'b', 'x', 'inner', named_struct('c', 10, 'd', 'inner1'))
      """
    When query
      """
      SELECT id, grp, dbl, name, s, arr, arr_s, m, m_s FROM column_mapping_operations ORDER BY id
      """
    Then query result ordered
      | id | grp  | dbl  | name    | s                    | arr       | arr_s     | m            | m_s             |
      | 1  | g1   | 1.5  | matched | {1, x, {10, inner1}} | [1, 2]    | [{1, y1}] | {k1 -> 1}    | {k1 -> {1, q1}} |
      | 2  | g1   | NULL | NULL    | {2, y, {20, inner2}} | [2]       | [{2, y2}] | {k1 -> 2}    | {k1 -> {2, q2}} |
      | 3  | g2   | 3.5  | n3      | NULL                 | NULL      | NULL      | NULL         | NULL            |
      | 4  | NULL | 4.5  | n4      | {4, w, {40, inner4}} | [4, NULL] | [{4, y4}] | {k1 -> NULL} | {k1 -> {4, q4}} |
    Given statement
      """
      DELETE FROM column_mapping_operations WHERE s.inner = named_struct('c', 40, 'd', 'inner4')
      """
    When query
      """
      SELECT id, grp, dbl, name, s, arr, arr_s, m, m_s FROM column_mapping_operations ORDER BY id
      """
    Then query result ordered
      | id | grp | dbl  | name    | s                    | arr    | arr_s     | m         | m_s             |
      | 1  | g1  | 1.5  | matched | {1, x, {10, inner1}} | [1, 2] | [{1, y1}] | {k1 -> 1} | {k1 -> {1, q1}} |
      | 2  | g1  | NULL | NULL    | {2, y, {20, inner2}} | [2]    | [{2, y2}] | {k1 -> 2} | {k1 -> {2, q2}} |
      | 3  | g2  | 3.5  | n3      | NULL                 | NULL   | NULL      | NULL      | NULL            |

    Examples:
      | mapping | partition_clause     |
      | name    |                      |
      | name    | PARTITIONED BY (grp) |
      | id      |                      |
      | id      | PARTITIONED BY (grp) |

  Scenario Outline: Merge updates deletes and inserts mapped rows
    Given statement
      """
      MERGE INTO column_mapping_operations AS target
      USING (
        SELECT 2 AS id, 'g1' AS grp, 2.5 AS dbl, 'replacement' AS name,
          named_struct('a', 20, 'b', 'merged', 'inner', named_struct('c', 200, 'd', 'new2')) AS s,
          array(20) AS arr, CAST(array(named_struct('x', 20, 'y', 'new2')) AS ARRAY<STRUCT<x: INT, y: STRING>>) AS arr_s,
          map('k1', 20) AS m, map('k1', named_struct('p', 20, 'q', 'merged')) AS m_s
        UNION ALL
        SELECT 3, 'g2', 3.5, 'deleted', named_struct('a', 70, 'b', 'deleted', 'inner', named_struct('c', 700, 'd', 'new3')),
          NULL, NULL, NULL, NULL
        UNION ALL
        SELECT 5, 'g2', 5.5, 'new', named_struct('a', 5, 'b', 'new', 'inner', named_struct('c', 50, 'd', 'inner5')),
          array(5), CAST(array(named_struct('x', 5, 'y', 'y5')) AS ARRAY<STRUCT<x: INT, y: STRING>>), map('k1', 5), map('k1', named_struct('p', 5, 'q', 'q5'))
      ) AS source ON target.id = source.id
      WHEN MATCHED AND source.s.a > 50 THEN DELETE
      WHEN MATCHED THEN UPDATE SET target.s = source.s, target.m_s = source.m_s
      WHEN NOT MATCHED THEN INSERT *
      """
    When query
      """
      SELECT id, grp, dbl, name, s, arr, arr_s, m, m_s FROM column_mapping_operations ORDER BY id
      """
    Then query result ordered
      | id | grp  | dbl  | name | s                         | arr       | arr_s     | m            | m_s                  |
      | 1  | g1   | 1.5  | n1   | {1, x, {10, inner1}}      | [1, 2]    | [{1, y1}] | {k1 -> 1}    | {k1 -> {1, q1}}      |
      | 2  | g1   | NULL | NULL | {20, merged, {200, new2}} | [2]       | [{2, y2}] | {k1 -> 2}    | {k1 -> {20, merged}} |
      | 4  | NULL | 4.5  | n4   | {4, w, {40, inner4}}      | [4, NULL] | [{4, y4}] | {k1 -> NULL} | {k1 -> {4, q4}}      |
      | 5  | g2   | 5.5  | new  | {5, new, {50, inner5}}    | [5]       | [{5, y5}] | {k1 -> 5}    | {k1 -> {5, q5}}      |

    Examples:
      | mapping | partition_clause     |
      | name    |                      |
      | name    | PARTITIONED BY (grp) |
      | id      |                      |
      | id      | PARTITIONED BY (grp) |

  Scenario Outline: Time travel reads each committed state
    Given statement
      """
      DELETE FROM column_mapping_operations WHERE id > 2
      """
    Given statement
      """
      UPDATE column_mapping_operations
      SET name = 'travelled', s = named_struct('a', 0, 'b', '', 'inner', s.inner) WHERE id = 1
      """
    When query
      """
      SELECT id, grp, dbl, name, s, arr, arr_s, m, m_s FROM column_mapping_operations VERSION AS OF 1 ORDER BY id
      """
    Then query result ordered
      | id | grp  | dbl  | name | s                    | arr       | arr_s     | m            | m_s             |
      | 1  | g1   | 1.5  | n1   | {1, x, {10, inner1}} | [1, 2]    | [{1, y1}] | {k1 -> 1}    | {k1 -> {1, q1}} |
      | 2  | g1   | NULL | NULL | {2, y, {20, inner2}} | [2]       | [{2, y2}] | {k1 -> 2}    | {k1 -> {2, q2}} |
      | 3  | g2   | 3.5  | n3   | NULL                 | NULL      | NULL      | NULL         | NULL            |
      | 4  | NULL | 4.5  | n4   | {4, w, {40, inner4}} | [4, NULL] | [{4, y4}] | {k1 -> NULL} | {k1 -> {4, q4}} |
    When query
      """
      SELECT id, grp, dbl, name, s, arr, arr_s, m, m_s FROM column_mapping_operations VERSION AS OF 2 ORDER BY id
      """
    Then query result ordered
      | id | grp | dbl  | name | s                    | arr    | arr_s     | m         | m_s             |
      | 1  | g1  | 1.5  | n1   | {1, x, {10, inner1}} | [1, 2] | [{1, y1}] | {k1 -> 1} | {k1 -> {1, q1}} |
      | 2  | g1  | NULL | NULL | {2, y, {20, inner2}} | [2]    | [{2, y2}] | {k1 -> 2} | {k1 -> {2, q2}} |
    When query
      """
      SELECT id, grp, dbl, name, s, arr, arr_s, m, m_s FROM column_mapping_operations VERSION AS OF 3 ORDER BY id
      """
    Then query result ordered
      | id | grp | dbl  | name      | s                    | arr    | arr_s     | m         | m_s             |
      | 1  | g1  | 1.5  | travelled | {0, , {10, inner1}}  | [1, 2] | [{1, y1}] | {k1 -> 1} | {k1 -> {1, q1}} |
      | 2  | g1  | NULL | NULL      | {2, y, {20, inner2}} | [2]    | [{2, y2}] | {k1 -> 2} | {k1 -> {2, q2}} |

    Examples:
      | mapping | partition_clause     |
      | name    |                      |
      | name    | PARTITIONED BY (grp) |
      | id      |                      |
      | id      | PARTITIONED BY (grp) |
