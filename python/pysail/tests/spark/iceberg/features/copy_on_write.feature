Feature: Iceberg copy-on-write row operations

  Background:
    Given variable location for temporary directory iceberg_cow
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_cow
      """
    Given statement template
      """
      CREATE TABLE iceberg_cow (id INT, value INT, part STRING)
      USING iceberg
      PARTITIONED BY (part)
      LOCATION {{ location.uri }}
      """
    Given statement
      """
      INSERT INTO iceberg_cow VALUES (1, 10, 'A'), (2, NULL, 'A')
      """
    Given statement
      """
      INSERT INTO iceberg_cow VALUES (3, 30, 'B'), (4, 40, 'B')
      """

  Scenario: COW DELETE preserves null predicates and untouched files
    Given remember current iceberg data manifest paths
    Given statement
      """
      DELETE FROM iceberg_cow WHERE value < 20
      """
    Then iceberg current data manifests reuse 1 remembered paths
    Then iceberg snapshot count is 3
    Then iceberg snapshot operation is overwrite
    When query
      """
      SELECT * FROM iceberg_cow ORDER BY id
      """
    Then query result ordered
      | id | value | part |
      | 2  | NULL  | A    |
      | 3  | 30    | B    |
      | 4  | 40    | B    |

  Scenario: COW UPDATE uses original assignment values and moves partitions
    Given remember current iceberg data manifest paths
    Given statement
      """
      UPDATE iceberg_cow SET id = value, value = id, part = 'C' WHERE id = 1
      """
    Then iceberg current data manifests reuse 1 remembered paths
    Then iceberg snapshot count is 3
    When query
      """
      SELECT * FROM iceberg_cow ORDER BY id
      """
    Then query result ordered
      | id | value | part |
      | 2  | NULL  | A    |
      | 3  | 30    | B    |
      | 4  | 40    | B    |
      | 10 | 1     | C    |

  Scenario: COW MERGE applies ordered matched insert and target-only clauses
    Given statement
      """
      MERGE INTO iceberg_cow AS t
      USING (SELECT * FROM VALUES (1, 100, 'C'), (2, 200, 'A'), (5, 500, 'D') AS src(id, value, part)) AS s
      ON t.id = s.id
      WHEN MATCHED AND s.id = 1 THEN UPDATE SET value = s.value, part = s.part
      WHEN MATCHED THEN DELETE
      WHEN NOT MATCHED THEN INSERT *
      WHEN NOT MATCHED BY SOURCE AND t.id = 3 THEN UPDATE SET value = t.value + 1
      WHEN NOT MATCHED BY SOURCE THEN DELETE
      """
    Then iceberg snapshot count is 3
    Then iceberg snapshot operation is overwrite
    When query
      """
      SELECT * FROM iceberg_cow ORDER BY id
      """
    Then query result ordered
      | id | value | part |
      | 1  | 100   | C    |
      | 3  | 31    | B    |
      | 5  | 500   | D    |

  Scenario: COW target-only MERGE preserves matched rows once with duplicate source keys
    Given statement
      """
      MERGE INTO iceberg_cow AS t
      USING (SELECT * FROM VALUES (1), (1) AS src(id)) AS s
      ON t.id = s.id
      WHEN NOT MATCHED BY SOURCE AND t.id = 2 THEN UPDATE SET value = 200
      WHEN NOT MATCHED BY SOURCE THEN DELETE
      """
    When query
      """
      SELECT * FROM iceberg_cow ORDER BY id
      """
    Then query result ordered
      | id | value | part |
      | 1  | 10    | A    |
      | 2  | 200   | A    |

  Scenario: COW unconditional matched DELETE accepts duplicate source matches
    Given statement
      """
      MERGE INTO iceberg_cow AS t
      USING (SELECT * FROM VALUES (1), (1), (2) AS src(id)) AS s
      ON t.id = s.id
      WHEN MATCHED THEN DELETE
      """
    Then iceberg snapshot operation is delete
    When query
      """
      SELECT * FROM iceberg_cow ORDER BY id
      """
    Then query result ordered
      | id | value | part |
      | 3  | 30    | B    |
      | 4  | 40    | B    |

  Scenario: COW operations without matches do not create snapshots
    Given statement
      """
      DELETE FROM iceberg_cow WHERE id = 99
      """
    Given statement
      """
      UPDATE iceberg_cow SET value = 0 WHERE id = 99
      """
    Given statement
      """
      MERGE INTO iceberg_cow AS t
      USING (SELECT 99 AS id) AS s ON t.id = s.id
      WHEN MATCHED THEN DELETE
      """
    Then iceberg snapshot count is 2
    When query
      """
      SELECT COUNT(*) AS count FROM iceberg_cow
      """
    Then query result
      | count |
      | 4     |

  Scenario: COW operations support conditionless UPDATE and DELETE
    Given statement
      """
      UPDATE iceberg_cow SET value = id * 10
      """
    When query
      """
      SELECT * FROM iceberg_cow ORDER BY id
      """
    Then query result ordered
      | id | value | part |
      | 1  | 10    | A    |
      | 2  | 20    | A    |
      | 3  | 30    | B    |
      | 4  | 40    | B    |
    Given statement
      """
      DELETE FROM iceberg_cow
      """
    Then iceberg snapshot operation is delete
    Then iceberg snapshot count is 4
    Given statement
      """
      UPDATE iceberg_cow SET value = 0
      """
    Given statement
      """
      DELETE FROM iceberg_cow
      """
    Then iceberg snapshot count is 4
    When query
      """
      SELECT COUNT(*) AS count FROM iceberg_cow
      """
    Then query result
      | count |
      | 0     |

  Scenario Outline: EXPLAIN selects COW without writing
    When query
      """
      EXPLAIN <operation>
      """
    Then query plan matches snapshot
    Then iceberg snapshot count is 2
    Examples:
      | operation                                                                                                                             |
      | DELETE FROM iceberg_cow WHERE id = 1                                                                                                   |
      | UPDATE iceberg_cow SET value = value + 1 WHERE id = 1                                                                                   |
      | MERGE INTO iceberg_cow AS t USING (SELECT 1 AS id) AS s ON t.id = s.id WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id) |

  Scenario: COW MERGE rejects ambiguous updates before committing
    When query
      """
      MERGE INTO iceberg_cow AS t
      USING (SELECT * FROM VALUES (1, 100), (1, 200) AS src(id, value)) AS s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      """
    Then query error MERGE_CARDINALITY_VIOLATION
    Then iceberg snapshot count is 2
    When query
      """
      SELECT * FROM iceberg_cow ORDER BY id
      """
    Then query result ordered
      | id | value | part |
      | 1  | 10    | A    |
      | 2  | NULL  | A    |
      | 3  | 30    | B    |
      | 4  | 40    | B    |

  Scenario: COW UPDATE and DELETE support complex and floating columns
    Given variable complex_location for temporary directory iceberg_cow_complex
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_cow_complex
      """
    Given statement template
      """
      CREATE TABLE iceberg_cow_complex (id INT, payload STRUCT<x: INT, label: STRING>, tags ARRAY<INT>, score DOUBLE)
      USING iceberg LOCATION {{ complex_location.uri }}
      """
    Given statement
      """
      INSERT INTO iceberg_cow_complex VALUES
        (1, named_struct('x', 10, 'label', 'one'), array(1, 2), CAST('NaN' AS DOUBLE)),
        (2, named_struct('x', 20, 'label', 'two'), array(3), 2.5)
      """
    Given statement
      """
      UPDATE iceberg_cow_complex SET payload.x = payload.x + 1, tags = array(5, 6) WHERE id = 1
      """
    When query
      """
      SELECT id, payload.x AS x, payload.label AS label, size(tags) AS size FROM iceberg_cow_complex ORDER BY id
      """
    Then query result ordered
      | id | x  | label | size |
      | 1  | 11 | one   | 2    |
      | 2  | 20 | two   | 1    |
    Given statement
      """
      DELETE FROM iceberg_cow_complex WHERE isnan(score)
      """
    When query
      """
      SELECT id, payload.x AS x, score FROM iceberg_cow_complex
      """
    Then query result
      | id | x  | score |
      | 2  | 20 | 2.5   |

  Scenario: COW UPDATE supports aliases case-sensitive columns and null partition values
    Given config spark.sql.caseSensitive = true
    Given variable case_location for temporary directory iceberg_cow_case
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_cow_case
      """
    Given statement template
      """
      CREATE TABLE iceberg_cow_case (`Key` BIGINT, `Value` BIGINT, Part STRING)
      USING iceberg PARTITIONED BY (Part) LOCATION {{ case_location.uri }}
      """
    Given statement
      """
      INSERT INTO iceberg_cow_case VALUES (1, 10, NULL), (2, 20, NULL)
      """
    Given statement
      """
      UPDATE iceberg_cow_case AS t SET `Value` = t.`Value` + 1 WHERE t.`Key` = 1
      """
    Given statement
      """
      DELETE FROM iceberg_cow_case AS t WHERE t.`Key` IN (2, 3)
      """
    When query
      """
      SELECT `Key`, `Value`, Part FROM iceberg_cow_case
      """
    Then query result
      | Key | Value | Part |
      | 1   | 11    | NULL |
