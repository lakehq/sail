Feature: Binary grouping optimization

  Scenario: Binary DISTINCT uses LargeBinary grouping
    When query
      """
      SELECT DISTINCT k FROM VALUES (X'01'), (X'01') t(k)
      """
    Then query result
      | k    |
      | [01] |
    When query
      """
      EXPLAIN CODEGEN SELECT DISTINCT k FROM VALUES (X'01'), (X'01') t(k)
      """
    Then query plan matches snapshot

  Scenario: Binary DISTINCT preserves nulls, empty values, and arbitrary bytes
    When query
      """
      SELECT DISTINCT k
      FROM VALUES (X'00FF80'), (X'00FF80'), (X''), (CAST(NULL AS BINARY)) t(k)
      ORDER BY k
      """
    Then query schema
      """
      root
       |-- k: binary (nullable = true)
      """
    Then query result
      | k          |
      | NULL       |
      | []         |
      | [00 FF 80] |

  Scenario: Binary grouping preserves multiple keys and binary aggregate results
    When query
      """
      SELECT k, i, min(k) AS lo, max(k) AS hi, count(*) AS n
      FROM VALUES
        (X'00FF80', 1), (X'00FF80', 1), (X'', 2), (CAST(NULL AS BINARY), 3) t(k, i)
      GROUP BY k, i
      ORDER BY i
      """
    Then query schema
      """
      root
       |-- k: binary (nullable = true)
       |-- i: integer (nullable = false)
       |-- lo: binary (nullable = true)
       |-- hi: binary (nullable = true)
       |-- n: long (nullable = false)
      """
    Then query result
      | k          | i | lo         | hi         | n |
      | [00 FF 80] | 1 | [00 FF 80] | [00 FF 80] | 2 |
      | []         | 2 | []         | []         | 1 |
      | NULL       | 3 | NULL       | NULL       | 1 |

  Scenario: Binary grouping sets distinguish null keys from the total
    When query
      """
      SELECT k, count(*) AS n, grouping(k) AS g
      FROM VALUES (X'00FF80'), (X'00FF80'), (X''), (CAST(NULL AS BINARY)) t(k)
      GROUP BY GROUPING SETS ((k), ())
      ORDER BY g, k
      """
    Then query result
      | k          | n | g |
      | NULL       | 1 | 0 |
      | []         | 1 | 0 |
      | [00 FF 80] | 2 | 0 |
      | NULL       | 4 | 1 |
