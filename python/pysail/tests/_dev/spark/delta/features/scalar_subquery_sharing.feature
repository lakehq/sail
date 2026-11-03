Feature: Repeated scalar subqueries over Delta tables

  Background:
    Given variable location for temporary directory delta_scalar_sharing
    Given final statement
      """
      DROP TABLE IF EXISTS delta_scalar_sharing
      """
    Given statement template
      """
      CREATE TABLE delta_scalar_sharing (d DATE, w INT) USING DELTA LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO delta_scalar_sharing VALUES (DATE'2000-01-01', 1), (DATE'2000-01-08', 2)
      """
    Given statement
      """
      UPDATE delta_scalar_sharing SET w = 3 WHERE d = DATE'2000-01-08'
      """

  Scenario Outline: Repeated scalar subqueries over one Delta table version share one result
    When query
      """
      SELECT 'store' AS channel, sum(v) AS total
      FROM VALUES (1, 10), (2, 20), (2, 5), (3, 100) AS f(w, v)
      WHERE w = (SELECT w FROM delta_scalar_sharing <version> WHERE d = DATE'2000-01-08')
      UNION ALL
      SELECT 'catalog' AS channel, sum(v) AS total
      FROM VALUES (1, 10), (2, 20), (2, 5), (3, 100) AS f(w, v)
      WHERE w = (SELECT w FROM delta_scalar_sharing <version> WHERE d = DATE'2000-01-08')
      UNION ALL
      SELECT 'web' AS channel, sum(v) AS total
      FROM VALUES (1, 10), (2, 20), (2, 5), (3, 100) AS f(w, v)
      WHERE w = (SELECT w FROM delta_scalar_sharing <version> WHERE d = DATE'2000-01-08')
      """
    Then query result
      | channel | total   |
      | store   | <total> |
      | catalog | <total> |
      | web     | <total> |
    When query
      """
      EXPLAIN
      SELECT 'store' AS channel, sum(v) AS total
      FROM VALUES (1, 10), (2, 20), (2, 5), (3, 100) AS f(w, v)
      WHERE w = (SELECT w FROM delta_scalar_sharing <version> WHERE d = DATE'2000-01-08')
      UNION ALL
      SELECT 'catalog' AS channel, sum(v) AS total
      FROM VALUES (1, 10), (2, 20), (2, 5), (3, 100) AS f(w, v)
      WHERE w = (SELECT w FROM delta_scalar_sharing <version> WHERE d = DATE'2000-01-08')
      UNION ALL
      SELECT 'web' AS channel, sum(v) AS total
      FROM VALUES (1, 10), (2, 20), (2, 5), (3, 100) AS f(w, v)
      WHERE w = (SELECT w FROM delta_scalar_sharing <version> WHERE d = DATE'2000-01-08')
      """
    Then query plan matches snapshot

    Examples:
      | version         | total |
      |                 | 100   |
      | VERSION AS OF 1 | 25    |

  Scenario Outline: Scalar subqueries over different Delta table versions are not shared
    When query
      """
      SELECT
        (SELECT w FROM delta_scalar_sharing <left> WHERE d = DATE'2000-01-08') AS a,
        (SELECT w FROM delta_scalar_sharing <right> WHERE d = DATE'2000-01-08') AS b
      """
    Then query result
      | a   | b   |
      | <a> | <b> |
    When query
      """
      EXPLAIN
      SELECT
        (SELECT w FROM delta_scalar_sharing <left> WHERE d = DATE'2000-01-08') AS a,
        (SELECT w FROM delta_scalar_sharing <right> WHERE d = DATE'2000-01-08') AS b
      """
    Then query plan matches snapshot

    Examples:
      | left            | right           | a | b |
      | VERSION AS OF 1 | VERSION AS OF 2 | 2 | 3 |
      | VERSION AS OF 1 |                 | 2 | 3 |
