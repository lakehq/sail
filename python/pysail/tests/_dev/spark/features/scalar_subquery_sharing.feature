Feature: Repeated scalar subqueries over catalog tables

  Background:
    Given variable dates for temporary directory scalar_sharing_dates
    Given variable other_dates for temporary directory scalar_sharing_other_dates
    Given final statement
      """
      DROP TABLE IF EXISTS scalar_sharing_dates
      """
    Given final statement
      """
      DROP TABLE IF EXISTS scalar_sharing_other_dates
      """
    Given statement template
      """
      CREATE TABLE scalar_sharing_dates (d DATE, w INT) USING parquet LOCATION {{ dates.sql }}
      """
    Given statement
      """
      INSERT INTO scalar_sharing_dates VALUES (DATE'2000-01-01', 1), (DATE'2000-01-08', 2)
      """
    Given statement template
      """
      CREATE TABLE scalar_sharing_other_dates (d DATE, w INT) USING parquet LOCATION {{ other_dates.sql }}
      """
    Given statement
      """
      INSERT INTO scalar_sharing_other_dates VALUES (DATE'2000-01-01', 1), (DATE'2000-01-08', 3)
      """

  @sail-only
  Scenario: Repeated scalar subqueries over one catalog table share one result
    When query
      """
      SELECT 'store' AS channel, sum(v) AS total
      FROM VALUES (1, 10), (2, 20), (2, 5), (3, 100) AS f(w, v)
      WHERE w = (SELECT w FROM scalar_sharing_dates WHERE d = DATE'2000-01-08')
      UNION ALL
      SELECT 'catalog' AS channel, sum(v) AS total
      FROM VALUES (1, 10), (2, 20), (2, 5), (3, 100) AS f(w, v)
      WHERE w = (SELECT w FROM scalar_sharing_dates WHERE d = DATE'2000-01-08')
      UNION ALL
      SELECT 'web' AS channel, sum(v) AS total
      FROM VALUES (1, 10), (2, 20), (2, 5), (3, 100) AS f(w, v)
      WHERE w = (SELECT w FROM scalar_sharing_dates WHERE d = DATE'2000-01-08')
      """
    Then query result
      | channel | total |
      | store   | 25    |
      | catalog | 25    |
      | web     | 25    |
    When query
      """
      EXPLAIN
      SELECT 'store' AS channel, sum(v) AS total
      FROM VALUES (1, 10), (2, 20), (2, 5), (3, 100) AS f(w, v)
      WHERE w = (SELECT w FROM scalar_sharing_dates WHERE d = DATE'2000-01-08')
      UNION ALL
      SELECT 'catalog' AS channel, sum(v) AS total
      FROM VALUES (1, 10), (2, 20), (2, 5), (3, 100) AS f(w, v)
      WHERE w = (SELECT w FROM scalar_sharing_dates WHERE d = DATE'2000-01-08')
      UNION ALL
      SELECT 'web' AS channel, sum(v) AS total
      FROM VALUES (1, 10), (2, 20), (2, 5), (3, 100) AS f(w, v)
      WHERE w = (SELECT w FROM scalar_sharing_dates WHERE d = DATE'2000-01-08')
      """
    Then query plan matches snapshot

  @sail-only
  Scenario: Scalar subqueries over different catalog tables with the same schema are not shared
    When query
      """
      SELECT
        (SELECT w FROM scalar_sharing_dates WHERE d = DATE'2000-01-08') AS a,
        (SELECT w FROM scalar_sharing_other_dates WHERE d = DATE'2000-01-08') AS b
      """
    Then query result
      | a | b |
      | 2 | 3 |
    When query
      """
      EXPLAIN
      SELECT
        (SELECT w FROM scalar_sharing_dates WHERE d = DATE'2000-01-08') AS a,
        (SELECT w FROM scalar_sharing_other_dates WHERE d = DATE'2000-01-08') AS b
      """
    Then query plan matches snapshot

  @sail-only
  Scenario: Repeated scalar subqueries over ranges and sorts share one result
    When query
      """
      SELECT
        (SELECT sum(id) FROM range(10)) AS a,
        (SELECT sum(id) FROM range(10)) AS b,
        (SELECT max(id) FROM (SELECT id FROM range(5) SORT BY id)) AS c,
        (SELECT max(id) FROM (SELECT id FROM range(5) SORT BY id)) AS d
      """
    Then query result
      | a  | b  | c | d |
      | 45 | 45 | 4 | 4 |
    When query
      """
      EXPLAIN
      SELECT
        (SELECT sum(id) FROM range(10)) AS a,
        (SELECT sum(id) FROM range(10)) AS b,
        (SELECT max(id) FROM (SELECT id FROM range(5) SORT BY id)) AS c,
        (SELECT max(id) FROM (SELECT id FROM range(5) SORT BY id)) AS d
      """
    Then query plan matches snapshot

  Scenario: Shared catalog table sources do not outlive the query
    When query
      """
      SELECT
        (SELECT w FROM scalar_sharing_dates WHERE d = DATE'2000-01-08') AS a,
        (SELECT w FROM scalar_sharing_dates WHERE d = DATE'2000-01-08') AS b
      """
    Then query result
      | a | b |
      | 2 | 2 |
    Given statement
      """
      INSERT OVERWRITE TABLE scalar_sharing_dates VALUES (DATE'2000-01-08', 5)
      """
    Then query result
      | a | b |
      | 5 | 5 |
