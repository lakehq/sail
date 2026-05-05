Feature: simple sort test

  @sail-only
  Scenario: simple presorted
    Given variable location for temporary directory simple_presorted
    Given final statement
      """
      DROP TABLE IF EXISTS simple_presorted_t
      """
    Given statement template
      """
      CREATE TABLE simple_presorted_t USING PARQUET LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES (3), (1), (2) AS t(col) ORDER BY col
      """
    When query
      """
      EXPLAIN SELECT col FROM simple_presorted_t ORDER BY col
      """
    Then query plan matches snapshot

  Scenario: CTAS ORDER BY ASC puts NULLs first (Spark default)
    Given variable location for temporary directory sort_nulls_asc
    Given final statement
      """
      DROP TABLE IF EXISTS sort_nulls_asc_t
      """
    Given statement template
      """
      CREATE TABLE sort_nulls_asc_t USING PARQUET LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES (3), (NULL), (1), (NULL), (2) AS t(col) ORDER BY col
      """
    When query
      """
      SELECT col FROM sort_nulls_asc_t ORDER BY col
      """
    Then query result ordered
      | col  |
      | NULL |
      | NULL |
      | 1    |
      | 2    |
      | 3    |

  Scenario: CTAS ORDER BY DESC puts NULLs last (Spark default)
    Given variable location for temporary directory sort_nulls_desc
    Given final statement
      """
      DROP TABLE IF EXISTS sort_nulls_desc_t
      """
    Given statement template
      """
      CREATE TABLE sort_nulls_desc_t USING PARQUET LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES (3), (NULL), (1), (NULL), (2) AS t(col) ORDER BY col DESC
      """
    When query
      """
      SELECT col FROM sort_nulls_desc_t ORDER BY col DESC
      """
    Then query result ordered
      | col  |
      | 3    |
      | 2    |
      | 1    |
      | NULL |
      | NULL |
