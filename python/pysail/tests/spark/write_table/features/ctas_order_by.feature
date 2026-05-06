Feature: CTAS ORDER BY produces globally sorted output

  @ctas_order_by
  Scenario: CTAS ORDER BY ASC writes globally sorted data
    Given variable location for temporary directory ctas_order_asc
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_order_asc_t
      """
    Given statement template
      """
      CREATE TABLE ctas_order_asc_t USING PARQUET LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES (5), (3), (1), (4), (2) AS t(col) ORDER BY col ASC
      """
    When query
      """
      SELECT col FROM ctas_order_asc_t
      """
    Then query result ordered
      | col |
      | 1   |
      | 2   |
      | 3   |
      | 4   |
      | 5   |

  @ctas_order_by
  Scenario: CTAS ORDER BY DESC writes globally sorted data
    Given variable location for temporary directory ctas_order_desc
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_order_desc_t
      """
    Given statement template
      """
      CREATE TABLE ctas_order_desc_t USING PARQUET LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES (5), (3), (1), (4), (2) AS t(col) ORDER BY col DESC
      """
    When query
      """
      SELECT col FROM ctas_order_desc_t
      """
    Then query result ordered
      | col |
      | 5   |
      | 4   |
      | 3   |
      | 2   |
      | 1   |

  @ctas_order_by
  Scenario: CTAS ORDER BY ASC with NULLs — NULLs first (Spark default)
    Given variable location for temporary directory ctas_order_nulls_asc
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_order_nulls_asc_t
      """
    Given statement template
      """
      CREATE TABLE ctas_order_nulls_asc_t USING PARQUET LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES (3), (NULL), (1), (NULL), (2) AS t(col) ORDER BY col ASC
      """
    When query
      """
      SELECT col FROM ctas_order_nulls_asc_t
      """
    Then query result ordered
      | col  |
      | NULL |
      | NULL |
      | 1    |
      | 2    |
      | 3    |

  @ctas_order_by
  Scenario: CTAS ORDER BY DESC with NULLs — NULLs last (Spark default)
    Given variable location for temporary directory ctas_order_nulls_desc
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_order_nulls_desc_t
      """
    Given statement template
      """
      CREATE TABLE ctas_order_nulls_desc_t USING PARQUET LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES (3), (NULL), (1), (NULL), (2) AS t(col) ORDER BY col DESC
      """
    When query
      """
      SELECT col FROM ctas_order_nulls_desc_t
      """
    Then query result ordered
      | col  |
      | 3    |
      | 2    |
      | 1    |
      | NULL |
      | NULL |
