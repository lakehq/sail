Feature: CTAS ORDER BY produces globally sorted output

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

  Scenario: CTAS ORDER BY multi-column ASC then DESC
    Given variable location for temporary directory ctas_order_multi
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_order_multi_t
      """
    Given statement template
      """
      CREATE TABLE ctas_order_multi_t USING PARQUET LOCATION {{ location.sql }}
      AS SELECT col1, col2 FROM VALUES (1, 'b'), (2, 'a'), (1, 'a'), (2, 'b') AS t(col1, col2)
      ORDER BY col1 ASC, col2 DESC
      """
    When query
      """
      SELECT col1, col2 FROM ctas_order_multi_t
      """
    Then query result ordered
      | col1 | col2 |
      | 1    | b    |
      | 1    | a    |
      | 2    | b    |
      | 2    | a    |

  Scenario: CTAS ORDER BY DOUBLE ASC — NaN sorts after Infinity
    Given variable location for temporary directory ctas_order_nan_asc
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_order_nan_asc_t
      """
    Given statement template
      """
      CREATE TABLE ctas_order_nan_asc_t USING PARQUET LOCATION {{ location.sql }}
      AS SELECT v FROM VALUES
        (CAST('NaN' AS DOUBLE)), (2.0), (CAST('Infinity' AS DOUBLE)),
        (1.0), (CAST('-Infinity' AS DOUBLE))
      AS t(v) ORDER BY v ASC
      """
    When query
      """
      SELECT v FROM ctas_order_nan_asc_t
      """
    Then query result ordered
      | v         |
      | -Infinity |
      | 1.0       |
      | 2.0       |
      | Infinity  |
      | NaN       |

  Scenario: CTAS ORDER BY DOUBLE DESC — NaN sorts first
    Given variable location for temporary directory ctas_order_nan_desc
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_order_nan_desc_t
      """
    Given statement template
      """
      CREATE TABLE ctas_order_nan_desc_t USING PARQUET LOCATION {{ location.sql }}
      AS SELECT v FROM VALUES
        (CAST('NaN' AS DOUBLE)), (2.0), (CAST('Infinity' AS DOUBLE)),
        (1.0), (CAST('-Infinity' AS DOUBLE))
      AS t(v) ORDER BY v DESC
      """
    When query
      """
      SELECT v FROM ctas_order_nan_desc_t
      """
    Then query result ordered
      | v         |
      | NaN       |
      | Infinity  |
      | 2.0       |
      | 1.0       |
      | -Infinity |

  Scenario: CTAS ORDER BY DOUBLE ASC with NULLs — NULLs first, NaN last
    Given variable location for temporary directory ctas_order_nan_null_asc
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_order_nan_null_asc_t
      """
    Given statement template
      """
      CREATE TABLE ctas_order_nan_null_asc_t USING PARQUET LOCATION {{ location.sql }}
      AS SELECT v FROM VALUES
        (CAST('NaN' AS DOUBLE)), (CAST(NULL AS DOUBLE)), (2.0),
        (CAST('Infinity' AS DOUBLE)), (1.0)
      AS t(v) ORDER BY v ASC
      """
    When query
      """
      SELECT v FROM ctas_order_nan_null_asc_t
      """
    Then query result ordered
      | v        |
      | NULL     |
      | 1.0      |
      | 2.0      |
      | Infinity |
      | NaN      |

  Scenario: CTAS ORDER BY ASC with multi-partition source preserves physical row order
    Given variable location_src for temporary directory ctas_mp_asc_src
    Given variable location_dst for temporary directory ctas_mp_asc_dst
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_mp_asc_src_t
      """
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_mp_asc_dst_t
      """
    Given statement template
      """
      CREATE TABLE ctas_mp_asc_src_t USING PARQUET LOCATION {{ location_src.sql }}
      AS SELECT * FROM VALUES (5), (3), (1) AS t(col)
      """
    Given statement template
      """
      INSERT INTO ctas_mp_asc_src_t SELECT * FROM VALUES (4), (2) AS t(col)
      """
    Given statement template
      """
      CREATE TABLE ctas_mp_asc_dst_t USING PARQUET LOCATION {{ location_dst.sql }}
      AS SELECT col FROM ctas_mp_asc_src_t ORDER BY col ASC
      """
    When query
      """
      SELECT col FROM ctas_mp_asc_dst_t
      """
    Then query result
      | col |
      | 1   |
      | 2   |
      | 3   |
      | 4   |
      | 5   |

  Scenario: CTAS ORDER BY DESC with multi-partition source preserves physical row order
    Given variable location_src for temporary directory ctas_mp_desc_src
    Given variable location_dst for temporary directory ctas_mp_desc_dst
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_mp_desc_src_t
      """
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_mp_desc_dst_t
      """
    Given statement template
      """
      CREATE TABLE ctas_mp_desc_src_t USING PARQUET LOCATION {{ location_src.sql }}
      AS SELECT * FROM VALUES (1), (3), (5) AS t(col)
      """
    Given statement template
      """
      INSERT INTO ctas_mp_desc_src_t SELECT * FROM VALUES (2), (4) AS t(col)
      """
    Given statement template
      """
      CREATE TABLE ctas_mp_desc_dst_t USING PARQUET LOCATION {{ location_dst.sql }}
      AS SELECT col FROM ctas_mp_desc_src_t ORDER BY col DESC
      """
    When query
      """
      SELECT col FROM ctas_mp_desc_dst_t
      """
    Then query result
      | col |
      | 5   |
      | 4   |
      | 3   |
      | 2   |
      | 1   |

  @sail-only
  Scenario: CTAS ORDER BY with multi-partition source writes physically sorted file
    Given variable location_src for temporary directory ctas_physical_src
    Given variable location_dst for temporary directory ctas_physical_dst
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_physical_src_t
      """
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_physical_dst_t
      """
    Given statement template
      """
      CREATE TABLE ctas_physical_src_t USING PARQUET LOCATION {{ location_src.sql }}
      AS SELECT * FROM VALUES (9), (3), (7), (1), (5) AS t(col)
      """
    Given statement template
      """
      INSERT INTO ctas_physical_src_t
      SELECT * FROM VALUES (8), (2), (6), (4), (10) AS t(col)
      """
    Given statement template
      """
      CREATE TABLE ctas_physical_dst_t USING PARQUET LOCATION {{ location_dst.sql }}
      AS SELECT col FROM ctas_physical_src_t ORDER BY col ASC
      """
    When query
      """
      SELECT col FROM ctas_physical_dst_t
      """
    Then query result ordered
      | col |
      | 1   |
      | 2   |
      | 3   |
      | 4   |
      | 5   |
      | 6   |
      | 7   |
      | 8   |
      | 9   |
      | 10  |
