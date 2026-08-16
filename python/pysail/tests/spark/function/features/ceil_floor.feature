Feature: ceil and floor functions

  Rule: ceil and floor on integer inputs return the same value as BIGINT

    Scenario: ceil on INT returns same value as BIGINT
      When query
        """
        SELECT ceil(CAST(5 AS INT)) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: ceil on BIGINT returns same value
      When query
        """
        SELECT ceil(CAST(5 AS BIGINT)) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: ceil on negative INT returns same value
      When query
        """
        SELECT ceil(CAST(-3 AS INT)) AS result
        """
      Then query result
        | result |
        | -3     |

    Scenario: ceil on NULL INT returns NULL
      When query
        """
        SELECT ceil(CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: floor on INT returns same value as BIGINT
      When query
        """
        SELECT floor(CAST(5 AS INT)) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: floor on BIGINT returns same value
      When query
        """
        SELECT floor(CAST(5 AS BIGINT)) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: floor on negative INT returns same value
      When query
        """
        SELECT floor(CAST(-3 AS INT)) AS result
        """
      Then query result
        | result |
        | -3     |

    Scenario: floor on NULL INT returns NULL
      When query
        """
        SELECT floor(CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: ceil on INT column returns BIGINT schema
      When query
        """
        SELECT ceil(CAST(5 AS INT)) AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: floor on INT column returns BIGINT schema
      When query
        """
        SELECT floor(CAST(5 AS INT)) AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

  Rule: ceil and floor on float inputs

    Scenario: ceil on positive float rounds up
      When query
        """
        SELECT ceil(1.5) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: ceil on negative float rounds toward zero
      When query
        """
        SELECT ceil(-1.5) AS result
        """
      Then query result
        | result |
        | -1     |

    Scenario: floor on positive float rounds down
      When query
        """
        SELECT floor(1.9) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: floor on negative float rounds away from zero
      When query
        """
        SELECT floor(-1.1) AS result
        """
      Then query result
        | result |
        | -2     |

  Rule: Plan snapshot — ORDER BY on pre-sorted Parquet column eliminates SortExec

    @sail-only
    Scenario: EXPLAIN ORDER BY on pre-sorted Parquet column eliminates SortExec
      Given variable location for temporary directory ceil_sort_parquet
      Given final statement
        """
        DROP TABLE IF EXISTS ceil_sort_parquet_t
        """
      Given statement template
        """
        CREATE TABLE ceil_sort_parquet_t
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES (3), (1), (5), (2), (4) AS t(col)
        ORDER BY col
        """
      When query
        """
        EXPLAIN SELECT ceil(col) AS c FROM ceil_sort_parquet_t ORDER BY ceil(col)
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN ORDER BY floor on pre-sorted Parquet column keeps SortExec
      Given variable location for temporary directory floor_sort_parquet
      Given final statement
        """
        DROP TABLE IF EXISTS floor_sort_parquet_t
        """
      Given statement template
        """
        CREATE TABLE floor_sort_parquet_t
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES (3), (1), (5), (2), (4) AS t(col)
        ORDER BY col
        """
      When query
        """
        EXPLAIN SELECT floor(col) AS c FROM floor_sort_parquet_t ORDER BY floor(col)
        """
      Then query plan matches snapshot

  Rule: Result correctness on Parquet

    @sail-only
    Scenario: ceil on Parquet column returns correct ordered results
      Given variable location for temporary directory ceil_result_parquet
      Given final statement
        """
        DROP TABLE IF EXISTS ceil_result_parquet_t
        """
      Given statement template
        """
        CREATE TABLE ceil_result_parquet_t
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES (1), (3), (2), (5), (4) AS t(col)
        """
      When query
        """
        SELECT ceil(col) AS c FROM ceil_result_parquet_t ORDER BY col
        """
      Then query result ordered
        | c |
        | 1 |
        | 2 |
        | 3 |
        | 4 |
        | 5 |
