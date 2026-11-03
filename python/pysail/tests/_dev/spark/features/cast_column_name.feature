Feature: Cast column names
  Background:
    Given config spark.sql.ansi.enabled = true
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW t AS
      SELECT * FROM VALUES
        (1, TIMESTAMP'2024-01-02 03:04:05'),
        (2, TIMESTAMP'2024-02-03 04:05:06')
      AS t(k, ts)
      """
    Given final statement
      """
      DROP VIEW IF EXISTS t
      """

  Rule: a cast at the top of a SQL projection item is named after a named child

    Scenario: CAST of a column keeps the column name
      When query
        """
        SELECT CAST(k AS BIGINT) FROM t
        """
      Then query column types
        | column | type   |
        | k      | bigint |

    Scenario: TRY_CAST of a column keeps the column name
      When query
        """
        SELECT TRY_CAST(k AS BIGINT) FROM t
        """
      Then query column types
        | column | type   |
        | k      | bigint |

    Scenario: Double colon cast of a column keeps the column name
      When query
        """
        SELECT k::BIGINT FROM t
        """
      Then query column types
        | column | type   |
        | k      | bigint |

    Scenario: Cast of a timestamp column to date keeps the column name
      When query
        """
        SELECT CAST(ts AS DATE) FROM t
        """
      Then query column types
        | column | type |
        | ts     | date |

    Scenario: Cast of a literal is named after the cast
      When query
        """
        SELECT CAST('1' AS INT) FROM t
        """
      Then query column types
        | column         | type |
        | CAST(1 AS INT) | int  |

    Scenario: Double colon cast of a literal is named after the cast
      When query
        """
        SELECT '1'::INT FROM t
        """
      Then query column types
        | column         | type |
        | CAST(1 AS INT) | int  |

    Scenario: Cast of an unnamed expression is named after the cast
      When query
        """
        SELECT CAST(k + 1 AS BIGINT) FROM t
        """
      Then query column types
        | column                  | type   |
        | CAST((k + 1) AS BIGINT) | bigint |

    Scenario: Cast of a grouping key keeps the column name
      When query
        """
        SELECT CAST(k AS BIGINT) FROM t GROUP BY CAST(k AS BIGINT)
        """
      Then query column types
        | column | type   |
        | k      | bigint |

  Rule: SQL looks through a single cast only

    Scenario: Nested CAST of a column is named after both casts
      When query
        """
        SELECT CAST(CAST(k AS BIGINT) AS STRING) FROM t
        """
      Then query column types
        | column                            | type   |
        | CAST(CAST(k AS BIGINT) AS STRING) | string |

    Scenario: TRY_CAST over a CAST of a column is named after both casts
      When query
        """
        SELECT TRY_CAST(CAST(k AS BIGINT) AS STRING) FROM t
        """
      Then query column types
        | column                                | type   |
        | TRY_CAST(CAST(k AS BIGINT) AS STRING) | string |

    Scenario: Chained double colon casts of a column are named after both casts
      When query
        """
        SELECT k::BIGINT::STRING FROM t
        """
      Then query column types
        | column                            | type   |
        | CAST(CAST(k AS BIGINT) AS STRING) | string |

  Rule: a cast nested in another expression is named after the cast

    Scenario: Cast in an arithmetic expression is named after the cast
      When query
        """
        SELECT CAST(k AS BIGINT) + 1 FROM t
        """
      Then query column types
        | column                  | type   |
        | (CAST(k AS BIGINT) + 1) | bigint |

    Scenario: Cast in a function argument is named after the cast
      When query
        """
        SELECT abs(CAST(k AS BIGINT)) FROM t
        """
      Then query column types
        | column                 | type   |
        | abs(CAST(k AS BIGINT)) | bigint |

    Scenario: Cast in a struct field is named after the cast
      When query
        """
        SELECT struct(CAST(k AS BIGINT)) FROM t
        """
      Then query column types
        | column                    | type                |
        | struct(CAST(k AS BIGINT)) | struct<col1:bigint> |

    Scenario: Cast in an aggregate function argument is named after the cast
      When query
        """
        SELECT max(CAST(k AS BIGINT)) FROM t
        """
      Then query column types
        | column                 | type   |
        | max(CAST(k AS BIGINT)) | bigint |

    Scenario: Function over a column without a cast keeps the function name
      When query
        """
        SELECT to_date(ts) FROM t
        """
      Then query column types
        | column      | type |
        | to_date(ts) | date |

  Rule: the DataFrame API looks through a whole chain of casts

    Scenario: Cast of a DataFrame column keeps the column name
      When dataframe for cast of a column
      Then dataframe schema
        """
        root
         |-- k: long (nullable = false)
        """

    Scenario: Chained casts of a DataFrame column keep the column name
      When dataframe for chained casts of a column
      Then dataframe schema
        """
        root
         |-- k: string (nullable = false)
        """

    Scenario: Cast of a DataFrame column in arithmetic is named after the cast
      When dataframe for cast of a column in arithmetic
      Then dataframe schema
        """
        root
         |-- (CAST(k AS BIGINT) + 1): long (nullable = false)
        """

    Scenario: Chained casts parsed from a string in a DataFrame projection keep the column name
      When dataframe for chained casts of a column from a string expression
      Then dataframe schema
        """
        root
         |-- k: string (nullable = false)
        """
