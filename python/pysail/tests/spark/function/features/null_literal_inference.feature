Feature: NULL literal and timestamp inference

  Rule: untyped NULL literals

    @spark-4
    Scenario: SQL NULL literal is inferred as void
      When query
      """
      SELECT NULL AS result
      """
      Then query schema
      """
      root
       |-- result: void (nullable = true)
      """
      Then query result
      | result |
      | NULL   |

    @spark-4
    Scenario: SQL NULL literal remains void across aliases and projections
      When query
      """
      SELECT result
      FROM (SELECT NULL AS result) AS t
      """
      Then query schema
      """
      root
       |-- result: void (nullable = true)
      """
      Then query result
      | result |
      | NULL   |

    Scenario: SQL NULL literal type is visible to downstream expressions
      When query
      """
      SELECT typeof(result) AS result
      FROM (SELECT NULL AS result) AS t
      """
      Then query schema
      """
      root
       |-- result: string (nullable = false)
      """
      Then query result
      | result |
      | void   |

    @spark-4
    Scenario: DataFrame NULL literal is inferred as void
      When dataframe for null literal
      Then dataframe schema
      """
      root
       |-- result: void (nullable = true)
      """

    @spark-4
    Scenario: DataFrame NULL literal remains void across aliases and projections
      When dataframe for null literal alias projection
      Then dataframe schema
      """
      root
       |-- result: void (nullable = true)
      """

    @spark-4
    Scenario: DataFrame NULL literal remains void through withColumn and select
      When dataframe for null literal with column
      Then dataframe schema
      """
      root
       |-- result: void (nullable = true)
      """

  Rule: timestamp conversion of untyped NULL literals

    Scenario Outline: SQL timestamp conversion of NULL expression is typed
      When query
      """
      SELECT <function>(NULL) AS result
      """
      Then query schema
      """
      root
       |-- result: <type> (nullable = true)
      """
      Then query result
      | result |
      | NULL   |

      Examples:
      | function         | type          |
      | to_timestamp     | timestamp     |
      | try_to_timestamp | timestamp     |
      | to_timestamp_ltz | timestamp     |
      | to_timestamp_ntz | timestamp_ntz |

    Scenario Outline: SQL timestamp conversion with NULL expression and format is typed
      When query
      """
      SELECT <function>(NULL, 'yyyy-MM-dd') AS result
      """
      Then query schema
      """
      root
       |-- result: <type> (nullable = true)
      """
      Then query result
      | result |
      | NULL   |

      Examples:
      | function         | type          |
      | to_timestamp     | timestamp     |
      | try_to_timestamp | timestamp     |
      | to_timestamp_ltz | timestamp     |
      | to_timestamp_ntz | timestamp_ntz |

    Scenario Outline: SQL timestamp conversion with NULL format is typed and returns NULL
      When query
      """
      SELECT <function>('2024-01-02', NULL) AS result
      """
      Then query schema
      """
      root
       |-- result: <type> (nullable = true)
      """
      Then query result
      | result |
      | NULL   |

      Examples:
      | function         | type          |
      | to_timestamp     | timestamp     |
      | try_to_timestamp | timestamp     |
      | to_timestamp_ltz | timestamp     |
      | to_timestamp_ntz | timestamp_ntz |

    Scenario Outline: SQL timestamp conversion with NULL expression and NULL format is typed
      When query
      """
      SELECT <function>(NULL, NULL) AS result
      """
      Then query schema
      """
      root
       |-- result: <type> (nullable = true)
      """
      Then query result
      | result |
      | NULL   |

      Examples:
      | function         | type          |
      | to_timestamp     | timestamp     |
      | try_to_timestamp | timestamp     |
      | to_timestamp_ltz | timestamp     |
      | to_timestamp_ntz | timestamp_ntz |

    Scenario Outline: SQL timestamp conversion keeps type across aliases and projections
      When query
      """
      SELECT result
      FROM (SELECT <function>(NULL, 'yyyy-MM-dd') AS result) AS t
      """
      Then query schema
      """
      root
       |-- result: <type> (nullable = true)
      """
      Then query result
      | result |
      | NULL   |

      Examples:
      | function         | type          |
      | to_timestamp     | timestamp     |
      | try_to_timestamp | timestamp     |
      | to_timestamp_ltz | timestamp     |
      | to_timestamp_ntz | timestamp_ntz |

    Scenario: DataFrame to_timestamp of NULL literal is inferred as timestamp
      When dataframe for to_timestamp null literal
      Then dataframe schema
      """
      root
       |-- result: timestamp (nullable = true)
      """

    Scenario: DataFrame try_to_timestamp of NULL literal with format is inferred as timestamp
      When dataframe for try_to_timestamp null literal with format
      Then dataframe schema
      """
      root
       |-- result: timestamp (nullable = true)
      """

    Scenario: DataFrame try_to_timestamp of value with NULL format is inferred as timestamp
      When dataframe for try_to_timestamp value with null format
      Then dataframe schema
      """
      root
       |-- result: timestamp (nullable = true)
      """

    Scenario: DataFrame to_timestamp_ltz of NULL literal with format is inferred as timestamp
      When dataframe for to_timestamp_ltz null literal with format
      Then dataframe schema
      """
      root
       |-- result: timestamp (nullable = true)
      """

    Scenario: DataFrame to_timestamp_ltz of value with NULL format is inferred as timestamp
      When dataframe for to_timestamp_ltz value with null format
      Then dataframe schema
      """
      root
       |-- result: timestamp (nullable = true)
      """

    Scenario: DataFrame to_timestamp_ntz of NULL literal with format keeps timestamp_ntz
      When dataframe for to_timestamp_ntz null literal with format
      Then dataframe schema
      """
      root
       |-- result: timestamp_ntz (nullable = true)
      """

    Scenario: DataFrame to_timestamp_ntz of value with NULL format keeps timestamp_ntz
      When dataframe for to_timestamp_ntz value with null format
      Then dataframe schema
      """
      root
       |-- result: timestamp_ntz (nullable = true)
      """

    Scenario: DataFrame to_timestamp of NULL literal with format is inferred as timestamp
      When dataframe for to_timestamp null literal with format
      Then dataframe schema
      """
      root
       |-- result: timestamp (nullable = true)
      """

    Scenario: DataFrame to_timestamp follows configured default timestamp type
      Given config spark.sql.timestampType = TIMESTAMP_NTZ
      When dataframe for to_timestamp null literal
      Then dataframe schema
      """
      root
       |-- result: timestamp_ntz (nullable = true)
      """

    Scenario: DataFrame try_to_timestamp follows configured default timestamp type
      Given config spark.sql.timestampType = TIMESTAMP_NTZ
      When dataframe for try_to_timestamp null literal with format
      Then dataframe schema
      """
      root
       |-- result: timestamp_ntz (nullable = true)
      """
