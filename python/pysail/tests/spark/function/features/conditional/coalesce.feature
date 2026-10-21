Feature: coalesce returns the first non-null argument

  Rule: Spark-compatible coercion for mixed string and temporal arguments

    Scenario Outline: Coercion: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        WITH t(<cols>) AS (
          SELECT <vals>
        )
        SELECT
          coalesce(<args>) AS result,
          typeof(coalesce(<args>)) AS result_type
        FROM t
        """
      Then query result
        | result   | result_type |
        | <result> | string      |

      Examples:
        | case                                                                               | cols                      | vals                                                  | args                      | result              |
        | Coalesce null string column falls back to date column as string                    | string_col, date_col      | CAST(NULL AS STRING), DATE '2024-01-15'               | string_col, date_col      | 2024-01-15          |
        | Coalesce null string column falls back to timestamp column as string               | string_col, timestamp_col | CAST(NULL AS STRING), TIMESTAMP '2024-01-15 10:30:00' | string_col, timestamp_col | 2024-01-15 10:30:00 |
        | Coalesce string literal before a date column wins without temporal casting         | date_col                  | DATE '2024-01-15'                                     | 'default', date_col       | default             |
        | Coalesce non-null string column before a date column wins without temporal casting | string_col, date_col      | CAST('hello' AS STRING), DATE '2024-01-15'            | string_col, date_col      | hello               |
        | Coalesce date column before a string column returns a string value                 | date_col, string_col      | DATE '2024-01-15', CAST('fallback' AS STRING)         | date_col, string_col      | 2024-01-15          |

    Scenario: Coalesce multiple mixed temporal arguments still coerces to string
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          coalesce(
            CAST(NULL AS STRING),
            CAST(NULL AS DATE),
            TIMESTAMP '2024-01-15 10:30:00'
          ) AS result,
          typeof(coalesce(
            CAST(NULL AS STRING),
            CAST(NULL AS DATE),
            TIMESTAMP '2024-01-15 10:30:00'
          )) AS result_type
        """
      Then query result
        | result              | result_type |
        | 2024-01-15 10:30:00 | string      |

    Scenario: Coalesce all-null mixed string and date arguments returns null with string type
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          coalesce(CAST(NULL AS STRING), CAST(NULL AS DATE)) AS result,
          typeof(coalesce(CAST(NULL AS STRING), CAST(NULL AS DATE))) AS result_type
        """
      Then query result
        | result | result_type |
        | NULL   | string      |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null argument yields a non-nullable value
      When query
        """
        SELECT coalesce(1, 2) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a non-null column argument yields a non-nullable value
      When query
        """
        SELECT coalesce(id, 0) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    Scenario: all-nullable arguments stay nullable
      When query
        """
        SELECT coalesce(c, d) AS result FROM VALUES (CAST(NULL AS INT), CAST(NULL AS INT)) AS t(c, d)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """


  Rule: Struct field comments do not distinguish values

    Scenario Outline: Coalesce parsed structs with different <format> comments
      When query
        """
        SELECT coalesce(<left>, <right>).x AS result
        """
      Then query result
        | result |
        | 1      |

      Examples:
        | format | left                                          | right                              |
        | CSV    | from_csv('1', 'x INT COMMENT "a"')            | from_csv('2', 'x INT')             |
        | JSON   | from_json('{"x":1}', 'x INT COMMENT "a"')     | from_json('{"x":2}', 'x INT')      |
