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

  Rule: Spark-compatible coercion for temporal arguments

    Scenario Outline: COALESCE combines <kind> and <timestamp_type> in <zone> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      And config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT coalesce(NULL, first, second) AS forward,
               coalesce(NULL, second, first) AS reversed
        FROM VALUES
          (0, <first>, <timestamp_type> '2024-07-02 03:04:05.654321'),
          (1, CAST(NULL AS <kind>), <timestamp_type> '2024-07-02 03:04:05.654321'),
          (2, <first>, CAST(NULL AS <timestamp_type>)),
          (3, CAST(NULL AS <kind>), CAST(NULL AS <timestamp_type>))
        AS t(id, first, second) ORDER BY id
        """
      Then query result ordered
        | forward                    | reversed                   |
        | <first_value>              | 2024-07-02 03:04:05.654321 |
        | 2024-07-02 03:04:05.654321 | 2024-07-02 03:04:05.654321 |
        | <first_value>              | <first_value>              |
        | NULL                       | NULL                       |
      And query schema
        """
        root
         |-- forward: <result_type> (nullable = true)
         |-- reversed: <result_type> (nullable = true)
        """

      Examples:
        | kind          | timestamp_type | zone                | ansi  | first                                   | first_value                | result_type   |
        | DATE          | TIMESTAMP_LTZ  | UTC                 | false | DATE '2024-07-01'                        | 2024-07-01 00:00:00         | timestamp     |
        | DATE          | TIMESTAMP_LTZ  | UTC                 | true  | DATE '2024-07-01'                        | 2024-07-01 00:00:00         | timestamp     |
        | DATE          | TIMESTAMP_LTZ  | America/Los_Angeles | false | DATE '2024-07-01'                        | 2024-07-01 00:00:00         | timestamp     |
        | DATE          | TIMESTAMP_LTZ  | America/Los_Angeles | true  | DATE '2024-07-01'                        | 2024-07-01 00:00:00         | timestamp     |
        | DATE          | TIMESTAMP_NTZ  | UTC                 | false | DATE '2024-07-01'                        | 2024-07-01 00:00:00         | timestamp_ntz |
        | DATE          | TIMESTAMP_NTZ  | UTC                 | true  | DATE '2024-07-01'                        | 2024-07-01 00:00:00         | timestamp_ntz |
        | DATE          | TIMESTAMP_NTZ  | America/Los_Angeles | false | DATE '2024-07-01'                        | 2024-07-01 00:00:00         | timestamp_ntz |
        | DATE          | TIMESTAMP_NTZ  | America/Los_Angeles | true  | DATE '2024-07-01'                        | 2024-07-01 00:00:00         | timestamp_ntz |
        | TIMESTAMP_NTZ | TIMESTAMP_LTZ  | UTC                 | false | TIMESTAMP_NTZ '2024-07-01 01:02:03.123456' | 2024-07-01 01:02:03.123456 | timestamp     |
        | TIMESTAMP_NTZ | TIMESTAMP_LTZ  | UTC                 | true  | TIMESTAMP_NTZ '2024-07-01 01:02:03.123456' | 2024-07-01 01:02:03.123456 | timestamp     |
        | TIMESTAMP_NTZ | TIMESTAMP_LTZ  | America/Los_Angeles | false | TIMESTAMP_NTZ '2024-07-01 01:02:03.123456' | 2024-07-01 01:02:03.123456 | timestamp     |
        | TIMESTAMP_NTZ | TIMESTAMP_LTZ  | America/Los_Angeles | true  | TIMESTAMP_NTZ '2024-07-01 01:02:03.123456' | 2024-07-01 01:02:03.123456 | timestamp     |

    Scenario: COALESCE includes temporal cast nullability and all argument types
      When query
        """
        SELECT
          coalesce(DATE '2024-07-01', CAST(NULL AS TIMESTAMP_NTZ)) AS nullable_ntz,
          coalesce(DATE '2024-07-01', TIMESTAMP_NTZ '2024-07-02 03:04:05') AS nonnull_ntz,
          coalesce(NULL, DATE '2024-07-01', TIMESTAMP_NTZ '2024-07-02 03:04:05',
                   TIMESTAMP_LTZ '2024-07-03 04:05:06') AS nonnull_ltz
        """
      Then query result
        | nullable_ntz        | nonnull_ntz         | nonnull_ltz         |
        | 2024-07-01 00:00:00 | 2024-07-01 00:00:00 | 2024-07-01 00:00:00 |
      And query schema
        """
        root
         |-- nullable_ntz: timestamp_ntz (nullable = true)
         |-- nonnull_ntz: timestamp_ntz (nullable = false)
         |-- nonnull_ltz: timestamp (nullable = false)
        """

    Scenario Outline: COALESCE leaves an unselected temporal error lazy with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT coalesce(date_add(DATE '2024-07-01', CAST(id AS INT)),
                        CAST(raise_error('unselected temporal error') AS TIMESTAMP_NTZ)) AS result
        FROM range(2) ORDER BY id
        """
      Then query result ordered
        | result              |
        | 2024-07-01 00:00:00 |
        | 2024-07-02 00:00:00 |

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario: COALESCE evaluates a selected temporal error
      When query
        """
        SELECT coalesce(CAST(NULL AS DATE),
                        CAST(raise_error('selected temporal error') AS TIMESTAMP_NTZ)) AS result
        """
      Then query error selected temporal error

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
