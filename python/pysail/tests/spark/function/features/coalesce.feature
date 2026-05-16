Feature: coalesce returns the first non-null argument

  Rule: Spark-compatible coercion for mixed string and temporal arguments

    Scenario: Coalesce null string column falls back to date column as string
      Given config spark.sql.ansi.enabled = false
      When query
      """
      WITH t(string_col, date_col) AS (
        SELECT CAST(NULL AS STRING), DATE '2024-01-15'
      )
      SELECT
        coalesce(string_col, date_col) AS result,
        typeof(coalesce(string_col, date_col)) AS result_type
      FROM t
      """
      Then query result
      | result     | result_type |
      | 2024-01-15 | string      |

    Scenario: Coalesce null string column falls back to timestamp column as string
      Given config spark.sql.ansi.enabled = false
      When query
      """
      WITH t(string_col, timestamp_col) AS (
        SELECT CAST(NULL AS STRING), TIMESTAMP '2024-01-15 10:30:00'
      )
      SELECT
        coalesce(string_col, timestamp_col) AS result,
        typeof(coalesce(string_col, timestamp_col)) AS result_type
      FROM t
      """
      Then query result
      | result              | result_type |
      | 2024-01-15 10:30:00 | string      |

    Scenario: Coalesce string literal before a date column wins without temporal casting
      Given config spark.sql.ansi.enabled = false
      When query
      """
      WITH t(date_col) AS (
        SELECT DATE '2024-01-15'
      )
      SELECT
        coalesce('default', date_col) AS result,
        typeof(coalesce('default', date_col)) AS result_type
      FROM t
      """
      Then query result
      | result  | result_type |
      | default | string      |

    Scenario: Coalesce non-null string column before a date column wins without temporal casting
      Given config spark.sql.ansi.enabled = false
      When query
      """
      WITH t(string_col, date_col) AS (
        SELECT CAST('hello' AS STRING), DATE '2024-01-15'
      )
      SELECT
        coalesce(string_col, date_col) AS result,
        typeof(coalesce(string_col, date_col)) AS result_type
      FROM t
      """
      Then query result
      | result | result_type |
      | hello  | string      |

    Scenario: Coalesce date column before a string column returns a string value
      Given config spark.sql.ansi.enabled = false
      When query
      """
      WITH t(date_col, string_col) AS (
        SELECT DATE '2024-01-15', CAST('fallback' AS STRING)
      )
      SELECT
        coalesce(date_col, string_col) AS result,
        typeof(coalesce(date_col, string_col)) AS result_type
      FROM t
      """
      Then query result
      | result     | result_type |
      | 2024-01-15 | string      |

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
