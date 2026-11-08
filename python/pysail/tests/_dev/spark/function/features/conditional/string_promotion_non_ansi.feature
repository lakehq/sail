Feature: Values with STRING values are promoted to STRING without ANSI mode

  Without ANSI mode, Spark promotes atomic values other than BOOLEAN and BINARY values to STRING
  when they are mixed with STRING values in `coalesce`, `array`, and `map`, and formats them like
  `CAST(value AS STRING)`.

  Background:
    Given config spark.sql.ansi.enabled = false
    Given config spark.sql.session.timeZone = UTC

  Rule: The promoted value is formatted like a cast to STRING

    Scenario Outline: <case> with STRING values
      When query
        """
        SELECT
          array(s, <value>)[1] AS a,
          map(1, s, 2, <value>)[2] AS m,
          coalesce(n, <value>) AS c,
          typeof(coalesce(n, <value>)) AS c_type
        FROM VALUES ('x', CAST(NULL AS STRING)) AS t(s, n)
        """
      Then query result
        | a        | m        | c        | c_type |
        | <result> | <result> | <result> | string |

      Examples:
        | case               | value                                  | result                                |
        | TIMESTAMP          | TIMESTAMP'2020-01-01 01:02:03'         | 2020-01-01 01:02:03                   |
        | TIMESTAMP_NTZ      | TIMESTAMP_NTZ'2020-01-01 01:02:03'     | 2020-01-01 01:02:03                   |
        | DATE               | DATE'2020-01-01'                       | 2020-01-01                            |
        | TIME               | TIME'01:02:03'                         | 01:02:03                              |
        | YEAR interval      | INTERVAL '1' YEAR                      | INTERVAL '1' YEAR                     |
        | YEAR TO MONTH      | INTERVAL '1-2' YEAR TO MONTH           | INTERVAL '1-2' YEAR TO MONTH          |
        | DAY interval       | INTERVAL '1' DAY                       | INTERVAL '1' DAY                      |
        | DAY TO SECOND      | INTERVAL '1 02:03:04.5' DAY TO SECOND  | INTERVAL '1 02:03:04.5' DAY TO SECOND |
        | large DOUBLE       | 1e20D                                  | 1.0E20                                |
        | small DOUBLE       | 1e-5D                                  | 1.0E-5                                |
        | FLOAT              | CAST(1.5 AS FLOAT)                     | 1.5                                   |
        | DECIMAL            | CAST(1.50 AS DECIMAL(5,2))             | 1.50                                  |
        | BIGINT             | 7L                                     | 7                                     |
