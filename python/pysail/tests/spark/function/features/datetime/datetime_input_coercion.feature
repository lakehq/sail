Feature: Datetime input coercion and output schemas

  Scenario Outline: Date functions parse each string using Spark date rules for <function>
    When query
      """
      SELECT <function>(value) AS result
      FROM VALUES (1, '2023'), (2, '2024-1-5'), (3, CAST(NULL AS STRING)) AS t(id, value)
      ORDER BY id
      """
    Then query result ordered
      | result   |
      | <first>  |
      | <second> |
      | NULL     |

    Examples:
      | function   | first      | second     |
      | day        | 1          | 5          |
      | dayofmonth | 1          | 5          |
      | dayofyear  | 1          | 5          |
      | month      | 1          | 1          |
      | quarter    | 1          | 1          |
      | year       | 2023       | 2024       |
      | weekday    | 6          | 4          |
      | dayofweek  | 1          | 6          |
      | last_day   | 2023-01-31 | 2024-01-31 |

  Scenario Outline: Legacy parsing preserves null rows through <function>
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id FROM VALUES (1, '2024-01-05 12:34:56'), (2, 'bad'), (3, ''), (4, CAST(NULL AS STRING)) AS t(id, value)
      WHERE <expression> IS NULL ORDER BY id
      """
    Then query result ordered
      | id |
      | 2  |
      | 3  |
      | 4  |

    Examples:
      | function    | expression                      |
      | day         | day(value)                      |
      | dayofmonth  | dayofmonth(value)               |
      | dayofyear   | dayofyear(value)                |
      | month       | month(value)                    |
      | quarter     | quarter(value)                  |
      | year        | year(value)                     |
      | weekday     | weekday(value)                  |
      | dayofweek   | dayofweek(value)                |
      | last_day    | last_day(value)                 |
      | datediff    | datediff(value, '2024-01-01')    |
      | hour        | hour(value)                     |
      | minute      | minute(value)                   |
      | second      | second(value)                   |
      | date_format | date_format(value, 'yyyy-MM-dd') |

  Scenario Outline: ANSI parsing retains errors in <function>
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT <expression> FROM VALUES ('bad') AS t(value)
      """
    Then query error (?i)malformed|cannot.*cast|invalid.*timestamp

    Examples:
      | function    | expression                      |
      | day         | day(value)                      |
      | year        | year(value)                     |
      | last_day    | last_day(value)                 |
      | datediff    | datediff(value, '2024-01-01')    |
      | hour        | hour(value)                     |
      | date_format | date_format(value, 'yyyy-MM-dd') |

  Scenario: String time fields resolve DST gaps while timestamp_ntz keeps wall time
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT hour(value) AS string_hour, hour(CAST(value AS TIMESTAMP_NTZ)) AS wall_hour,
             minute(value) AS minute, second(value) AS second
      FROM VALUES ('2024-03-10 02:30:12.123456') AS t(value)
      """
    Then query result
      | string_hour | wall_hour | minute | second |
      | 3           | 2         | 30     | 12     |

  Scenario: Date functions retain Spark timestamp_ntz cast nullability
    When query
      """
      SELECT year(t) AS year, day(t) AS day, last_day(t) AS last_day,
             datediff(t, TIMESTAMP_NTZ '2024-01-01 00:00:00') AS difference
      FROM VALUES (TIMESTAMP_NTZ '2024-02-29 12:00:00') AS v(t)
      """
    Then query schema
      """
      root
       |-- year: integer (nullable = true)
       |-- day: integer (nullable = true)
       |-- last_day: date (nullable = true)
       |-- difference: integer (nullable = true)
      """
    Then query result
      | year | day | last_day   | difference |
      | 2024 | 29  | 2024-02-29 | 59         |

  Scenario Outline: Current date aliases use the current instant in <zone>
    Given config spark.sql.session.timeZone = <zone>
    When query
      """
      SELECT current_date() = CAST(current_timestamp() AS DATE) AS today,
             curdate() = current_date() AS same_date,
             now() = current_timestamp() AS same_instant
      """
    Then query schema
      """
      root
       |-- today: boolean (nullable = false)
       |-- same_date: boolean (nullable = false)
       |-- same_instant: boolean (nullable = false)
      """
    Then query result
      | today | same_date | same_instant |
      | true  | true      | true         |

    Examples:
      | zone                |
      | UTC                 |
      | America/Los_Angeles |
      | Asia/Shanghai       |
      | Pacific/Kiritimati  |
