Feature: Dates convert to the first valid time of the local day

  Scenario Outline: Date casts and timestamp functions use the canonical start in <zone>
    Given config spark.sql.session.timeZone = <zone>
    When query
      """
      SELECT id,
             CAST(CAST(d AS TIMESTAMP_LTZ) AS STRING) AS strict_cast,
             CAST(TRY_CAST(d AS TIMESTAMP_LTZ) AS STRING) AS safe_cast,
             CAST(to_timestamp_ltz(d) AS STRING) AS timestamp_function,
             CAST(to_timestamp_ltz(d, 'yyyy') AS STRING) AS formatted_function
      FROM VALUES (0, DATE '<date>'), (1, CAST(NULL AS DATE)) AS t(id, d)
      ORDER BY id
      """
    Then query result ordered
      | id | strict_cast | safe_cast | timestamp_function | formatted_function |
      | 0  | <expected>  | <expected> | <expected>        | <expected>         |
      | 1  | NULL        | NULL       | NULL              | NULL               |

    Examples:
      | zone              | date       | expected            |
      | America/Toronto   | 1919-03-31 | 1919-03-31 00:30:00 |
      | America/Sao_Paulo | 2018-11-04 | 2018-11-04 01:00:00 |
      | America/Havana    | 2015-11-01 | 2015-11-01 00:00:00 |
      | Pacific/Apia      | 2011-12-30 | 2011-12-31 00:00:00 |

  Scenario: A date and a naive midnight resolve differently inside a midnight gap
    Given config spark.sql.session.timeZone = America/Toronto
    When query
      """
      SELECT CAST(CAST(DATE '1919-03-31' AS TIMESTAMP_LTZ) AS STRING) AS day_start,
             CAST(CAST(TIMESTAMP_NTZ '1919-03-31 00:00:00' AS TIMESTAMP_LTZ) AS STRING) AS shifted_midnight,
             DATE '1919-03-31' > TIMESTAMP '1919-03-31 00:30:00' AS date_after_start,
             DATE '1919-03-31' <= TIMESTAMP '1919-03-31 00:30:00' AS date_at_or_before_start,
             TIMESTAMP '1919-03-31 00:30:00' < DATE '1919-03-31' AS start_before_date
      """
    Then query result
      | day_start           | shifted_midnight     | date_after_start | date_at_or_before_start | start_before_date |
      | 1919-03-31 00:30:00 | 1919-03-31 01:00:00 | false            | true                    | false             |

  Scenario: Date conversion chooses the first occurrence of a repeated midnight
    Given config spark.sql.session.timeZone = America/Havana
    When query
      """
      SELECT unix_micros(CAST(d AS TIMESTAMP_LTZ)) AS day_start,
             unix_micros(TIMESTAMP '2015-11-01 04:00:00Z') AS first_midnight,
             unix_micros(TIMESTAMP '2015-11-01 05:00:00Z') AS second_midnight
      FROM VALUES (DATE '2015-11-01') AS t(d)
      """
    Then query result
      | day_start       | first_midnight  | second_midnight |
      | 1446350400000000 | 1446350400000000 | 1446354000000000 |

  Scenario Outline: Date casts preserve the supported wide timestamp range in <zone>
    Given config spark.sql.session.timeZone = <zone>
    When query
      """
      SELECT unix_micros(CAST(date_from_unix_date(100000000) AS TIMESTAMP_LTZ)) AS value
      """
    Then query result
      | value      |
      | <expected> |

    Examples:
      | zone   | expected            |
      | UTC    | 8640000000000000000 |
      | +05:30 | 8639999980200000000 |
