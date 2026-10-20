Feature: extract output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to extract yields the schema Spark declares
      When query
        """
        SELECT extract(YEAR FROM TIMESTAMP '2019-08-12 01:00:00.123456') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

  Rule: Spark fields and components

    Scenario: extract uses Spark weekday and ISO week-year numbering
      When query
        """
        SELECT extract(DOW FROM DATE '2024-01-07') AS weekday,
               extract(DAYOFWEEK_ISO FROM DATE '2024-01-07') AS iso_weekday,
               extract(YEAROFWEEK FROM DATE '2021-01-01') AS iso_year
        """
      Then query result
        | weekday | iso_weekday | iso_year |
        | 1       | 7           | 2020     |

    Scenario: extract returns interval components and calendar fractions
      When query
        """
        SELECT extract(HOUR FROM INTERVAL '2 03:04:05.123456' DAY TO SECOND) AS hours,
               extract(MINUTE FROM INTERVAL '2 03:04:05.123456' DAY TO SECOND) AS minutes,
               extract(SECOND FROM make_interval(0, 0, 0, 0, 0, 0, 5.123456)) AS seconds
        """
      Then query result
        | hours | minutes | seconds  |
        | 3     | 4       | 5.123456 |

    Scenario: extract uses the session timezone when parsing timestamp strings
      Given config spark.sql.session.timeZone = Asia/Shanghai
      When query
        """
        SELECT extract(HOUR FROM '2024-01-15 12:30:00Z') AS result
        """
      Then query result
        | result |
        | 20     |

  Rule: Year comparisons retain rows across a missing January midnight

    Scenario Outline: extract year equality retains <year> rows in <zone>
      Given config spark.sql.session.timeZone = <zone>
      And config spark.sql.timestampType = TIMESTAMP_LTZ
      When query
        """
        SELECT id
        FROM range(2)
        WHERE extract(YEAR FROM CAST(concat('<year>-06-15 12:00:0', id) AS TIMESTAMP)) = <year>
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 0  |
        | 1  |

      Examples:
        | zone           | year |
        | Asia/Kathmandu | 1985 |
        | Asia/Kathmandu | 1986 |
        | Asia/Kathmandu | 1987 |
        | UTC            | 1986 |

    Scenario: extract year comparison projections preserve all operators and NULLs
      Given config spark.sql.session.timeZone = Asia/Kathmandu
      And config spark.sql.timestampType = TIMESTAMP_LTZ
      When query
        """
        SELECT id, extract(YEAR FROM ts) AS extracted, year(ts) AS direct_year,
               extract(YEAR FROM ts) = 1986 AS eq,
               extract(YEAR FROM ts) <> 1986 AS ne,
               extract(YEAR FROM ts) < 1986 AS lt,
               extract(YEAR FROM ts) <= 1986 AS le,
               extract(YEAR FROM ts) > 1986 AS gt,
               extract(YEAR FROM ts) >= 1986 AS ge
        FROM (
          SELECT id, CAST(CASE WHEN id = 0 THEN '1985-12-31 23:59:59'
                              WHEN id = 1 THEN '1986-01-01 00:15:00'
                              WHEN id = 2 THEN '1987-01-01 00:00:00' END AS TIMESTAMP) AS ts
          FROM range(4)
        )
        ORDER BY id
        """
      Then query result ordered
        | id | extracted | direct_year | eq    | ne    | lt    | le    | gt    | ge    |
        | 0  | 1985      | 1985        | false | true  | true  | true  | false | false |
        | 1  | 1986      | 1986        | true  | false | false | true  | false | true  |
        | 2  | 1987      | 1987        | false | true  | false | false | true  | true  |
        | 3  | NULL      | NULL        | NULL  | NULL  | NULL  | NULL  | NULL  | NULL  |
