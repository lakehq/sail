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
