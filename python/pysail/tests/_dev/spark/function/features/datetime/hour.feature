Feature: hour output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to hour yields the schema Spark declares
      When query
        """
        SELECT hour('2018-02-14 12:58:59') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to hour yields the schema Spark declares
      When query
        """
        SELECT hour(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to hour stays nullable
      When query
        """
        SELECT hour(c) AS result FROM VALUES ('2018-02-14 12:58:59'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Session time zones

    Scenario Outline: date parts of timestamps in the session time zone <time_zone>
      Given config spark.sql.session.timeZone = <time_zone>
      When query
        """
        SELECT
          s,
          hour(ts) AS h,
          minute(ts) AS m,
          second(ts) AS sec,
          dayofyear(ts) AS doy,
          extract(HOUR FROM ts) AS eh
        FROM (
          SELECT s, CAST(s AS TIMESTAMP) AS ts
          FROM VALUES ('0001-01-01 00:00:00'), ('1969-12-31 23:59:59.999999'), ('2020-03-08 10:30:15') AS t(s)
        )
        ORDER BY s
        """
      Then query result ordered
        | s                          | h  | m  | sec | doy | eh |
        | 0001-01-01 00:00:00        | 0  | 0  | 0   | 1   | 0  |
        | 1969-12-31 23:59:59.999999 | 23 | 59 | 59  | 365 | 23 |
        | 2020-03-08 10:30:15        | 10 | 30 | 15  | 68  | 10 |

      Examples:
        | time_zone           |
        | UTC                 |
        | +05:30              |
        | -08:00              |
        | America/Los_Angeles |
