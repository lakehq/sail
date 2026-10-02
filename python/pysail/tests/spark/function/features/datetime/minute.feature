Feature: minute output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to minute yields the schema Spark declares
      When query
        """
        SELECT minute('2009-07-30 12:58:59') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to minute yields the schema Spark declares
      When query
        """
        SELECT minute(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to minute stays nullable
      When query
        """
        SELECT minute(c) AS result FROM VALUES ('2009-07-30 12:58:59'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: The minute is read in the session time zone

    # Spark 4.2.0 Minute -> DateTimeUtils.getMinutes(micros, zoneId): Asia/Kathmandu was +05:30
    # until 1986 and +05:45 afterwards, so only the minute field tells the zone rules apart.

    Scenario: minute of epoch instants built with CAST in a quarter-hour zone
      Given config spark.sql.session.timeZone = Asia/Kathmandu
      When query
        """
        SELECT id, minute(CAST(e AS TIMESTAMP)) AS result
        FROM VALUES (1, 0), (2, 504901800) AS t(id, e)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | 30     |
        | 2  | 15     |

    @sail-bug
    Scenario: minute of epoch instants built with timestamp_seconds in a quarter-hour zone
      Given config spark.sql.session.timeZone = Asia/Kathmandu
      When query
        """
        SELECT id, minute(timestamp_seconds(e)) AS result
        FROM VALUES (1, 0), (2, 504901800) AS t(id, e)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | 30     |
        | 2  | 15     |
