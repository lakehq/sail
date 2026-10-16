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

  Rule: The hour is read in the session time zone

    # Spark 4.2.0 Hour.nullSafeEval -> DateTimeUtils.getHours(micros, zoneId), where zoneId is the
    # session time zone: the same instant reads a different hour in each zone, whatever produced it.

    Scenario: hour of distinct instants around both DST transitions built with CAST
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT id, hour(CAST(e AS TIMESTAMP)) AS result
        FROM VALUES (1, 1730622600), (2, 1730626200), (3, 1710066600) AS t(id, e)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | 1      |
        | 2  | 1      |
        | 3  | 3      |

    @sail-bug
    Scenario: hour of distinct instants around both DST transitions built with timestamp_seconds
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT id, hour(timestamp_seconds(e)) AS result
        FROM VALUES (1, 1730622600), (2, 1730626200), (3, 1710066600) AS t(id, e)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | 1      |
        | 2  | 1      |
        | 3  | 3      |

    Scenario: hour of a local time inside the spring-forward gap
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          hour(TIMESTAMP '2024-03-10 02:30:00') AS ltz,
          hour(TIMESTAMP_NTZ '2024-03-10 02:30:00') AS ntz
        """
      Then query result
        | ltz | ntz |
        | 3   | 2   |

    @sail-bug
    Scenario: hour of a date whose local midnight does not exist
      Given config spark.sql.session.timeZone = America/Sao_Paulo
      When query
        """
        SELECT id, hour(CAST(d AS TIMESTAMP)) AS result
        FROM VALUES (1, DATE '2018-11-03'), (2, DATE '2018-11-04') AS t(id, d)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | 0      |
        | 2  | 1      |
