Feature: second output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to second yields the schema Spark declares
      When query
        """
        SELECT second('2018-02-14 12:58:59') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to second yields the schema Spark declares
      When query
        """
        SELECT second(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to second stays nullable
      When query
        """
        SELECT second(c) AS result FROM VALUES ('2018-02-14 12:58:59'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: The second is read in the session time zone

    # Spark 4.2.0 Second -> DateTimeUtils.getSeconds(micros, zoneId). Before 1854 Asia/Kolkata used
    # local mean time +05:53:28, the only kind of offset that moves the second field.

    Scenario: second of a historical instant built with CAST
      Given config spark.sql.session.timeZone = Asia/Kolkata
      When query
        """
        SELECT second(CAST(-5000000000 AS TIMESTAMP)) AS result
        """
      Then query result
        | result |
        | 8      |

    @sail-bug
    Scenario: second of a historical instant built with timestamp_seconds
      Given config spark.sql.session.timeZone = Asia/Kolkata
      When query
        """
        SELECT second(timestamp_seconds(-5000000000)) AS result
        """
      Then query result
        | result |
        | 8      |

  Rule: The second truncates fractional microseconds toward the past

    # getLocalDateTime(micros, zone).getSecond floors: one microsecond before the epoch is second
    # 59, not 0. extract(SECOND ...) keeps the fraction as decimal(8,6).

    Scenario: second and extract second of distinct fractional boundary rows
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT id, second(ts) AS s, extract(SECOND FROM ts) AS es
        FROM VALUES
          (1, TIMESTAMP '1969-12-31 23:59:59.999999'),
          (2, TIMESTAMP '2024-01-01 00:00:59.999999'),
          (3, TIMESTAMP '0001-01-01 00:00:00.000001'),
          (4, TIMESTAMP '9999-12-31 23:59:58.5')
          AS t(id, ts)
        ORDER BY id
        """
      Then query result ordered
        | id | s  | es        |
        | 1  | 59 | 59.999999 |
        | 2  | 59 | 59.999999 |
        | 3  | 0  | 0.000001  |
        | 4  | 58 | 58.500000 |
