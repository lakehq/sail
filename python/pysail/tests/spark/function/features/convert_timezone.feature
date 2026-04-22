Feature: convert_timezone respects timezone offsets for TIMESTAMP_NTZ inputs

  Scenario: convert_timezone adjusts winter Europe/Amsterdam to UTC
    Given config spark.sql.session.timeZone = Europe/Amsterdam
    When query
    """
    SELECT date_format(
      convert_timezone('Europe/Amsterdam', 'UTC', to_timestamp_ntz(ts)),
      'yyyy-MM-dd HH'
    ) AS result
    FROM VALUES (TIMESTAMP '2023-01-01 10:00:00') AS t(ts)
    """
    Then query result
    | result        |
    | 2023-01-01 09 |
