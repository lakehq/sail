Feature: TIMESTAMP converts to TIMESTAMP_NTZ in the session time zone
  Spark casts TIMESTAMP to TIMESTAMP_NTZ, formats TIMESTAMP, and counts calendar units in the session time zone.
  The Parquet table is written in UTC, so its TIMESTAMP column is labeled with UTC rather than the session time zone.

  Background:
    Given variable location for temporary directory timestamp_to_timestamp_ntz
    Given final statement
      """
      DROP TABLE IF EXISTS timestamp_to_timestamp_ntz
      """
    Given config spark.sql.session.timeZone = UTC
    Given statement template
      """
      CREATE TABLE timestamp_to_timestamp_ntz (id INT, ntz TIMESTAMP_NTZ, ltz TIMESTAMP) USING parquet LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO timestamp_to_timestamp_ntz VALUES
        (0, TIMESTAMP_NTZ '2020-03-08 02:30:00', NULL),
        (1, NULL, TIMESTAMP '2020-06-14 04:00:00'),
        (2, TIMESTAMP_NTZ '2020-11-01 01:30:00', TIMESTAMP '2020-11-01 08:30:00')
      """

  Scenario: TIMESTAMP casts to TIMESTAMP_NTZ in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        CAST(CAST(TIMESTAMP '2020-06-14 02:00:00' AS TIMESTAMP_NTZ) AS STRING) AS literal,
        CAST(CAST(ltz AS TIMESTAMP_NTZ) AS STRING) AS column,
        date_format(TIMESTAMP '2020-06-14 02:00:00', 'yyyy-MM-dd HH:mm') AS formatted
      FROM timestamp_to_timestamp_ntz
      WHERE id = 1
      """
    Then query result
      | literal             | column              | formatted        |
      | 2020-06-14 02:00:00 | 2020-06-14 12:00:00 | 2020-06-14 02:00 |

  Scenario: TIMESTAMP casts to TIMESTAMP_NTZ in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        CAST(CAST(TIMESTAMP '2020-06-14 02:00:00' AS TIMESTAMP_NTZ) AS STRING) AS literal,
        CAST(CAST(ltz AS TIMESTAMP_NTZ) AS STRING) AS column,
        date_format(TIMESTAMP '2020-06-14 02:00:00', 'yyyy-MM-dd HH:mm') AS formatted
      FROM timestamp_to_timestamp_ntz
      WHERE id = 1
      """
    Then query result
      | literal             | column              | formatted        |
      | 2020-06-14 02:00:00 | 2020-06-13 21:00:00 | 2020-06-14 02:00 |

  Scenario: TIMESTAMP casts to TIMESTAMP_NTZ in the session time zone (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT
        CAST(CAST(TIMESTAMP '2020-06-14 02:00:00' AS TIMESTAMP_NTZ) AS STRING) AS literal,
        CAST(CAST(ltz AS TIMESTAMP_NTZ) AS STRING) AS column,
        date_format(TIMESTAMP '2020-06-14 02:00:00', 'yyyy-MM-dd HH:mm') AS formatted
      FROM timestamp_to_timestamp_ntz
      WHERE id = 1
      """
    Then query result
      | literal             | column              | formatted        |
      | 2020-06-14 02:00:00 | 2020-06-14 09:30:00 | 2020-06-14 02:00 |

  Scenario: TIMESTAMP casts to TIMESTAMP_NTZ in the session time zone (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT
        CAST(CAST(TIMESTAMP '2020-06-14 02:00:00' AS TIMESTAMP_NTZ) AS STRING) AS literal,
        CAST(CAST(ltz AS TIMESTAMP_NTZ) AS STRING) AS column,
        date_format(TIMESTAMP '2020-06-14 02:00:00', 'yyyy-MM-dd HH:mm') AS formatted
      FROM timestamp_to_timestamp_ntz
      WHERE id = 1
      """
    Then query result
      | literal             | column              | formatted        |
      | 2020-06-14 02:00:00 | 2020-06-14 04:00:00 | 2020-06-14 02:00 |

  Scenario: TIMESTAMPDIFF counts calendar units like LocalDateTime.until (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT
        timestampdiff(MONTH, TIMESTAMP '2020-01-31 20:00:00', TIMESTAMP '2020-03-01 05:00:00') AS month_end,
        timestampdiff(MONTH, TIMESTAMP '2020-03-31 00:00:00', TIMESTAMP '2020-02-29 18:00:00') AS backward,
        timestampdiff(QUARTER, TIMESTAMP '2019-12-31 23:59:59.999999', TIMESTAMP '2020-03-31 00:00:00') AS quarter,
        timestampdiff(YEAR, TIMESTAMP '2020-02-29 12:00:00', TIMESTAMP '2021-02-28 12:00:00') AS year
      """
    Then query result
      | month_end | backward | quarter | year |
      | 0         | 0        | 0       | 0    |

  Scenario: TIMESTAMPDIFF counts calendar units in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        timestampdiff(MONTH, TIMESTAMP '2020-06-30 20:00:00', TIMESTAMP '2020-07-30 21:00:00') AS months,
        timestampdiff(MONTH, ltz, TIMESTAMP '2020-07-13 20:00:00') AS column_months
      FROM timestamp_to_timestamp_ntz
      WHERE id = 1
      """
    Then query result
      | months | column_months |
      | 1      | 0             |

  Scenario: TIMESTAMPDIFF counts calendar units in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        timestampdiff(MONTH, TIMESTAMP '2020-06-30 20:00:00', TIMESTAMP '2020-07-30 21:00:00') AS months,
        timestampdiff(MONTH, ltz, TIMESTAMP '2020-07-13 20:00:00') AS column_months
      FROM timestamp_to_timestamp_ntz
      WHERE id = 1
      """
    Then query result
      | months | column_months |
      | 1      | 0             |

  Scenario: TIMESTAMPDIFF counts calendar units in the session time zone (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT
        timestampdiff(MONTH, TIMESTAMP '2020-06-30 20:00:00', TIMESTAMP '2020-07-30 21:00:00') AS months,
        timestampdiff(MONTH, ltz, TIMESTAMP '2020-07-13 20:00:00') AS column_months
      FROM timestamp_to_timestamp_ntz
      WHERE id = 1
      """
    Then query result
      | months | column_months |
      | 1      | 0             |

  Scenario: TIMESTAMPDIFF counts calendar units in the session time zone (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT
        timestampdiff(MONTH, TIMESTAMP '2020-06-30 20:00:00', TIMESTAMP '2020-07-30 21:00:00') AS months,
        timestampdiff(MONTH, ltz, TIMESTAMP '2020-07-13 20:00:00') AS column_months
      FROM timestamp_to_timestamp_ntz
      WHERE id = 1
      """
    Then query result
      | months | column_months |
      | 1      | 0             |
