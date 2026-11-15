Feature: TIMESTAMP_NTZ compares with Parquet TIMESTAMP columns in the session time zone
  Spark casts a TIMESTAMP_NTZ value compared with a TIMESTAMP value to TIMESTAMP in the session time zone, also when
  the TIMESTAMP column is read from a Parquet file whose values are adjusted to UTC. CASE, COALESCE and UNION widen
  the same values the same way.

  Background:
    Given variable location for temporary directory timestamp_ntz_parquet_comparison
    Given final statement
      """
      DROP TABLE IF EXISTS timestamp_ntz_parquet_comparison
      """
    Given config spark.sql.session.timeZone = UTC
    Given statement template
      """
      CREATE TABLE timestamp_ntz_parquet_comparison (id INT, ntz TIMESTAMP_NTZ, ltz TIMESTAMP) USING parquet LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO timestamp_ntz_parquet_comparison VALUES
        (0, TIMESTAMP_NTZ '2020-06-14 12:00:00', TIMESTAMP '2020-06-14 19:00:00'),
        (1, TIMESTAMP_NTZ '2020-03-08 02:30:00', TIMESTAMP '2020-03-08 10:30:00'),
        (2, TIMESTAMP_NTZ '2020-11-01 01:30:00', TIMESTAMP '2020-11-01 08:30:00'),
        (3, TIMESTAMP_NTZ '2020-11-01 01:30:00', TIMESTAMP '2020-11-01 09:30:00'),
        (4, TIMESTAMP_NTZ '2020-06-14 19:00:00', TIMESTAMP '2020-06-14 19:00:00'),
        (5, TIMESTAMP_NTZ '2020-06-15 03:00:00', TIMESTAMP '2020-06-14 19:00:00'),
        (6, TIMESTAMP_NTZ '2020-06-15 00:30:00', TIMESTAMP '2020-06-14 19:00:00'),
        (7, NULL, TIMESTAMP '2020-06-14 19:00:00')
      """

  Scenario: TIMESTAMP_NTZ compares with Parquet TIMESTAMP values in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query template
      """
      SELECT
        id,
        ntz = ltz AS equals,
        ntz < ltz AS less_than,
        ltz <= ntz AS reversed,
        ntz <=> ltz AS null_safe,
        ntz IN (ltz, ltz + INTERVAL 1 HOUR) AS in_list,
        ntz BETWEEN ltz - INTERVAL 1 MINUTE AND ltz AS between_columns
      FROM parquet.`{{ location.string }}`
      ORDER BY id
      """
    Then query result ordered
      | id | equals | less_than | reversed | null_safe | in_list | between_columns |
      | 0  | true   | false     | true     | true      | true    | true            |
      | 1  | true   | false     | true     | true      | true    | true            |
      | 2  | true   | false     | true     | true      | true    | true            |
      | 3  | false  | true      | false    | false     | false   | false           |
      | 4  | false  | false     | true     | false     | false   | false           |
      | 5  | false  | false     | true     | false     | false   | false           |
      | 6  | false  | false     | true     | false     | false   | false           |
      | 7  | NULL   | NULL      | NULL     | false     | NULL    | NULL            |

  Scenario: TIMESTAMP_NTZ compares with Parquet TIMESTAMP values in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query template
      """
      SELECT
        id,
        ntz = ltz AS equals,
        ntz < ltz AS less_than,
        ltz <= ntz AS reversed,
        ntz <=> ltz AS null_safe,
        ntz IN (ltz, ltz + INTERVAL 1 HOUR) AS in_list,
        ntz BETWEEN ltz - INTERVAL 1 MINUTE AND ltz AS between_columns
      FROM parquet.`{{ location.string }}`
      ORDER BY id
      """
    Then query result ordered
      | id | equals | less_than | reversed | null_safe | in_list | between_columns |
      | 0  | false  | true      | false    | false     | false   | false           |
      | 1  | false  | true      | false    | false     | false   | false           |
      | 2  | false  | true      | false    | false     | false   | false           |
      | 3  | false  | true      | false    | false     | false   | false           |
      | 4  | false  | true      | false    | false     | false   | false           |
      | 5  | true   | false     | true     | true      | true    | true            |
      | 6  | false  | true      | false    | false     | false   | false           |
      | 7  | NULL   | NULL      | NULL     | false     | NULL    | NULL            |

  Scenario: TIMESTAMP_NTZ compares with Parquet TIMESTAMP values in the session time zone (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query template
      """
      SELECT
        id,
        ntz = ltz AS equals,
        ntz < ltz AS less_than,
        ltz <= ntz AS reversed,
        ntz <=> ltz AS null_safe,
        ntz IN (ltz, ltz + INTERVAL 1 HOUR) AS in_list,
        ntz BETWEEN ltz - INTERVAL 1 MINUTE AND ltz AS between_columns
      FROM parquet.`{{ location.string }}`
      ORDER BY id
      """
    Then query result ordered
      | id | equals | less_than | reversed | null_safe | in_list | between_columns |
      | 0  | false  | true      | false    | false     | false   | false           |
      | 1  | false  | true      | false    | false     | false   | false           |
      | 2  | false  | true      | false    | false     | false   | false           |
      | 3  | false  | true      | false    | false     | false   | false           |
      | 4  | false  | true      | false    | false     | false   | false           |
      | 5  | false  | false     | true     | false     | false   | false           |
      | 6  | true   | false     | true     | true      | true    | true            |
      | 7  | NULL   | NULL      | NULL     | false     | NULL    | NULL            |

  Scenario: TIMESTAMP_NTZ compares with Parquet TIMESTAMP values in the session time zone (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query template
      """
      SELECT
        id,
        ntz = ltz AS equals,
        ntz < ltz AS less_than,
        ltz <= ntz AS reversed,
        ntz <=> ltz AS null_safe,
        ntz IN (ltz, ltz + INTERVAL 1 HOUR) AS in_list,
        ntz BETWEEN ltz - INTERVAL 1 MINUTE AND ltz AS between_columns
      FROM parquet.`{{ location.string }}`
      ORDER BY id
      """
    Then query result ordered
      | id | equals | less_than | reversed | null_safe | in_list | between_columns |
      | 0  | false  | true      | false    | false     | false   | false           |
      | 1  | false  | true      | false    | false     | false   | false           |
      | 2  | false  | true      | false    | false     | false   | false           |
      | 3  | false  | true      | false    | false     | false   | false           |
      | 4  | true   | false     | true     | true      | true    | true            |
      | 5  | false  | false     | true     | false     | false   | false           |
      | 6  | false  | false     | true     | false     | false   | false           |
      | 7  | NULL   | NULL      | NULL     | false     | NULL    | NULL            |

  Scenario: Joins compare TIMESTAMP_NTZ with Parquet TIMESTAMP values in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query template
      """
      SELECT a.id AS ntz_id, b.id AS timestamp_id
      FROM parquet.`{{ location.string }}` a JOIN parquet.`{{ location.string }}` b ON a.ntz = b.ltz
      ORDER BY a.id, b.id
      """
    Then query result ordered
      | ntz_id | timestamp_id |
      | 0      | 0            |
      | 0      | 4            |
      | 0      | 5            |
      | 0      | 6            |
      | 0      | 7            |
      | 1      | 1            |
      | 2      | 2            |
      | 3      | 2            |

  Scenario: Joins compare TIMESTAMP_NTZ with Parquet TIMESTAMP values in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query template
      """
      SELECT a.id AS ntz_id, b.id AS timestamp_id
      FROM parquet.`{{ location.string }}` a JOIN parquet.`{{ location.string }}` b ON a.ntz = b.ltz
      ORDER BY a.id, b.id
      """
    Then query result ordered
      | ntz_id | timestamp_id |
      | 5      | 0            |
      | 5      | 4            |
      | 5      | 5            |
      | 5      | 6            |
      | 5      | 7            |

  Scenario: CASE and COALESCE widen TIMESTAMP_NTZ like comparisons (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query template
      """
      SELECT
        id,
        ntz = ltz AS equals,
        CASE WHEN id >= 0 THEN ntz ELSE ltz END = ltz AS case_equals,
        coalesce(ntz, ltz) = ltz AS coalesce_equals,
        unix_seconds(CASE WHEN id >= 0 THEN ntz ELSE ltz END) - unix_seconds(ltz) AS case_offset,
        unix_seconds(coalesce(ntz, ltz)) - unix_seconds(ltz) AS coalesce_offset
      FROM parquet.`{{ location.string }}`
      ORDER BY id
      """
    Then query result ordered
      | id | equals | case_equals | coalesce_equals | case_offset | coalesce_offset |
      | 0  | true   | true        | true            | 0           | 0               |
      | 1  | true   | true        | true            | 0           | 0               |
      | 2  | true   | true        | true            | 0           | 0               |
      | 3  | false  | false       | false           | -3600       | -3600           |
      | 4  | false  | false       | false           | 25200       | 25200           |
      | 5  | false  | false       | false           | 54000       | 54000           |
      | 6  | false  | false       | false           | 45000       | 45000           |
      | 7  | NULL   | NULL        | true            | NULL        | 0               |

  Scenario: CASE and COALESCE widen TIMESTAMP_NTZ like comparisons (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query template
      """
      SELECT
        id,
        ntz = ltz AS equals,
        CASE WHEN id >= 0 THEN ntz ELSE ltz END = ltz AS case_equals,
        coalesce(ntz, ltz) = ltz AS coalesce_equals,
        unix_seconds(CASE WHEN id >= 0 THEN ntz ELSE ltz END) - unix_seconds(ltz) AS case_offset,
        unix_seconds(coalesce(ntz, ltz)) - unix_seconds(ltz) AS coalesce_offset
      FROM parquet.`{{ location.string }}`
      ORDER BY id
      """
    Then query result ordered
      | id | equals | case_equals | coalesce_equals | case_offset | coalesce_offset |
      | 0  | false  | false       | false           | -45000      | -45000          |
      | 1  | false  | false       | false           | -48600      | -48600          |
      | 2  | false  | false       | false           | -45000      | -45000          |
      | 3  | false  | false       | false           | -48600      | -48600          |
      | 4  | false  | false       | false           | -19800      | -19800          |
      | 5  | false  | false       | false           | 9000        | 9000            |
      | 6  | true   | true        | true            | 0           | 0               |
      | 7  | NULL   | NULL        | true            | NULL        | 0               |

  Scenario: UNION widens TIMESTAMP_NTZ like comparisons (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query template
      """
      SELECT u.id, u.v = t.ltz AS union_equals, unix_seconds(u.v) - unix_seconds(t.ltz) AS union_offset
      FROM (SELECT id, ntz AS v FROM parquet.`{{ location.string }}` UNION ALL SELECT id, ltz AS v FROM parquet.`{{ location.string }}` WHERE id < 0) u
      JOIN parquet.`{{ location.string }}` t ON u.id = t.id
      ORDER BY u.id
      """
    Then query result ordered
      | id | union_equals | union_offset |
      | 0  | true         | 0            |
      | 1  | true         | 0            |
      | 2  | true         | 0            |
      | 3  | false        | -3600        |
      | 4  | false        | 25200        |
      | 5  | false        | 54000        |
      | 6  | false        | 45000        |
      | 7  | NULL         | NULL         |

  Scenario: UNION widens TIMESTAMP_NTZ like comparisons (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query template
      """
      SELECT u.id, u.v = t.ltz AS union_equals, unix_seconds(u.v) - unix_seconds(t.ltz) AS union_offset
      FROM (SELECT id, ntz AS v FROM parquet.`{{ location.string }}` UNION ALL SELECT id, ltz AS v FROM parquet.`{{ location.string }}` WHERE id < 0) u
      JOIN parquet.`{{ location.string }}` t ON u.id = t.id
      ORDER BY u.id
      """
    Then query result ordered
      | id | union_equals | union_offset |
      | 0  | false        | -54000       |
      | 1  | false        | -57600       |
      | 2  | false        | -54000       |
      | 3  | false        | -57600       |
      | 4  | false        | -28800       |
      | 5  | true         | 0            |
      | 6  | false        | -9000        |
      | 7  | NULL         | NULL         |

  Scenario: Parquet TIMESTAMP and TIMESTAMP_NTZ subtraction uses the session wall clock (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query template
      """
      SELECT id, ltz - ntz AS timestamp_minus_ntz, ntz - ltz AS ntz_minus_timestamp
      FROM parquet.`{{ location.string }}`
      ORDER BY id
      """
    Then query result ordered
      | id | timestamp_minus_ntz                  | ntz_minus_timestamp                  |
      | 0  | INTERVAL '0 00:00:00' DAY TO SECOND  | INTERVAL '0 00:00:00' DAY TO SECOND  |
      | 1  | INTERVAL '0 01:00:00' DAY TO SECOND  | INTERVAL '-0 01:00:00' DAY TO SECOND |
      | 2  | INTERVAL '0 00:00:00' DAY TO SECOND  | INTERVAL '0 00:00:00' DAY TO SECOND  |
      | 3  | INTERVAL '0 00:00:00' DAY TO SECOND  | INTERVAL '0 00:00:00' DAY TO SECOND  |
      | 4  | INTERVAL '-0 07:00:00' DAY TO SECOND | INTERVAL '0 07:00:00' DAY TO SECOND  |
      | 5  | INTERVAL '-0 15:00:00' DAY TO SECOND | INTERVAL '0 15:00:00' DAY TO SECOND  |
      | 6  | INTERVAL '-0 12:30:00' DAY TO SECOND | INTERVAL '0 12:30:00' DAY TO SECOND  |
      | 7  | NULL                                 | NULL                                 |

  Scenario: Parquet TIMESTAMP and TIMESTAMP_NTZ subtraction uses the session wall clock (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query template
      """
      SELECT id, ltz - ntz AS timestamp_minus_ntz, ntz - ltz AS ntz_minus_timestamp
      FROM parquet.`{{ location.string }}`
      ORDER BY id
      """
    Then query result ordered
      | id | timestamp_minus_ntz                 | ntz_minus_timestamp                  |
      | 0  | INTERVAL '0 15:00:00' DAY TO SECOND | INTERVAL '-0 15:00:00' DAY TO SECOND |
      | 1  | INTERVAL '0 16:00:00' DAY TO SECOND | INTERVAL '-0 16:00:00' DAY TO SECOND |
      | 2  | INTERVAL '0 15:00:00' DAY TO SECOND | INTERVAL '-0 15:00:00' DAY TO SECOND |
      | 3  | INTERVAL '0 16:00:00' DAY TO SECOND | INTERVAL '-0 16:00:00' DAY TO SECOND |
      | 4  | INTERVAL '0 08:00:00' DAY TO SECOND | INTERVAL '-0 08:00:00' DAY TO SECOND |
      | 5  | INTERVAL '0 00:00:00' DAY TO SECOND | INTERVAL '0 00:00:00' DAY TO SECOND  |
      | 6  | INTERVAL '0 02:30:00' DAY TO SECOND | INTERVAL '-0 02:30:00' DAY TO SECOND |
      | 7  | NULL                                | NULL                                 |
