Feature: TIMESTAMP_NTZ compares with TIMESTAMP constants in the session time zone
  Spark compares a TIMESTAMP_NTZ value with a TIMESTAMP constant by converting the constant to its wall-clock time in
  the session time zone when the conversion round-trips (UnwrapCastInBinaryComparison), so filters keep pruning.

  Background:
    Given variable location for temporary directory timestamp_ntz_constant_comparison
    Given final statement
      """
      DROP TABLE IF EXISTS timestamp_ntz_constant_comparison
      """
    Given config spark.sql.session.timeZone = UTC
    Given statement template
      """
      CREATE TABLE timestamp_ntz_constant_comparison (id INT, ntz TIMESTAMP_NTZ) USING parquet LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO timestamp_ntz_constant_comparison VALUES (0, TIMESTAMP_NTZ '2020-03-08 01:30:00')
      """
    Given statement
      """
      INSERT INTO timestamp_ntz_constant_comparison VALUES (1, TIMESTAMP_NTZ '2020-03-08 02:30:00')
      """
    Given statement
      """
      INSERT INTO timestamp_ntz_constant_comparison VALUES (2, TIMESTAMP_NTZ '2020-03-08 03:30:00')
      """
    Given statement
      """
      INSERT INTO timestamp_ntz_constant_comparison VALUES (3, TIMESTAMP_NTZ '2020-11-01 01:30:00')
      """
    Given statement
      """
      INSERT INTO timestamp_ntz_constant_comparison VALUES (4, TIMESTAMP_NTZ '2020-11-01 02:30:00')
      """
    Given statement
      """
      INSERT INTO timestamp_ntz_constant_comparison VALUES (5, TIMESTAMP_NTZ '2020-06-14 12:00:00')
      """
    Given statement
      """
      INSERT INTO timestamp_ntz_constant_comparison VALUES (6, TIMESTAMP_NTZ '2020-06-14 19:00:00')
      """

  Scenario: TIMESTAMP_NTZ compares with TIMESTAMP constants in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        ntz = TIMESTAMP '2020-06-14 12:00:00' AS equals,
        ntz < TIMESTAMP '2020-06-14 12:00:00' AS less_than,
        TIMESTAMP '2020-06-14 12:00:00' <= ntz AS literal_left,
        ntz IN (TIMESTAMP '2020-06-14 12:00:00', TIMESTAMP '2020-11-01 01:30:00') AS in_list,
        ntz BETWEEN TIMESTAMP '2020-03-08 01:00:00' AND TIMESTAMP '2020-03-08 03:00:00' AS between_constants,
        ntz = TIMESTAMP '2020-03-08 02:30:00' AS gap_equals,
        ntz < TIMESTAMP '2020-03-08 03:00:00' AS gap_less_than,
        ntz = TIMESTAMP '2020-11-01 01:30:00' AS overlap_equals,
        ntz = TIMESTAMP '2020-11-01 01:30:00-08:00' AS later_offset_equals,
        ntz < TIMESTAMP '2020-11-01 01:30:00-08:00' AS later_offset_less_than,
        CAST(ntz AS TIMESTAMP) = TIMESTAMP '2020-11-01 01:30:00' AS explicit_cast
      FROM timestamp_ntz_constant_comparison
      ORDER BY id
      """
    Then query result ordered
      | id | equals | less_than | literal_left | in_list | between_constants | gap_equals | gap_less_than | overlap_equals | later_offset_equals | later_offset_less_than | explicit_cast |
      | 0  | false  | true      | false        | false   | true              | false      | true          | false          | false               | true                   | false         |
      | 1  | false  | true      | false        | false   | true              | false      | true          | false          | false               | true                   | false         |
      | 2  | false  | true      | false        | false   | false             | true       | false         | false          | false               | true                   | false         |
      | 3  | false  | false     | true         | true    | false             | false      | false         | true           | false               | true                   | true          |
      | 4  | false  | false     | true         | false   | false             | false      | false         | false          | false               | false                  | false         |
      | 5  | true   | false     | true         | true    | false             | false      | false         | false          | false               | true                   | false         |
      | 6  | false  | false     | true         | false   | false             | false      | false         | false          | false               | true                   | false         |

  Scenario: TIMESTAMP_NTZ compares with TIMESTAMP constants in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        id,
        ntz = TIMESTAMP '2020-06-14 12:00:00' AS equals,
        ntz < TIMESTAMP '2020-06-14 12:00:00' AS less_than,
        TIMESTAMP '2020-06-14 12:00:00' <= ntz AS literal_left,
        ntz IN (TIMESTAMP '2020-06-14 12:00:00', TIMESTAMP '2020-11-01 01:30:00') AS in_list,
        ntz BETWEEN TIMESTAMP '2020-03-08 01:00:00' AND TIMESTAMP '2020-03-08 03:00:00' AS between_constants,
        ntz = TIMESTAMP '2020-03-08 02:30:00' AS gap_equals,
        ntz < TIMESTAMP '2020-03-08 03:00:00' AS gap_less_than,
        ntz = TIMESTAMP '2020-11-01 01:30:00' AS overlap_equals,
        ntz = TIMESTAMP '2020-11-01 01:30:00-08:00' AS later_offset_equals,
        ntz < TIMESTAMP '2020-11-01 01:30:00-08:00' AS later_offset_less_than,
        CAST(ntz AS TIMESTAMP) = TIMESTAMP '2020-11-01 01:30:00' AS explicit_cast
      FROM timestamp_ntz_constant_comparison
      ORDER BY id
      """
    Then query result ordered
      | id | equals | less_than | literal_left | in_list | between_constants | gap_equals | gap_less_than | overlap_equals | later_offset_equals | later_offset_less_than | explicit_cast |
      | 0  | false  | true      | false        | false   | true              | false      | true          | false          | false               | true                   | false         |
      | 1  | false  | true      | false        | false   | true              | true       | true          | false          | false               | true                   | false         |
      | 2  | false  | true      | false        | false   | false             | false      | false         | false          | false               | true                   | false         |
      | 3  | false  | false     | true         | true    | false             | false      | false         | true           | false               | true                   | true          |
      | 4  | false  | false     | true         | false   | false             | false      | false         | false          | false               | true                   | false         |
      | 5  | true   | false     | true         | true    | false             | false      | false         | false          | false               | true                   | false         |
      | 6  | false  | false     | true         | false   | false             | false      | false         | false          | false               | true                   | false         |

  Scenario: TIMESTAMP_NTZ compares with TIMESTAMP constants in the session time zone (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT
        id,
        ntz = TIMESTAMP '2020-06-14 12:00:00' AS equals,
        ntz < TIMESTAMP '2020-06-14 12:00:00' AS less_than,
        TIMESTAMP '2020-06-14 12:00:00' <= ntz AS literal_left,
        ntz IN (TIMESTAMP '2020-06-14 12:00:00', TIMESTAMP '2020-11-01 01:30:00') AS in_list,
        ntz BETWEEN TIMESTAMP '2020-03-08 01:00:00' AND TIMESTAMP '2020-03-08 03:00:00' AS between_constants,
        ntz = TIMESTAMP '2020-03-08 02:30:00' AS gap_equals,
        ntz < TIMESTAMP '2020-03-08 03:00:00' AS gap_less_than,
        ntz = TIMESTAMP '2020-11-01 01:30:00' AS overlap_equals,
        ntz = TIMESTAMP '2020-11-01 01:30:00-08:00' AS later_offset_equals,
        ntz < TIMESTAMP '2020-11-01 01:30:00-08:00' AS later_offset_less_than,
        CAST(ntz AS TIMESTAMP) = TIMESTAMP '2020-11-01 01:30:00' AS explicit_cast
      FROM timestamp_ntz_constant_comparison
      ORDER BY id
      """
    Then query result ordered
      | id | equals | less_than | literal_left | in_list | between_constants | gap_equals | gap_less_than | overlap_equals | later_offset_equals | later_offset_less_than | explicit_cast |
      | 0  | false  | true      | false        | false   | true              | false      | true          | false          | false               | true                   | false         |
      | 1  | false  | true      | false        | false   | true              | true       | true          | false          | false               | true                   | false         |
      | 2  | false  | true      | false        | false   | false             | false      | false         | false          | false               | true                   | false         |
      | 3  | false  | false     | true         | true    | false             | false      | false         | true           | false               | true                   | true          |
      | 4  | false  | false     | true         | false   | false             | false      | false         | false          | false               | true                   | false         |
      | 5  | true   | false     | true         | true    | false             | false      | false         | false          | false               | true                   | false         |
      | 6  | false  | false     | true         | false   | false             | false      | false         | false          | false               | true                   | false         |

  Scenario: TIMESTAMP_NTZ compares with TIMESTAMP constants in the session time zone (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT
        id,
        ntz = TIMESTAMP '2020-06-14 12:00:00' AS equals,
        ntz < TIMESTAMP '2020-06-14 12:00:00' AS less_than,
        TIMESTAMP '2020-06-14 12:00:00' <= ntz AS literal_left,
        ntz IN (TIMESTAMP '2020-06-14 12:00:00', TIMESTAMP '2020-11-01 01:30:00') AS in_list,
        ntz BETWEEN TIMESTAMP '2020-03-08 01:00:00' AND TIMESTAMP '2020-03-08 03:00:00' AS between_constants,
        ntz = TIMESTAMP '2020-03-08 02:30:00' AS gap_equals,
        ntz < TIMESTAMP '2020-03-08 03:00:00' AS gap_less_than,
        ntz = TIMESTAMP '2020-11-01 01:30:00' AS overlap_equals,
        ntz = TIMESTAMP '2020-11-01 01:30:00-08:00' AS later_offset_equals,
        ntz < TIMESTAMP '2020-11-01 01:30:00-08:00' AS later_offset_less_than,
        CAST(ntz AS TIMESTAMP) = TIMESTAMP '2020-11-01 01:30:00' AS explicit_cast
      FROM timestamp_ntz_constant_comparison
      ORDER BY id
      """
    Then query result ordered
      | id | equals | less_than | literal_left | in_list | between_constants | gap_equals | gap_less_than | overlap_equals | later_offset_equals | later_offset_less_than | explicit_cast |
      | 0  | false  | true      | false        | false   | true              | false      | true          | false          | false               | true                   | false         |
      | 1  | false  | true      | false        | false   | true              | true       | true          | false          | false               | true                   | false         |
      | 2  | false  | true      | false        | false   | false             | false      | false         | false          | false               | true                   | false         |
      | 3  | false  | false     | true         | true    | false             | false      | false         | true           | false               | true                   | true          |
      | 4  | false  | false     | true         | false   | false             | false      | false         | false          | false               | true                   | false         |
      | 5  | true   | false     | true         | true    | false             | false      | false         | false          | false               | true                   | false         |
      | 6  | false  | false     | true         | false   | false             | false      | false         | false          | false               | true                   | false         |

  Scenario: Filters compare TIMESTAMP_NTZ with TIMESTAMP constants in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT 'equals' AS predicate, id FROM timestamp_ntz_constant_comparison WHERE ntz = TIMESTAMP '2020-06-14 12:00:00'
      UNION ALL SELECT 'less_than', id FROM timestamp_ntz_constant_comparison WHERE ntz < TIMESTAMP '2020-03-08 03:00:00'
      UNION ALL SELECT 'in_list', id FROM timestamp_ntz_constant_comparison WHERE ntz IN (TIMESTAMP '2020-06-14 12:00:00', TIMESTAMP '2020-11-01 01:30:00')
      UNION ALL SELECT 'between', id FROM timestamp_ntz_constant_comparison WHERE ntz BETWEEN TIMESTAMP '2020-03-08 02:00:00' AND TIMESTAMP '2020-03-08 03:30:00'
      UNION ALL SELECT 'later_offset', id FROM timestamp_ntz_constant_comparison WHERE ntz <= TIMESTAMP '2020-11-01 01:30:00-08:00' AND ntz > TIMESTAMP '2020-06-14 12:00:00'
      UNION ALL SELECT 'interval', id FROM timestamp_ntz_constant_comparison WHERE ntz >= TIMESTAMP '2020-11-01 00:30:00' + INTERVAL 1 HOUR
      ORDER BY predicate, id
      """
    Then query result ordered
      | predicate    | id |
      | between      | 2  |
      | equals       | 5  |
      | in_list      | 3  |
      | in_list      | 5  |
      | interval     | 3  |
      | interval     | 4  |
      | later_offset | 3  |
      | later_offset | 6  |
      | less_than    | 0  |
      | less_than    | 1  |

  Scenario: Filters compare TIMESTAMP_NTZ with TIMESTAMP constants in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT 'equals' AS predicate, id FROM timestamp_ntz_constant_comparison WHERE ntz = TIMESTAMP '2020-06-14 12:00:00'
      UNION ALL SELECT 'less_than', id FROM timestamp_ntz_constant_comparison WHERE ntz < TIMESTAMP '2020-03-08 03:00:00'
      UNION ALL SELECT 'in_list', id FROM timestamp_ntz_constant_comparison WHERE ntz IN (TIMESTAMP '2020-06-14 12:00:00', TIMESTAMP '2020-11-01 01:30:00')
      UNION ALL SELECT 'between', id FROM timestamp_ntz_constant_comparison WHERE ntz BETWEEN TIMESTAMP '2020-03-08 02:00:00' AND TIMESTAMP '2020-03-08 03:30:00'
      UNION ALL SELECT 'later_offset', id FROM timestamp_ntz_constant_comparison WHERE ntz <= TIMESTAMP '2020-11-01 01:30:00-08:00' AND ntz > TIMESTAMP '2020-06-14 12:00:00'
      UNION ALL SELECT 'interval', id FROM timestamp_ntz_constant_comparison WHERE ntz >= TIMESTAMP '2020-11-01 00:30:00' + INTERVAL 1 HOUR
      ORDER BY predicate, id
      """
    Then query result ordered
      | predicate    | id |
      | between      | 1  |
      | between      | 2  |
      | equals       | 5  |
      | in_list      | 3  |
      | in_list      | 5  |
      | interval     | 3  |
      | interval     | 4  |
      | later_offset | 3  |
      | later_offset | 4  |
      | later_offset | 6  |
      | less_than    | 0  |
      | less_than    | 1  |

  Scenario: Filters compare TIMESTAMP_NTZ with TIMESTAMP constants in the session time zone (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT 'equals' AS predicate, id FROM timestamp_ntz_constant_comparison WHERE ntz = TIMESTAMP '2020-06-14 12:00:00'
      UNION ALL SELECT 'less_than', id FROM timestamp_ntz_constant_comparison WHERE ntz < TIMESTAMP '2020-03-08 03:00:00'
      UNION ALL SELECT 'in_list', id FROM timestamp_ntz_constant_comparison WHERE ntz IN (TIMESTAMP '2020-06-14 12:00:00', TIMESTAMP '2020-11-01 01:30:00')
      UNION ALL SELECT 'between', id FROM timestamp_ntz_constant_comparison WHERE ntz BETWEEN TIMESTAMP '2020-03-08 02:00:00' AND TIMESTAMP '2020-03-08 03:30:00'
      UNION ALL SELECT 'later_offset', id FROM timestamp_ntz_constant_comparison WHERE ntz <= TIMESTAMP '2020-11-01 01:30:00-08:00' AND ntz > TIMESTAMP '2020-06-14 12:00:00'
      UNION ALL SELECT 'interval', id FROM timestamp_ntz_constant_comparison WHERE ntz >= TIMESTAMP '2020-11-01 00:30:00' + INTERVAL 1 HOUR
      ORDER BY predicate, id
      """
    Then query result ordered
      | predicate    | id |
      | between      | 1  |
      | between      | 2  |
      | equals       | 5  |
      | in_list      | 3  |
      | in_list      | 5  |
      | interval     | 3  |
      | interval     | 4  |
      | later_offset | 3  |
      | later_offset | 4  |
      | later_offset | 6  |
      | less_than    | 0  |
      | less_than    | 1  |

  Scenario: Filters compare TIMESTAMP_NTZ with TIMESTAMP constants in the session time zone (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT 'equals' AS predicate, id FROM timestamp_ntz_constant_comparison WHERE ntz = TIMESTAMP '2020-06-14 12:00:00'
      UNION ALL SELECT 'less_than', id FROM timestamp_ntz_constant_comparison WHERE ntz < TIMESTAMP '2020-03-08 03:00:00'
      UNION ALL SELECT 'in_list', id FROM timestamp_ntz_constant_comparison WHERE ntz IN (TIMESTAMP '2020-06-14 12:00:00', TIMESTAMP '2020-11-01 01:30:00')
      UNION ALL SELECT 'between', id FROM timestamp_ntz_constant_comparison WHERE ntz BETWEEN TIMESTAMP '2020-03-08 02:00:00' AND TIMESTAMP '2020-03-08 03:30:00'
      UNION ALL SELECT 'later_offset', id FROM timestamp_ntz_constant_comparison WHERE ntz <= TIMESTAMP '2020-11-01 01:30:00-08:00' AND ntz > TIMESTAMP '2020-06-14 12:00:00'
      UNION ALL SELECT 'interval', id FROM timestamp_ntz_constant_comparison WHERE ntz >= TIMESTAMP '2020-11-01 00:30:00' + INTERVAL 1 HOUR
      ORDER BY predicate, id
      """
    Then query result ordered
      | predicate    | id |
      | between      | 1  |
      | between      | 2  |
      | equals       | 5  |
      | in_list      | 3  |
      | in_list      | 5  |
      | interval     | 3  |
      | interval     | 4  |
      | later_offset | 3  |
      | later_offset | 4  |
      | later_offset | 6  |
      | less_than    | 0  |
      | less_than    | 1  |

  Scenario: IN lists compare TIMESTAMP_NTZ with a DST-gap TIMESTAMP constant (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        ntz IN (TIMESTAMP '2020-03-08 03:30:00') AS in_one,
        ntz = TIMESTAMP '2020-03-08 03:30:00' OR ntz = TIMESTAMP '2020-06-14 12:00:00' AS or_equals
      FROM timestamp_ntz_constant_comparison
      ORDER BY id
      """
    Then query result ordered
      | id | in_one | or_equals |
      | 0  | false  | false     |
      | 1  | false  | false     |
      | 2  | true   | true      |
      | 3  | false  | false     |
      | 4  | false  | false     |
      | 5  | false  | true      |
      | 6  | false  | false     |

  @sail-only
  Scenario: IN lists of TIMESTAMP constants match TIMESTAMP_NTZ values like OR of equalities (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        ntz IN (TIMESTAMP '2020-03-08 03:30:00', TIMESTAMP '2020-06-14 12:00:00') AS in_two,
        ntz = TIMESTAMP '2020-03-08 03:30:00' OR ntz = TIMESTAMP '2020-06-14 12:00:00' AS or_equals
      FROM timestamp_ntz_constant_comparison
      ORDER BY id
      """
    Then query result ordered
      | id | in_two | or_equals |
      | 0  | false  | false     |
      | 1  | false  | false     |
      | 2  | true   | true      |
      | 3  | false  | false     |
      | 4  | false  | false     |
      | 5  | true   | true      |
      | 6  | false  | false     |
