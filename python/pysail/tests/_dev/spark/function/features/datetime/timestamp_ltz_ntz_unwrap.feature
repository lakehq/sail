Feature: TIMESTAMP cast to TIMESTAMP_NTZ compares with TIMESTAMP_NTZ constants
  Spark compares CAST(timestamp AS TIMESTAMP_NTZ) with a TIMESTAMP_NTZ constant by converting the constant to TIMESTAMP in
  the session time zone when the conversion round-trips (UnwrapCastInBinaryComparison), and pushes filters on a UNION
  of TIMESTAMP_NTZ and TIMESTAMP columns into both branches.
  In a DST overlap, Spark's rule keeps only the earlier instant of a wall-clock time, so Sail compares such constants per
  row instead (intended semantics, as with spark.sql.optimizer.excludedRules=UnwrapCastInBinaryComparison).

  Background:
    Given variable location for temporary directory timestamp_ltz_ntz_unwrap
    Given final statement
      """
      DROP TABLE IF EXISTS timestamp_ltz_ntz_unwrap
      """
    Given config spark.sql.session.timeZone = UTC
    Given statement template
      """
      CREATE TABLE timestamp_ltz_ntz_unwrap (id INT, ltz TIMESTAMP, ntz TIMESTAMP_NTZ) USING parquet LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO timestamp_ltz_ntz_unwrap VALUES (0, TIMESTAMP '2020-03-08 09:59:59', TIMESTAMP_NTZ '2020-03-08 01:59:59')
      """
    Given statement
      """
      INSERT INTO timestamp_ltz_ntz_unwrap VALUES (1, TIMESTAMP '2020-03-08 10:00:00', TIMESTAMP_NTZ '2020-03-08 02:30:00')
      """
    Given statement
      """
      INSERT INTO timestamp_ltz_ntz_unwrap VALUES (2, TIMESTAMP '2020-03-08 10:30:00', TIMESTAMP_NTZ '2020-03-08 03:30:00')
      """
    Given statement
      """
      INSERT INTO timestamp_ltz_ntz_unwrap VALUES (3, TIMESTAMP '2020-11-01 08:30:00', TIMESTAMP_NTZ '2020-11-01 01:30:00')
      """
    Given statement
      """
      INSERT INTO timestamp_ltz_ntz_unwrap VALUES (4, TIMESTAMP '2020-11-01 09:30:00', TIMESTAMP_NTZ '2020-11-01 02:30:00')
      """
    Given statement
      """
      INSERT INTO timestamp_ltz_ntz_unwrap VALUES (5, TIMESTAMP '2020-11-01 10:30:00', TIMESTAMP_NTZ '2020-06-14 12:00:00')
      """
    Given statement
      """
      INSERT INTO timestamp_ltz_ntz_unwrap VALUES (6, TIMESTAMP '2020-06-14 19:00:00', TIMESTAMP_NTZ '2020-06-15 03:00:00')
      """

  Scenario: TIMESTAMP cast to TIMESTAMP_NTZ compares with TIMESTAMP_NTZ constants (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        CAST(ltz AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '2020-06-14 12:00:00' AS equals,
        CAST(ltz AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '2020-03-08 02:30:00' AS gap_equals,
        CAST(ltz AS TIMESTAMP_NTZ) < TIMESTAMP_NTZ '2020-03-08 02:30:00' AS gap_less_than,
        CAST(ltz AS TIMESTAMP_NTZ) IN (TIMESTAMP_NTZ '2020-11-01 01:30:00', TIMESTAMP_NTZ '2020-06-14 12:00:00') AS in_list
      FROM timestamp_ltz_ntz_unwrap
      ORDER BY id
      """
    Then query result ordered
      | id | equals | gap_equals | gap_less_than | in_list |
      | 0  | false  | false      | true          | false   |
      | 1  | false  | false      | false         | false   |
      | 2  | false  | false      | false         | false   |
      | 3  | false  | false      | false         | true    |
      | 4  | false  | false      | false         | true    |
      | 5  | false  | false      | false         | false   |
      | 6  | true   | false      | false         | true    |

  @sail-only
  Scenario: TIMESTAMP cast to TIMESTAMP_NTZ compares with TIMESTAMP_NTZ constants in a DST overlap (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        CAST(ltz AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '2020-11-01 01:30:00' AS overlap_equals,
        CAST(ltz AS TIMESTAMP_NTZ) < TIMESTAMP_NTZ '2020-11-01 01:45:00' AS overlap_less_than,
        CAST(ltz AS TIMESTAMP_NTZ) >= TIMESTAMP_NTZ '2020-11-01 01:45:00' AS overlap_at_least
      FROM timestamp_ltz_ntz_unwrap
      ORDER BY id
      """
    Then query result ordered
      | id | overlap_equals | overlap_less_than | overlap_at_least |
      | 0  | false          | true              | false            |
      | 1  | false          | true              | false            |
      | 2  | false          | true              | false            |
      | 3  | true           | true              | false            |
      | 4  | true           | true              | false            |
      | 5  | false          | false             | true             |
      | 6  | false          | true              | false            |

  Scenario: TIMESTAMP cast to TIMESTAMP_NTZ compares with TIMESTAMP_NTZ constants (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        id,
        CAST(ltz AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '2020-06-14 12:00:00' AS equals,
        CAST(ltz AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '2020-03-08 02:30:00' AS gap_equals,
        CAST(ltz AS TIMESTAMP_NTZ) < TIMESTAMP_NTZ '2020-03-08 02:30:00' AS gap_less_than,
        CAST(ltz AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '2020-11-01 01:30:00' AS overlap_equals,
        CAST(ltz AS TIMESTAMP_NTZ) < TIMESTAMP_NTZ '2020-11-01 01:45:00' AS overlap_less_than,
        CAST(ltz AS TIMESTAMP_NTZ) >= TIMESTAMP_NTZ '2020-11-01 01:45:00' AS overlap_at_least,
        CAST(ltz AS TIMESTAMP_NTZ) IN (TIMESTAMP_NTZ '2020-11-01 01:30:00', TIMESTAMP_NTZ '2020-06-14 12:00:00') AS in_list
      FROM timestamp_ltz_ntz_unwrap
      ORDER BY id
      """
    Then query result ordered
      | id | equals | gap_equals | gap_less_than | overlap_equals | overlap_less_than | overlap_at_least | in_list |
      | 0  | false  | false      | false         | false          | true              | false            | false   |
      | 1  | false  | false      | false         | false          | true              | false            | false   |
      | 2  | false  | false      | false         | false          | true              | false            | false   |
      | 3  | false  | false      | false         | false          | false             | true             | false   |
      | 4  | false  | false      | false         | false          | false             | true             | false   |
      | 5  | false  | false      | false         | false          | false             | true             | false   |
      | 6  | false  | false      | false         | false          | true              | false            | false   |

  Scenario: TIMESTAMP cast to TIMESTAMP_NTZ compares with TIMESTAMP_NTZ constants (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT
        id,
        CAST(ltz AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '2020-06-14 12:00:00' AS equals,
        CAST(ltz AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '2020-03-08 02:30:00' AS gap_equals,
        CAST(ltz AS TIMESTAMP_NTZ) < TIMESTAMP_NTZ '2020-03-08 02:30:00' AS gap_less_than,
        CAST(ltz AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '2020-11-01 01:30:00' AS overlap_equals,
        CAST(ltz AS TIMESTAMP_NTZ) < TIMESTAMP_NTZ '2020-11-01 01:45:00' AS overlap_less_than,
        CAST(ltz AS TIMESTAMP_NTZ) >= TIMESTAMP_NTZ '2020-11-01 01:45:00' AS overlap_at_least,
        CAST(ltz AS TIMESTAMP_NTZ) IN (TIMESTAMP_NTZ '2020-11-01 01:30:00', TIMESTAMP_NTZ '2020-06-14 12:00:00') AS in_list
      FROM timestamp_ltz_ntz_unwrap
      ORDER BY id
      """
    Then query result ordered
      | id | equals | gap_equals | gap_less_than | overlap_equals | overlap_less_than | overlap_at_least | in_list |
      | 0  | false  | false      | false         | false          | true              | false            | false   |
      | 1  | false  | false      | false         | false          | true              | false            | false   |
      | 2  | false  | false      | false         | false          | true              | false            | false   |
      | 3  | false  | false      | false         | false          | false             | true             | false   |
      | 4  | false  | false      | false         | false          | false             | true             | false   |
      | 5  | false  | false      | false         | false          | false             | true             | false   |
      | 6  | false  | false      | false         | false          | true              | false            | false   |

  Scenario: Filters compare TIMESTAMP cast to TIMESTAMP_NTZ with TIMESTAMP_NTZ constants (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT 'equals' AS predicate, id FROM timestamp_ltz_ntz_unwrap WHERE CAST(ltz AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '2020-06-14 12:00:00'
      UNION ALL SELECT 'gap_less_than', id FROM timestamp_ltz_ntz_unwrap WHERE CAST(ltz AS TIMESTAMP_NTZ) < TIMESTAMP_NTZ '2020-03-08 02:30:00'
      UNION ALL SELECT 'equals_next_day', id FROM timestamp_ltz_ntz_unwrap WHERE CAST(ltz AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '2020-06-15 03:00:00'
      UNION ALL SELECT 'in_list', id FROM timestamp_ltz_ntz_unwrap WHERE CAST(ltz AS TIMESTAMP_NTZ) IN (TIMESTAMP_NTZ '2020-11-01 01:30:00', TIMESTAMP_NTZ '2020-06-14 12:00:00')
      ORDER BY predicate, id
      """
    Then query result ordered
      | predicate     | id |
      | equals        | 6  |
      | gap_less_than | 0  |
      | in_list       | 3  |
      | in_list       | 4  |
      | in_list       | 6  |

  @sail-only
  Scenario: Filters compare TIMESTAMP cast to TIMESTAMP_NTZ with TIMESTAMP_NTZ constants in a DST overlap (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT 'overlap_equals' AS predicate, id FROM timestamp_ltz_ntz_unwrap WHERE CAST(ltz AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '2020-11-01 01:30:00'
      UNION ALL SELECT 'overlap_between', id FROM timestamp_ltz_ntz_unwrap WHERE CAST(ltz AS TIMESTAMP_NTZ) BETWEEN TIMESTAMP_NTZ '2020-11-01 01:00:00' AND TIMESTAMP_NTZ '2020-11-01 01:45:00'
      ORDER BY predicate, id
      """
    Then query result ordered
      | predicate       | id |
      | overlap_between | 3  |
      | overlap_between | 4  |
      | overlap_equals  | 3  |
      | overlap_equals  | 4  |

  Scenario: Filters compare TIMESTAMP cast to TIMESTAMP_NTZ with TIMESTAMP_NTZ constants (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT 'equals' AS predicate, id FROM timestamp_ltz_ntz_unwrap WHERE CAST(ltz AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '2020-06-14 12:00:00'
      UNION ALL SELECT 'gap_less_than', id FROM timestamp_ltz_ntz_unwrap WHERE CAST(ltz AS TIMESTAMP_NTZ) < TIMESTAMP_NTZ '2020-03-08 02:30:00'
      UNION ALL SELECT 'overlap_equals', id FROM timestamp_ltz_ntz_unwrap WHERE CAST(ltz AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '2020-11-01 01:30:00'
      UNION ALL SELECT 'equals_next_day', id FROM timestamp_ltz_ntz_unwrap WHERE CAST(ltz AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '2020-06-15 03:00:00'
      UNION ALL SELECT 'overlap_between', id FROM timestamp_ltz_ntz_unwrap WHERE CAST(ltz AS TIMESTAMP_NTZ) BETWEEN TIMESTAMP_NTZ '2020-11-01 01:00:00' AND TIMESTAMP_NTZ '2020-11-01 01:45:00'
      ORDER BY predicate, id
      """
    Then query result ordered
      | predicate       | id |
      | equals_next_day | 6  |

  Scenario: Filters on a UNION of TIMESTAMP_NTZ and TIMESTAMP columns compare in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT branch, id FROM (
        SELECT 'ntz' AS branch, id, ntz AS v FROM timestamp_ltz_ntz_unwrap
        UNION ALL
        SELECT 'ltz' AS branch, id, ltz AS v FROM timestamp_ltz_ntz_unwrap
      ) u
      WHERE v = TIMESTAMP '2020-06-14 12:00:00' OR v = TIMESTAMP '2020-03-08 03:30:00' OR v < TIMESTAMP '2020-03-08 03:00:00'
        OR v = TIMESTAMP '2020-11-01 01:30:00' OR v = TIMESTAMP '2020-06-15 03:00:00'
      ORDER BY branch, id
      """
    Then query result ordered
      | branch | id |
      | ltz    | 0  |
      | ltz    | 2  |
      | ltz    | 3  |
      | ltz    | 6  |
      | ntz    | 0  |
      | ntz    | 1  |
      | ntz    | 2  |
      | ntz    | 3  |
      | ntz    | 5  |
      | ntz    | 6  |

  Scenario: Filters on a UNION of TIMESTAMP_NTZ and TIMESTAMP columns compare in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT branch, id FROM (
        SELECT 'ntz' AS branch, id, ntz AS v FROM timestamp_ltz_ntz_unwrap
        UNION ALL
        SELECT 'ltz' AS branch, id, ltz AS v FROM timestamp_ltz_ntz_unwrap
      ) u
      WHERE v = TIMESTAMP '2020-06-14 12:00:00' OR v = TIMESTAMP '2020-03-08 03:30:00' OR v < TIMESTAMP '2020-03-08 03:00:00'
        OR v = TIMESTAMP '2020-11-01 01:30:00' OR v = TIMESTAMP '2020-06-15 03:00:00'
      ORDER BY branch, id
      """
    Then query result ordered
      | branch | id |
      | ltz    | 6  |
      | ntz    | 0  |
      | ntz    | 1  |
      | ntz    | 2  |
      | ntz    | 3  |
      | ntz    | 5  |
      | ntz    | 6  |

  Scenario: Filters on a UNION of TIMESTAMP_NTZ and TIMESTAMP columns compare with a DST-gap constant (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT branch, id FROM (
        SELECT 'ntz' AS branch, id, ntz AS v FROM timestamp_ltz_ntz_unwrap
        UNION ALL
        SELECT 'ltz' AS branch, id, ltz AS v FROM timestamp_ltz_ntz_unwrap
      ) u
      WHERE v = TIMESTAMP '2020-03-08 02:30:00'
      ORDER BY branch, id
      """
    Then query result ordered
      | branch | id |
      | ltz    | 2  |
      | ntz    | 2  |
