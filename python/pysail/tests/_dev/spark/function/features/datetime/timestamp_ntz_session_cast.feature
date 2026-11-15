Feature: TIMESTAMP_NTZ casts to TIMESTAMP in the session time zone
  Spark casts TIMESTAMP_NTZ values compared with TIMESTAMP values in the session time zone, moving a time in a
  DST gap forward and resolving a time in a DST overlap to the earlier offset.

  Background:
    Given variable location for temporary directory timestamp_ntz_session_cast
    Given final statement
      """
      DROP TABLE IF EXISTS timestamp_ntz_session_cast
      """
    Given config spark.sql.session.timeZone = UTC
    Given statement template
      """
      CREATE TABLE timestamp_ntz_session_cast (id INT, ntz TIMESTAMP_NTZ, s STRING) USING parquet LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO timestamp_ntz_session_cast VALUES
        (0, TIMESTAMP_NTZ '2020-03-08 02:30:00', '2020-03-08 03:30:00'),
        (1, TIMESTAMP_NTZ '2020-06-14 12:00:00', '2020-06-14 12:00:00'),
        (2, TIMESTAMP_NTZ '2020-11-01 01:30:00', '2020-11-01 01:30:00')
      """

  Scenario: TIMESTAMP_NTZ compares with TIMESTAMP values in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        ntz = CAST(s AS TIMESTAMP) AS equals,
        ntz <= CAST(s AS TIMESTAMP) AS at_most,
        ntz IN (CAST(s AS TIMESTAMP)) AS in_list,
        array_contains(array(CAST(s AS TIMESTAMP)), ntz) AS array_contains,
        CAST(ntz AS TIMESTAMP) = CAST(s AS TIMESTAMP) AS explicit_cast
      FROM timestamp_ntz_session_cast
      ORDER BY id
      """
    Then query result
      | id | equals | at_most | in_list | array_contains | explicit_cast |
      | 0  | true   | true    | true    | true           | true          |
      | 1  | true   | true    | true    | true           | true          |
      | 2  | true   | true    | true    | true           | true          |

  Scenario: TIMESTAMP_NTZ compares with TIMESTAMP values in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        id,
        ntz = CAST(s AS TIMESTAMP) AS equals,
        ntz <= CAST(s AS TIMESTAMP) AS at_most,
        ntz IN (CAST(s AS TIMESTAMP)) AS in_list,
        array_contains(array(CAST(s AS TIMESTAMP)), ntz) AS array_contains,
        CAST(ntz AS TIMESTAMP) = CAST(s AS TIMESTAMP) AS explicit_cast
      FROM timestamp_ntz_session_cast
      ORDER BY id
      """
    Then query result
      | id | equals | at_most | in_list | array_contains | explicit_cast |
      | 0  | false  | true    | false   | false          | false         |
      | 1  | true   | true    | true    | true           | true          |
      | 2  | true   | true    | true    | true           | true          |

  Scenario: TIMESTAMP_NTZ compares with TIMESTAMP values in the session time zone (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT
        id,
        ntz = CAST(s AS TIMESTAMP) AS equals,
        ntz <= CAST(s AS TIMESTAMP) AS at_most,
        ntz IN (CAST(s AS TIMESTAMP)) AS in_list,
        array_contains(array(CAST(s AS TIMESTAMP)), ntz) AS array_contains,
        CAST(ntz AS TIMESTAMP) = CAST(s AS TIMESTAMP) AS explicit_cast
      FROM timestamp_ntz_session_cast
      ORDER BY id
      """
    Then query result
      | id | equals | at_most | in_list | array_contains | explicit_cast |
      | 0  | false  | true    | false   | false          | false         |
      | 1  | true   | true    | true    | true           | true          |
      | 2  | true   | true    | true    | true           | true          |

  Scenario: TIMESTAMP_NTZ compares with TIMESTAMP values in the session time zone (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT
        id,
        ntz = CAST(s AS TIMESTAMP) AS equals,
        ntz <= CAST(s AS TIMESTAMP) AS at_most,
        ntz IN (CAST(s AS TIMESTAMP)) AS in_list,
        array_contains(array(CAST(s AS TIMESTAMP)), ntz) AS array_contains,
        CAST(ntz AS TIMESTAMP) = CAST(s AS TIMESTAMP) AS explicit_cast
      FROM timestamp_ntz_session_cast
      ORDER BY id
      """
    Then query result
      | id | equals | at_most | in_list | array_contains | explicit_cast |
      | 0  | false  | true    | false   | false          | false         |
      | 1  | true   | true    | true    | true           | true          |
      | 2  | true   | true    | true    | true           | true          |

  Scenario: Joins compare TIMESTAMP_NTZ with TIMESTAMP values in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT a.id AS ntz_id, b.id AS string_id
      FROM timestamp_ntz_session_cast a JOIN timestamp_ntz_session_cast b ON a.ntz = CAST(b.s AS TIMESTAMP)
      ORDER BY a.id
      """
    Then query result
      | ntz_id | string_id |
      | 0      | 0         |
      | 1      | 1         |
      | 2      | 2         |

  Scenario: Joins compare TIMESTAMP_NTZ with TIMESTAMP values in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT a.id AS ntz_id, b.id AS string_id
      FROM timestamp_ntz_session_cast a JOIN timestamp_ntz_session_cast b ON a.ntz = CAST(b.s AS TIMESTAMP)
      ORDER BY a.id
      """
    Then query result
      | ntz_id | string_id |
      | 1      | 1         |
      | 2      | 2         |

  Scenario: Joins compare TIMESTAMP_NTZ with TIMESTAMP values in the session time zone (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT a.id AS ntz_id, b.id AS string_id
      FROM timestamp_ntz_session_cast a JOIN timestamp_ntz_session_cast b ON a.ntz = CAST(b.s AS TIMESTAMP)
      ORDER BY a.id
      """
    Then query result
      | ntz_id | string_id |
      | 1      | 1         |
      | 2      | 2         |

  Scenario: Joins compare TIMESTAMP_NTZ with TIMESTAMP values in the session time zone (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT a.id AS ntz_id, b.id AS string_id
      FROM timestamp_ntz_session_cast a JOIN timestamp_ntz_session_cast b ON a.ntz = CAST(b.s AS TIMESTAMP)
      ORDER BY a.id
      """
    Then query result
      | ntz_id | string_id |
      | 1      | 1         |
      | 2      | 2         |
