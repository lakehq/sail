@sail-only
Feature: Existing Sail datetime extensions remain supported

  Background:
    Given config spark.sql.session.timeZone = UTC

  Scenario Outline: Explicit <cast> converts numeric columns to naive timestamps and back
    When query
      """
      SELECT id, CAST(<cast>(v AS TIMESTAMP_NTZ) AS STRING) AS wall,
             <cast>(<cast>(v AS TIMESTAMP_NTZ) AS DOUBLE) AS seconds
      FROM VALUES (0, -0.5D), (1, 1.25D), (2, CAST(NULL AS DOUBLE)) AS t(id, v)
      ORDER BY id
      """
    Then query result
      | id | wall                       | seconds |
      | 0  | 1969-12-31 23:59:59.5      | -0.5    |
      | 1  | 1970-01-01 00:00:01.25     | 1.25    |
      | 2  | NULL                       | NULL    |

    Examples:
      | cast     |
      | CAST     |
      | TRY_CAST |

  Scenario Outline: Wide decimal columns support explicit <cast> to timestamps
    When query
      """
      SELECT id, unix_micros(<cast>(CAST(v AS DECIMAL(50,0)) AS TIMESTAMP_LTZ)) AS micros
      FROM VALUES (0, -1), (1, 1), (2, CAST(NULL AS INT)) AS t(id, v)
      ORDER BY id
      """
    Then query result
      | id | micros   |
      | 0  | -1000000 |
      | 1  | 1000000  |
      | 2  | NULL     |

    Examples:
      | cast     |
      | CAST     |
      | TRY_CAST |

  Scenario: Timestamp constructors retain castable string columns
    When query
      """
      SELECT id, CAST(timestamp_micros(ts) AS STRING) AS timestamp_value,
             unix_micros(timestamp_millis(n)) AS millis,
             unix_micros(timestamp_seconds(n)) AS seconds
      FROM VALUES (0, '2024-01-01', '123'),
                  (1, '1969-12-31 23:59:59.999999', '-1'),
                  (2, CAST(NULL AS STRING), CAST(NULL AS STRING)) AS t(id, ts, n)
      ORDER BY id
      """
    Then query result
      | id | timestamp_value            | millis | seconds   |
      | 0  | 2024-01-01 00:00:00        | 123000 | 123000000 |
      | 1  | 1969-12-31 23:59:59.999999 | -1000  | -1000000  |
      | 2  | NULL                       | NULL   | NULL      |

  Scenario: Timestamp milliseconds coerce castable inputs to integer milliseconds
    When query
      """
      SELECT id, unix_micros(timestamp_millis(v)) AS micros
      FROM VALUES (0, -1.5D), (1, 1.5D), (2, CAST(NULL AS DOUBLE)) AS t(id, v)
      ORDER BY id
      """
    Then query result
      | id | micros |
      | 0  | -1000  |
      | 1  | 1000   |
      | 2  | NULL   |

  Scenario: Timestamp string constructors preserve UTC instants
    # timestamp_micros retains its native cast to UTC; the session zone controls display.
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(timestamp_micros(v) AS STRING) AS wall,
             unix_micros(timestamp_micros(v)) AS micros
      FROM VALUES ('2024-01-01 00:00:00') AS t(v)
      """
    Then query result
      | wall                | micros           |
      | 2023-12-31 16:00:00 | 1704067200000000 |

  Scenario: Epoch helpers interpret local inputs in the session timezone
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT unix_micros(TIMESTAMP_NTZ '1970-01-01 00:00:01.5') AS ntz,
             unix_micros('1970-01-01 00:00:01.5') AS string,
             unix_seconds(DATE '1970-01-02') AS date
      """
    Then query result
      | ntz         | string      | date   |
      | 28801500000 | 28801500000 | 115200 |

  Scenario: Epoch helpers retain castable string columns
    When query
      """
      SELECT id, unix_seconds(v) AS seconds, unix_millis(v) AS millis, unix_micros(v) AS micros
      FROM VALUES (0, '1970-01-01 00:00:01.234567'),
                  (1, CAST(NULL AS STRING)) AS t(id, v)
      ORDER BY id
      """
    Then query result
      | id | seconds | millis | micros  |
      | 0  | 1       | 1234   | 1234567 |
      | 1  | NULL    | NULL   | NULL    |

  Scenario: Date differences retain numeric day inputs
    When query
      """
      SELECT id, datediff(a, b) AS days
      FROM VALUES (0, 2, 1), (1, -1, 1), (2, CAST(NULL AS INT), 1) AS t(id, a, b)
      ORDER BY id
      """
    Then query result
      | id | days |
      | 0  | 1    |
      | 1  | -2   |
      | 2  | NULL |

  Scenario: Date fields retain interval component extraction
    When query
      """
      SELECT day(INTERVAL '2' DAY) AS days,
             dayofmonth(INTERVAL '3' DAY) AS day_alias,
             month(INTERVAL '14' MONTH) AS months,
             day(CAST(NULL AS INTERVAL DAY)) AS null_days
      """
    Then query result
      | days | day_alias | months | null_days |
      | 2    | 3         | 2      | NULL      |

  Scenario Outline: <function> accepts temporal columns
    Given config spark.sql.timeType.enabled = true
    When query
      """
      SELECT id, CAST(<function>(ts) AS STRING) AS time_value
      FROM VALUES (0, TIMESTAMP '2024-01-01 12:34:56.123456'),
                  (1, CAST(NULL AS TIMESTAMP)) AS t(id, ts)
      ORDER BY id
      """
    Then query result
      | id | time_value      |
      | 0  | 12:34:56.123456 |
      | 1  | NULL            |

    Examples:
      | function    |
      | to_time     |
      | try_to_time |

  Scenario: Window slide and start arguments accept interval literals
    When query
      """
      SELECT window.start, window.end, count(*) AS n
      FROM VALUES (TIMESTAMP '2024-01-01 00:03:00') AS t(ts)
      GROUP BY window(ts, INTERVAL '5' MINUTE, INTERVAL '5' MINUTE, INTERVAL '1' MINUTE)
      """
    Then query result
      | start               | end                 | n |
      | 2024-01-01 00:01:00 | 2024-01-01 00:06:00 | 1 |
