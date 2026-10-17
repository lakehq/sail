Feature: Datetime parsing preserves validation around null values
  # Sail intentionally checks NULL strings before validating the datetime pattern.
  # Spark may initialize a constant formatter before generated row NULL checks, while
  # literal or interpreted NULL inputs skip it. See test_datetime_null_validation.py
  # for explicit engine-specific range/Parquet expectations in both ANSI modes.

  Background:
    Given config spark.sql.ansi.enabled = true

  Scenario Outline: <function> still skips the pattern for a null string literal
    When query
      """
      SELECT <function>(CAST(NULL AS STRING), 'QQQ') AS result
      """
    Then query result
      | result |
      | NULL   |

    Examples:
      | function          |
      | to_date           |
      | try_to_date       |
      | to_timestamp      |
      | try_to_timestamp  |
      | to_timestamp_ltz  |
      | to_timestamp_ntz  |

  Scenario Outline: <function> skips the pattern for null strings in a local relation
    When query
      """
      SELECT <function>(value, 'QQQ') AS result
      FROM VALUES (CAST(NULL AS STRING)), (CAST(NULL AS STRING)) AS t(value)
      """
    Then query result
      | result |
      | NULL   |
      | NULL   |

    Examples:
      | function          |
      | to_date           |
      | try_to_date       |
      | to_timestamp      |
      | try_to_timestamp  |
      | to_timestamp_ltz  |
      | to_timestamp_ntz  |

  Scenario Outline: <function> does not validate a pattern in an unreachable branch
    When query
      """
      SELECT CASE WHEN false THEN CAST(<function>(CAST(id AS STRING), 'QQQ') AS STRING)
        ELSE 'ok' END AS result
      FROM range(2)
      """
    Then query result
      | result |
      | ok     |
      | ok     |

    Examples:
      | function          |
      | to_date           |
      | try_to_date       |
      | to_timestamp      |
      | try_to_timestamp  |
      | to_timestamp_ltz  |
      | to_timestamp_ntz  |

  Scenario Outline: <function> rejects a null <type> value before parsing
    When query
      """
      SELECT <function>(CAST(NULL AS <type>), 'yyyy-MM-dd') AS result
      """
    Then query error (?i)(DATATYPE_MISMATCH|expected string|unsupported data type)

    Examples:
      | function         | type            |
      | to_date          | ARRAY<INT>      |
      | try_to_date      | ARRAY<INT>      |
      | to_timestamp     | ARRAY<INT>      |
      | try_to_timestamp | ARRAY<INT>      |
      | to_timestamp_ltz | ARRAY<INT>      |
      | to_timestamp_ntz | ARRAY<INT>      |
      | to_date          | STRUCT<a: INT>  |
      | to_timestamp     | MAP<STRING,INT> |

  Scenario Outline: Date format skips malformed input with a <format> format
    When query
      """
      SELECT date_format('bad', <format>) AS result
      """
    Then query result
      | result |
      | NULL   |

    Examples:
      | format                                                 |
      | NULL                                                   |
      | CAST(NULL AS STRING)                                   |
      | coalesce(CAST(NULL AS STRING), CAST(NULL AS STRING))     |

  Scenario: Date format skips malformed string columns with a null format
    When query
      """
      SELECT id, date_format(
        CASE WHEN id % 2 = 0 THEN 'bad' ELSE '2024-01-01' END,
        CAST(NULL AS STRING)
      ) AS result
      FROM range(2) ORDER BY id
      """
    Then query result
      | id | result |
      | 0  | NULL   |
      | 1  | NULL   |

  Scenario: Date format retains aggregate cardinality with a null format
    When query
      """
      SELECT date_format(min(value), CAST(NULL AS STRING)) AS result
      FROM VALUES ('2024-01-01'), ('2024-01-02') AS t(value)
      """
    Then query result
      | result |
      | NULL   |

  @sail-only
  Scenario: Date format retains generator cardinality with a null format
    When query
      """
      SELECT date_format(explode(array('2024-01-01', '2024-01-02')), CAST(NULL AS STRING)) AS result
      """
    Then query result
      | result |
      | NULL   |
      | NULL   |
