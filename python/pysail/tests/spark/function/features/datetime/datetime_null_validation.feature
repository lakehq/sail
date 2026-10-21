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

  # Known NULL results do not bypass argument validation: grouping references must
  # survive analysis before NULL folding or Python UDF extraction.
  Scenario Outline: Known NULL in <case> retains grouping validation with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT <expression> AS result FROM range(2) GROUP BY id % 2
      """
    Then query error (?i)(MISSING_AGGREGATION|No field named)

    Examples:
      | case | ansi | expression |
      | make_date year | true | make_date(CAST(NULL AS INT), CAST(id AS INT), 1) |
      | make_date year | false | make_date(CAST(NULL AS INT), CAST(id AS INT), 1) |
      | convert_timezone source | true | convert_timezone(CAST(NULL AS STRING), CAST(id AS STRING), TIMESTAMP_NTZ '2024-01-01') |
      | convert_timezone source | false | convert_timezone(CAST(NULL AS STRING), CAST(id AS STRING), TIMESTAMP_NTZ '2024-01-01') |
      | from_utc_timestamp zone | true | from_utc_timestamp(CAST(id AS TIMESTAMP), CAST(NULL AS STRING)) |
      | from_utc_timestamp zone | false | from_utc_timestamp(CAST(id AS TIMESTAMP), CAST(NULL AS STRING)) |
      | to_utc_timestamp zone | true | to_utc_timestamp(CAST(id AS TIMESTAMP), CAST(NULL AS STRING)) |
      | to_utc_timestamp zone | false | to_utc_timestamp(CAST(id AS TIMESTAMP), CAST(NULL AS STRING)) |
      | date_trunc unit | true | date_trunc(CAST(NULL AS STRING), CAST(id AS TIMESTAMP)) |
      | date_trunc unit | false | date_trunc(CAST(NULL AS STRING), CAST(id AS TIMESTAMP)) |
      | to_timestamp format | true | to_timestamp(CAST(id AS STRING), CAST(NULL AS STRING)) |
      | to_timestamp format | false | to_timestamp(CAST(id AS STRING), CAST(NULL AS STRING)) |
      | to_timestamp_ntz format | true | to_timestamp_ntz(CAST(id AS STRING), CAST(NULL AS STRING)) |
      | to_timestamp_ntz format | false | to_timestamp_ntz(CAST(id AS STRING), CAST(NULL AS STRING)) |
      | try_to_timestamp format | true | try_to_timestamp(CAST(id AS STRING), CAST(NULL AS STRING)) |
      | try_to_timestamp format | false | try_to_timestamp(CAST(id AS STRING), CAST(NULL AS STRING)) |
      | to_timestamp value | true | to_timestamp(CAST(NULL AS STRING), CAST(id AS STRING)) |
      | to_timestamp value | false | to_timestamp(CAST(NULL AS STRING), CAST(id AS STRING)) |

  Scenario Outline: Known NULL in <case> preserves valid grouped rows with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT <expression> AS result FROM range(2) GROUP BY id
      """
    Then query result
      | result |
      | NULL   |
      | NULL   |

    Examples:
      | case | ansi | expression |
      | make_date year | true | make_date(CAST(NULL AS INT), CAST(id AS INT), 1) |
      | make_date year | false | make_date(CAST(NULL AS INT), CAST(id AS INT), 1) |
      | convert_timezone source | true | convert_timezone(CAST(NULL AS STRING), CAST(id AS STRING), TIMESTAMP_NTZ '2024-01-01') |
      | convert_timezone source | false | convert_timezone(CAST(NULL AS STRING), CAST(id AS STRING), TIMESTAMP_NTZ '2024-01-01') |
      | from_utc_timestamp zone | true | from_utc_timestamp(CAST(id AS TIMESTAMP), CAST(NULL AS STRING)) |
      | from_utc_timestamp zone | false | from_utc_timestamp(CAST(id AS TIMESTAMP), CAST(NULL AS STRING)) |
      | to_utc_timestamp zone | true | to_utc_timestamp(CAST(id AS TIMESTAMP), CAST(NULL AS STRING)) |
      | to_utc_timestamp zone | false | to_utc_timestamp(CAST(id AS TIMESTAMP), CAST(NULL AS STRING)) |
      | date_trunc unit | true | date_trunc(CAST(NULL AS STRING), CAST(id AS TIMESTAMP)) |
      | date_trunc unit | false | date_trunc(CAST(NULL AS STRING), CAST(id AS TIMESTAMP)) |
      | to_timestamp format | true | to_timestamp(CAST(id AS STRING), CAST(NULL AS STRING)) |
      | to_timestamp format | false | to_timestamp(CAST(id AS STRING), CAST(NULL AS STRING)) |
      | to_timestamp_ntz format | true | to_timestamp_ntz(CAST(id AS STRING), CAST(NULL AS STRING)) |
      | to_timestamp_ntz format | false | to_timestamp_ntz(CAST(id AS STRING), CAST(NULL AS STRING)) |
      | try_to_timestamp format | true | try_to_timestamp(CAST(id AS STRING), CAST(NULL AS STRING)) |
      | try_to_timestamp format | false | try_to_timestamp(CAST(id AS STRING), CAST(NULL AS STRING)) |
      | to_timestamp value | true | to_timestamp(CAST(NULL AS STRING), CAST(id AS STRING)) |
      | to_timestamp value | false | to_timestamp(CAST(NULL AS STRING), CAST(id AS STRING)) |

  # NULL propagation must also retain static type checks inside scalar subqueries.
  Scenario Outline: Known NULL retains invalid <case> inside a scalar subquery
    When query
      """
      SELECT <expression> AS result
      """
    Then query error (?i)(DATATYPE_MISMATCH|cannot cast|unsupported.*cast|casting.*not supported)

    Examples:
      | case | expression |
      | CAST in make_date | make_date(CAST(NULL AS INT), (SELECT CAST(array(1) AS INT)), 1) |
      | CAST in convert_timezone | convert_timezone(CAST(NULL AS STRING), (SELECT CAST(CAST(array(1) AS INT) AS STRING)), TIMESTAMP_NTZ '2024-01-01') |
      | CAST in to_timestamp | to_timestamp(CAST(NULL AS STRING), (SELECT CAST(CAST(array(1) AS INT) AS STRING))) |
      | TRY_CAST in make_date | make_date(CAST(NULL AS INT), (SELECT TRY_CAST(array(1) AS INT)), 1) |
      | TRY_CAST in convert_timezone | convert_timezone(CAST(NULL AS STRING), (SELECT CAST(TRY_CAST(array(1) AS INT) AS STRING)), TIMESTAMP_NTZ '2024-01-01') |
      | TRY_CAST in to_timestamp | to_timestamp(CAST(NULL AS STRING), (SELECT CAST(TRY_CAST(array(1) AS INT) AS STRING))) |
