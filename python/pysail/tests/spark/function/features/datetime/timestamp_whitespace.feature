Feature: Unformatted timestamps trim outer Spark whitespace
  Dated timestamps ignore outer ASCII whitespace and control characters.
  Trimming does not change time-only markers, internal separators, or explicit formats.

  Background:
    Given config spark.sql.session.timeZone = UTC
    And config spark.sql.timestampType = TIMESTAMP_LTZ

  Scenario Outline: Dated timestamp columns trim every ASCII control under ANSI <ansi> with <timestamp_type>
    Given config spark.sql.ansi.enabled = <ansi>
    And config spark.sql.timestampType = <timestamp_type>
    When query
      """
      SELECT count(*) AS inputs,
             bool_and(CAST(value AS TIMESTAMP) <=> TIMESTAMP '2000-02-29 12:34:56.123456') AS cast_matches,
             bool_and(to_timestamp(value) <=> TIMESTAMP '2000-02-29 12:34:56.123456') AS function_matches,
             bool_and(try_to_timestamp(value) <=> TIMESTAMP '2000-02-29 12:34:56.123456') AS try_matches,
             bool_and(to_timestamp_ltz(value) <=> TIMESTAMP_LTZ '2000-02-29 12:34:56.123456') AS ltz_matches,
             bool_and(to_timestamp_ntz(value) <=> TIMESTAMP_NTZ '2000-02-29 12:34:56.123456') AS ntz_matches
      FROM (
        SELECT concat(padding, '2000-02-29 12:34:56.123456', padding) AS value
        FROM (SELECT chr(CASE WHEN id = 33 THEN 127 ELSE id END) AS padding FROM range(34))
      )
      """
    Then query result
      | inputs | cast_matches | function_matches | try_matches | ltz_matches | ntz_matches |
      | 34     | true         | true             | true        | true        | true        |

    Examples:
      | ansi  | timestamp_type |
      | false | TIMESTAMP_LTZ  |
      | true  | TIMESTAMP_LTZ  |
      | false | TIMESTAMP_NTZ  |
      | true  | TIMESTAMP_NTZ  |

  Scenario: Dated timestamp trimming preserves date-only values signed years and explicit offsets
    When query
      """
      SELECT id, CAST(value AS TIMESTAMP_LTZ) <=> expected AS matches
      FROM VALUES
        (1, concat(chr(0), '1970-01-01', chr(127)), TIMESTAMP_LTZ '1970-01-01 00:00:00'),
        (2, concat(chr(9), '+1970-01-01 01:58:00', chr(10)), TIMESTAMP_LTZ '1970-01-01 01:58:00'),
        (3, concat(chr(13), '1969-12-31 23:59:59.123456', chr(31)), TIMESTAMP_LTZ '1969-12-31 23:59:59.123456'),
        (4, concat(chr(9), '2020-01-01 01:58:00+05:30', chr(127)), TIMESTAMP_LTZ '2019-12-31 20:28:00')
        AS t(id, value, expected)
      ORDER BY id
      """
    Then query result
      | id | matches |
      | 1  | true    |
      | 2  | true    |
      | 3  | true    |
      | 4  | true    |

  Scenario: Dated timestamp trimming does not remove non-ASCII whitespace
    When query
      """
      SELECT bool_and(try_cast(value AS TIMESTAMP_LTZ) IS NULL) AS ltz_null,
             bool_and(try_cast(value AS TIMESTAMP_NTZ) IS NULL) AS ntz_null
      FROM VALUES
        (concat(chr(133), '2020-01-01 01:58:00')),
        (concat(chr(160), '2020-01-01 01:58:00')) AS t(value)
      """
    Then query result
      | ltz_null | ntz_null |
      | true     | true     |

  Scenario Outline: Dated timestamp trimming preserves null results under ANSI <ansi> with <timestamp_type>
    Given config spark.sql.ansi.enabled = <ansi>
    And config spark.sql.timestampType = <timestamp_type>
    When query
      """
      SELECT count(*) AS inputs,
             bool_and(<cast_function>(value AS TIMESTAMP) IS NULL) AS cast_null,
             bool_and(<timestamp_function>(value) IS NULL) AS function_null,
             bool_and(try_to_timestamp(value) IS NULL) AS try_null
      FROM VALUES
        (concat(chr(9), '2020-02-30 01:58:00', chr(127))),
        (concat(chr(0), chr(9), ' ', chr(127))),
        (concat('2020-01-01', chr(9), '01:58:00')),
        (''),
        (CAST(NULL AS STRING)) AS t(value)
      """
    Then query result
      | inputs | cast_null | function_null | try_null |
      | 5      | true      | true          | true     |

    Examples:
      | ansi  | timestamp_type | cast_function | timestamp_function |
      | false | TIMESTAMP_LTZ  | CAST          | to_timestamp       |
      | false | TIMESTAMP_NTZ  | CAST          | to_timestamp       |
      | true  | TIMESTAMP_LTZ  | TRY_CAST      | try_to_timestamp   |
      | true  | TIMESTAMP_NTZ  | TRY_CAST      | try_to_timestamp   |

  Scenario Outline: Dated timestamp trimming retains ANSI errors for <conversion>
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT <conversion> AS result
      """
    Then query error .*

    Examples:
      | conversion                                                                  |
      | CAST(concat(chr(9), '2020-02-30 01:58:00', chr(127)) AS TIMESTAMP_LTZ)         |
      | to_timestamp_ntz(concat(chr(9), '2020-02-30 01:58:00', chr(127)))             |
      | CAST(concat(chr(0), chr(9), ' ', chr(127)) AS TIMESTAMP_NTZ)                   |

  Scenario: Timestamp trimming preserves time-only marker rules for every ASCII control
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT count(*) AS inputs,
             bool_and(try_cast(concat(padding, 'T01:58:00', padding) AS TIMESTAMP_LTZ) IS NULL) AS prefixed_t_null,
             bool_and(CAST(concat(padding, '01:58:00', padding) AS TIMESTAMP_LTZ) <=> expected) AS numeric_matches,
             bool_and(CAST(concat('T01:58:00', padding) AS TIMESTAMP_LTZ) <=> expected) AS trailing_t_matches,
             bool_and(try_cast(concat(padding, '01:58:00', padding) AS TIMESTAMP_NTZ) IS NULL) AS ntz_null
      FROM (
        SELECT chr(CASE WHEN id = 33 THEN 127 ELSE id END) AS padding,
               CAST(concat(current_date(), ' 01:58:00') AS TIMESTAMP_LTZ) AS expected
        FROM range(34)
      )
      """
    Then query result
      | inputs | prefixed_t_null | numeric_matches | trailing_t_matches | ntz_null |
      | 34     | true            | true            | true               | true     |

  Scenario: Timestamp coercion callers share dated whitespace trimming
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT sequence(value, TIMESTAMP_LTZ '2020-01-02 01:58:00', INTERVAL 1 DAY) =
               array(TIMESTAMP_LTZ '2020-01-01 01:58:00', TIMESTAMP_LTZ '2020-01-02 01:58:00') AS sequence_matches,
             coalesce(CAST(NULL AS TIMESTAMP_LTZ), value) = TIMESTAMP_LTZ '2020-01-01 01:58:00' AS coalesce_matches,
             value + INTERVAL 1 DAY = TIMESTAMP_LTZ '2020-01-02 01:58:00' AS interval_matches
      FROM (SELECT concat(chr(0), chr(9), '2020-01-01 01:58:00', chr(10), chr(127)) AS value)
      """
    Then query result
      | sequence_matches | coalesce_matches | interval_matches |
      | true             | true             | true             |

  Scenario: Explicit timestamp formats retain their whitespace requirements
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.legacy.timeParserPolicy = CORRECTED
    When query
      """
      SELECT to_timestamp(' 2020-01-01 01:58:00 ', 'yyyy-MM-dd HH:mm:ss') AS ltz_result,
             to_timestamp_ntz(' 2020-01-01 01:58:00 ', 'yyyy-MM-dd HH:mm:ss') AS ntz_result,
             try_to_timestamp(' 2020-01-01 01:58:00 ', 'yyyy-MM-dd HH:mm:ss') AS try_result,
             to_timestamp(' 2020-01-01 01:58:00 ', ' yyyy-MM-dd HH:mm:ss ') AS matching_format
      """
    Then query result
      | ltz_result | ntz_result | try_result | matching_format     |
      | NULL       | NULL       | NULL       | 2020-01-01 01:58:00 |
