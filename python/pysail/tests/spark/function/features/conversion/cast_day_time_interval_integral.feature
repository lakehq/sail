Feature: Day-time interval integral casts use the end field without float rounding

  Scenario Outline: Interval integral literal <qualifier> <method> ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT <method>(INTERVAL '<value>' <qualifier> AS TINYINT) AS c0, <method>(INTERVAL '<value>' <qualifier> AS SMALLINT) AS c1, <method>(INTERVAL '<value>' <qualifier> AS INT) AS c2, <method>(INTERVAL '<value>' <qualifier> AS BIGINT) AS c3
      """
    Then query schema
      """
      root
       |-- c0: byte (nullable = <nullable>)
       |-- c1: short (nullable = <nullable>)
       |-- c2: integer (nullable = <nullable>)
       |-- c3: long (nullable = <nullable>)
      """
    Then query result ordered
      | c0 | c1 | c2 | c3 |
      | 2  | 2  | 2  | 2  |

    Examples:
      | qualifier | value | method | ansi | nullable |
      | DAY | 2 | CAST | true | false |
      | DAY | 2 | TRY_CAST | true | true |
      | HOUR | 2 | CAST | true | false |
      | HOUR | 2 | TRY_CAST | true | true |
      | MINUTE | 2 | CAST | true | false |
      | MINUTE | 2 | TRY_CAST | true | true |
      | SECOND | 2.999999 | CAST | true | false |
      | SECOND | 2.999999 | TRY_CAST | true | true |
      | DAY TO HOUR | 0 02 | CAST | true | false |
      | DAY TO HOUR | 0 02 | TRY_CAST | true | true |
      | DAY TO MINUTE | 0 00:02 | CAST | true | false |
      | DAY TO MINUTE | 0 00:02 | TRY_CAST | true | true |
      | DAY TO SECOND | 0 00:00:02.999999 | CAST | true | false |
      | DAY TO SECOND | 0 00:00:02.999999 | TRY_CAST | true | true |
      | HOUR TO MINUTE | 0:02 | CAST | true | false |
      | HOUR TO MINUTE | 0:02 | TRY_CAST | true | true |
      | HOUR TO SECOND | 0:00:02.999999 | CAST | true | false |
      | HOUR TO SECOND | 0:00:02.999999 | TRY_CAST | true | true |
      | MINUTE TO SECOND | 0:02.999999 | CAST | true | false |
      | MINUTE TO SECOND | 0:02.999999 | TRY_CAST | true | true |
      | DAY | 2 | CAST | false | false |
      | DAY | 2 | TRY_CAST | false | true |
      | HOUR | 2 | CAST | false | false |
      | HOUR | 2 | TRY_CAST | false | true |
      | MINUTE | 2 | CAST | false | false |
      | MINUTE | 2 | TRY_CAST | false | true |
      | SECOND | 2.999999 | CAST | false | false |
      | SECOND | 2.999999 | TRY_CAST | false | true |
      | DAY TO HOUR | 0 02 | CAST | false | false |
      | DAY TO HOUR | 0 02 | TRY_CAST | false | true |
      | DAY TO MINUTE | 0 00:02 | CAST | false | false |
      | DAY TO MINUTE | 0 00:02 | TRY_CAST | false | true |
      | DAY TO SECOND | 0 00:00:02.999999 | CAST | false | false |
      | DAY TO SECOND | 0 00:00:02.999999 | TRY_CAST | false | true |
      | HOUR TO MINUTE | 0:02 | CAST | false | false |
      | HOUR TO MINUTE | 0:02 | TRY_CAST | false | true |
      | HOUR TO SECOND | 0:00:02.999999 | CAST | false | false |
      | HOUR TO SECOND | 0:00:02.999999 | TRY_CAST | false | true |
      | MINUTE TO SECOND | 0:02.999999 | CAST | false | false |
      | MINUTE TO SECOND | 0:02.999999 | TRY_CAST | false | true |

  Scenario Outline: Interval integral column <qualifier> <method> ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT <method>(i AS TINYINT) AS c0, <method>(i AS SMALLINT) AS c1, <method>(i AS INT) AS c2, <method>(i AS BIGINT) AS c3
      FROM VALUES (INTERVAL '<value>' <qualifier>), (INTERVAL '-<value>' <qualifier>),
        (CAST(NULL AS INTERVAL <qualifier>)) AS t(i)
      """
    Then query schema
      """
      root
       |-- c0: byte (nullable = <nullable>)
       |-- c1: short (nullable = <nullable>)
       |-- c2: integer (nullable = <nullable>)
       |-- c3: long (nullable = <nullable>)
      """
    Then query result ordered
      | c0 | c1 | c2 | c3 |
      | 2  | 2  | 2  | 2  |
      | -2 | -2 | -2 | -2 |
      | NULL | NULL | NULL | NULL |

    Examples:
      | qualifier | value | method | ansi | nullable |
      | DAY | 2 | CAST | true | true |
      | DAY | 2 | TRY_CAST | true | true |
      | HOUR | 2 | CAST | true | true |
      | HOUR | 2 | TRY_CAST | true | true |
      | MINUTE | 2 | CAST | true | true |
      | MINUTE | 2 | TRY_CAST | true | true |
      | SECOND | 2.999999 | CAST | true | true |
      | SECOND | 2.999999 | TRY_CAST | true | true |
      | DAY TO HOUR | 0 02 | CAST | true | true |
      | DAY TO HOUR | 0 02 | TRY_CAST | true | true |
      | DAY TO MINUTE | 0 00:02 | CAST | true | true |
      | DAY TO MINUTE | 0 00:02 | TRY_CAST | true | true |
      | DAY TO SECOND | 0 00:00:02.999999 | CAST | true | true |
      | DAY TO SECOND | 0 00:00:02.999999 | TRY_CAST | true | true |
      | HOUR TO MINUTE | 0:02 | CAST | true | true |
      | HOUR TO MINUTE | 0:02 | TRY_CAST | true | true |
      | HOUR TO SECOND | 0:00:02.999999 | CAST | true | true |
      | HOUR TO SECOND | 0:00:02.999999 | TRY_CAST | true | true |
      | MINUTE TO SECOND | 0:02.999999 | CAST | true | true |
      | MINUTE TO SECOND | 0:02.999999 | TRY_CAST | true | true |
      | DAY | 2 | CAST | false | true |
      | DAY | 2 | TRY_CAST | false | true |
      | HOUR | 2 | CAST | false | true |
      | HOUR | 2 | TRY_CAST | false | true |
      | MINUTE | 2 | CAST | false | true |
      | MINUTE | 2 | TRY_CAST | false | true |
      | SECOND | 2.999999 | CAST | false | true |
      | SECOND | 2.999999 | TRY_CAST | false | true |
      | DAY TO HOUR | 0 02 | CAST | false | true |
      | DAY TO HOUR | 0 02 | TRY_CAST | false | true |
      | DAY TO MINUTE | 0 00:02 | CAST | false | true |
      | DAY TO MINUTE | 0 00:02 | TRY_CAST | false | true |
      | DAY TO SECOND | 0 00:00:02.999999 | CAST | false | true |
      | DAY TO SECOND | 0 00:00:02.999999 | TRY_CAST | false | true |
      | HOUR TO MINUTE | 0:02 | CAST | false | true |
      | HOUR TO MINUTE | 0:02 | TRY_CAST | false | true |
      | HOUR TO SECOND | 0:00:02.999999 | CAST | false | true |
      | HOUR TO SECOND | 0:00:02.999999 | TRY_CAST | false | true |
      | MINUTE TO SECOND | 0:02.999999 | CAST | false | true |
      | MINUTE TO SECOND | 0:02.999999 | TRY_CAST | false | true |

  Scenario: Interval integral exact seconds 9007199254.999999 ANSI true
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT CAST(i AS BIGINT) AS result FROM VALUES (INTERVAL '9007199254.999999' SECOND) AS t(i)
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result    |
      |9007199254|

  Scenario: Interval integral exact seconds -9007199254.999999 ANSI true
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT CAST(i AS BIGINT) AS result FROM VALUES (INTERVAL '-9007199254.999999' SECOND) AS t(i)
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result     |
      |-9007199254|

  Scenario: Interval integral exact seconds 9223372036854.775807 ANSI true
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT CAST(i AS BIGINT) AS result FROM VALUES (INTERVAL '9223372036854.775807' SECOND) AS t(i)
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result       |
      |9223372036854|

  Scenario: Interval integral exact seconds -9223372036854.775808 ANSI true
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT CAST(i AS BIGINT) AS result FROM VALUES (INTERVAL '-9223372036854.775808' SECOND) AS t(i)
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result        |
      |-9223372036854|

  Scenario: Interval integral TINYINT boundaries ANSI true
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT CAST(INTERVAL '-128' SECOND AS TINYINT) AS lo, CAST(INTERVAL '127' SECOND AS TINYINT) AS hi, TRY_CAST(INTERVAL '-129' SECOND AS TINYINT) AS underflow, TRY_CAST(INTERVAL '128' SECOND AS TINYINT) AS overflow
      """
    Then query schema
      """
      root
       |-- lo: byte (nullable = false)
       |-- hi: byte (nullable = false)
       |-- underflow: byte (nullable = true)
       |-- overflow: byte (nullable = true)
      """
    Then query result ordered
      |lo  |hi |underflow|overflow|
      |-128|127|NULL     |NULL    |

  Scenario: Interval integral SMALLINT boundaries ANSI true
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT CAST(INTERVAL '-32768' SECOND AS SMALLINT) AS lo, CAST(INTERVAL '32767' SECOND AS SMALLINT) AS hi, TRY_CAST(INTERVAL '-32769' SECOND AS SMALLINT) AS underflow, TRY_CAST(INTERVAL '32768' SECOND AS SMALLINT) AS overflow
      """
    Then query schema
      """
      root
       |-- lo: short (nullable = false)
       |-- hi: short (nullable = false)
       |-- underflow: short (nullable = true)
       |-- overflow: short (nullable = true)
      """
    Then query result ordered
      |lo    |hi   |underflow|overflow|
      |-32768|32767|NULL     |NULL    |

  Scenario: Interval integral INT boundaries ANSI true
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT CAST(INTERVAL '-2147483648' SECOND AS INT) AS lo, CAST(INTERVAL '2147483647' SECOND AS INT) AS hi, TRY_CAST(INTERVAL '-2147483649' SECOND AS INT) AS underflow, TRY_CAST(INTERVAL '2147483648' SECOND AS INT) AS overflow
      """
    Then query schema
      """
      root
       |-- lo: integer (nullable = false)
       |-- hi: integer (nullable = false)
       |-- underflow: integer (nullable = true)
       |-- overflow: integer (nullable = true)
      """
    Then query result ordered
      |lo         |hi        |underflow|overflow|
      |-2147483648|2147483647|NULL     |NULL    |

  Scenario: Interval integral exact seconds 9007199254.999999 ANSI false
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT CAST(i AS BIGINT) AS result FROM VALUES (INTERVAL '9007199254.999999' SECOND) AS t(i)
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result    |
      |9007199254|

  Scenario: Interval integral exact seconds -9007199254.999999 ANSI false
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT CAST(i AS BIGINT) AS result FROM VALUES (INTERVAL '-9007199254.999999' SECOND) AS t(i)
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result     |
      |-9007199254|

  Scenario: Interval integral exact seconds 9223372036854.775807 ANSI false
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT CAST(i AS BIGINT) AS result FROM VALUES (INTERVAL '9223372036854.775807' SECOND) AS t(i)
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result       |
      |9223372036854|

  Scenario: Interval integral exact seconds -9223372036854.775808 ANSI false
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT CAST(i AS BIGINT) AS result FROM VALUES (INTERVAL '-9223372036854.775808' SECOND) AS t(i)
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result        |
      |-9223372036854|

  Scenario: Interval integral TINYINT boundaries ANSI false
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT CAST(INTERVAL '-128' SECOND AS TINYINT) AS lo, CAST(INTERVAL '127' SECOND AS TINYINT) AS hi, TRY_CAST(INTERVAL '-129' SECOND AS TINYINT) AS underflow, TRY_CAST(INTERVAL '128' SECOND AS TINYINT) AS overflow
      """
    Then query schema
      """
      root
       |-- lo: byte (nullable = false)
       |-- hi: byte (nullable = false)
       |-- underflow: byte (nullable = true)
       |-- overflow: byte (nullable = true)
      """
    Then query result ordered
      |lo  |hi |underflow|overflow|
      |-128|127|NULL     |NULL    |

  Scenario: Interval integral SMALLINT boundaries ANSI false
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT CAST(INTERVAL '-32768' SECOND AS SMALLINT) AS lo, CAST(INTERVAL '32767' SECOND AS SMALLINT) AS hi, TRY_CAST(INTERVAL '-32769' SECOND AS SMALLINT) AS underflow, TRY_CAST(INTERVAL '32768' SECOND AS SMALLINT) AS overflow
      """
    Then query schema
      """
      root
       |-- lo: short (nullable = false)
       |-- hi: short (nullable = false)
       |-- underflow: short (nullable = true)
       |-- overflow: short (nullable = true)
      """
    Then query result ordered
      |lo    |hi   |underflow|overflow|
      |-32768|32767|NULL     |NULL    |

  Scenario: Interval integral INT boundaries ANSI false
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT CAST(INTERVAL '-2147483648' SECOND AS INT) AS lo, CAST(INTERVAL '2147483647' SECOND AS INT) AS hi, TRY_CAST(INTERVAL '-2147483649' SECOND AS INT) AS underflow, TRY_CAST(INTERVAL '2147483648' SECOND AS INT) AS overflow
      """
    Then query schema
      """
      root
       |-- lo: integer (nullable = false)
       |-- hi: integer (nullable = false)
       |-- underflow: integer (nullable = true)
       |-- overflow: integer (nullable = true)
      """
    Then query result ordered
      |lo         |hi        |underflow|overflow|
      |-2147483648|2147483647|NULL     |NULL    |

  Scenario: Interval integral shape JOIN with duplicate interval names
    When query
      """
      SELECT CAST(l.i AS BIGINT) AS days, CAST(r.i AS BIGINT) AS hours FROM VALUES (INTERVAL '2' DAY) AS l(i) CROSS JOIN VALUES (INTERVAL '3' HOUR) AS r(i)
      """
    Then query schema
      """
      root
       |-- days: long (nullable = false)
       |-- hours: long (nullable = false)
      """
    Then query result ordered
      |days|hours|
      |2   |3    |

  Scenario: Interval integral shape struct field
    When query
      """
      SELECT CAST(named_struct('x', INTERVAL '2' DAY).x AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |2     |

  Scenario: Interval integral shape nested struct field
    When query
      """
      SELECT CAST(named_struct('x', named_struct('y', INTERVAL '2' DAY)).x.y AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |2     |

  # Array extraction loses interval metadata/nullability (also on main).
  #
  # The VALUE is now fixed: `resolve_expression_extract_value` (misc.rs) reads the
  # interval field metadata straight off `array()`'s own (unfolded) arguments at
  # resolution time and reattaches it to the extracted `array_element(...)` via a
  # metadata-carrying `Cast`, the same technique `cast.rs` uses for `CAST ... AS
  # INTERVAL`. This does NOT touch `array()`'s own return type.
  #
  # A first attempt DID touch `array()` (SparkArray::return_field_from_args /
  # invoke_with_args, to carry the metadata on the list's own inner Field) and had
  # to be reverted: `INTERVAL '2' DAY` is a compile-time-constant Cast-with-metadata
  # expression, and DataFusion's constant-folding evaluates it down to a bare
  # ScalarValue (no Field, so no metadata) *before* re-invoking `array()`'s
  # `return_field_from_args` on the folded child -- so the type promised during the
  # original (unfolded) planning pass no longer matched what the fold step actually
  # returned, and Sail's own physical-plan assertion (`result_data_type ==
  # *expected_type`) tripped at runtime. Do not reattempt that approach without
  # first making the metadata survive constant folding.
  #
  # TODO: nullability is still wrong (`true` instead of `false`), but this is NOT
  # interval-specific: `array(1,2,3)[0]` (no intervals at all) is *also*
  # incorrectly `nullable=true` in Sail. Fixing it means proving, in the same
  # `DataType::List(...)` branch, that the array expression is non-nullable, its
  # element field is non-nullable, and a literal index is in bounds -- and doing
  # that generally, not just for this scenario, so it needs its own scoped pass
  # rather than a change bundled into this fix.
  # DAY, HOUR and MINUTE together are discriminating: a lookup that silently
  # fails and falls back to dividing by SECOND (the bug this fix targets) would
  # give 172800 / 7200 / 5400000000 instead, so a single DAY case alone
  # wouldn't have caught a regression back to that fallback.
  @sail-bug
  Scenario Outline: Interval integral shape array element: <qualifier>
    When query
      """
      SELECT CAST(array(INTERVAL '<value>' <qualifier>)[0] AS BIGINT) AS result
      """
    Then query result ordered
      |result   |
      |<result> |
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """

    Examples:
      | qualifier | value | result |
      | DAY       | 2     | 2      |
      | HOUR      | 2     | 2      |
      | MINUTE    | 90    | 90     |

  Scenario: Interval integral shape CASE with wider right interval
    When query
      """
      SELECT CAST(CASE WHEN true THEN INTERVAL '2' DAY ELSE INTERVAL '3' HOUR END AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |48    |

  Scenario: Interval integral shape CASE with wider left interval
    When query
      """
      SELECT CAST(CASE WHEN false THEN INTERVAL '3' HOUR ELSE INTERVAL '2' DAY END AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
    Then query result ordered
      |result|
      |48    |

  Scenario: Interval integral minimum micros through arithmetic ANSI true
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT CAST(-INTERVAL '9223372036854.775807' SECOND - INTERVAL '0.000001' SECOND AS BIGINT) AS result
      """
    Then query result ordered
      |result        |
      |-9223372036854|

  Scenario: Interval integral minimum micros schema through arithmetic ANSI true
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT CAST(-INTERVAL '9223372036854.775807' SECOND - INTERVAL '0.000001' SECOND AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """

  Scenario: Interval integral minimum micros through arithmetic ANSI false
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT CAST(-INTERVAL '9223372036854.775807' SECOND - INTERVAL '0.000001' SECOND AS BIGINT) AS result
      """
    Then query result ordered
      |result        |
      |-9223372036854|

  Scenario: Interval integral minimum micros schema through arithmetic ANSI false
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT CAST(-INTERVAL '9223372036854.775807' SECOND - INTERVAL '0.000001' SECOND AS BIGINT) AS result
      """
    Then query schema
      """
      root
       |-- result: long (nullable = false)
      """
