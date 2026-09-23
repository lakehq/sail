Feature: a STRING operand of arithmetic, vs Spark 4.2.0

  # Spark promotes a string operand of `+`, `-`, `*` and `%` before the operator sees it, and the two
  # ANSI modes do it DIFFERENTLY, so the same query changes type with the flag:
  #
  #   * ANSI off, `StringPromotionTypeCoercion.scala`: a string beside anything but an interval is
  #     cast to DOUBLE, on its own. `'2' + 1` is `3.0`.
  #   * ANSI on, `AnsiStringPromotionTypeCoercion.findWiderTypeForString`: BOTH sides go to BIGINT
  #     beside an integral and to DOUBLE beside a fractional. `'2' + 1` is `3`, and `'2.5' + 1`
  #     raises, because `'2.5'` is not a BIGINT.
  #
  # Sail handed DataFusion the `Utf8` operand as it was, which it cannot coerce, so every one of these
  # was REFUSED -- 159 rows of the arithmetic matrix. Every row below was measured on the JVM first.

  Rule: a string beside a number, another string or NULL is promoted to a number

    Scenario Outline: string promotion: <case> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(<expression>) AS t, CAST(<expression> AS STRING) AS v
        """
      Then query result
        | t      | v       |
        | <type> | <value> |

      Examples:
        | case            | ansi  | expression                       | type   | value |
        | str + int       | false | '2' + CAST(1 AS INT)             | double | 3.0   |
        | int + str       | false | CAST(1 AS INT) + '2'             | double | 3.0   |
        | str + tinyint   | false | '2' + CAST(1 AS TINYINT)         | double | 3.0   |
        | str + bigint    | false | '2' + CAST(1 AS BIGINT)          | double | 3.0   |
        | str + float     | false | '2' + CAST(1 AS FLOAT)           | double | 3.0   |
        | str + double    | false | '2' + CAST(1 AS DOUBLE)          | double | 3.0   |
        | str + decimal   | false | '2' + CAST(1 AS DECIMAL(10,2))   | double | 3.0   |
        | str - int       | false | '5' - CAST(1 AS INT)             | double | 4.0   |
        | str * int       | false | '3' * CAST(2 AS INT)             | double | 6.0   |
        | str % int       | false | '7' % CAST(3 AS INT)             | double | 1.0   |
        | int % str       | false | CAST(7 AS INT) % '3'             | double | 1.0   |
        | str / int       | false | '7' / CAST(2 AS INT)             | double | 3.5   |
        | frac str + int  | false | '2.5' + CAST(1 AS INT)           | double | 3.5   |
        | frac str + dec  | false | '2.5' + CAST(1 AS DECIMAL(10,2)) | double | 3.5   |
        | frac str * int  | false | '2.5' * CAST(2 AS INT)           | double | 5.0   |
        | bad str + int   | false | 'abc' + CAST(1 AS INT)           | double | NULL  |
        | bad str + dbl   | false | 'abc' + CAST(1 AS DOUBLE)        | double | NULL  |
        | space str + int | false | ' 2 ' + CAST(1 AS INT)           | double | 3.0   |
        | str + str       | false | '1' + '2'                        | double | 3.0   |
        | str * str       | false | '3' * '2'                        | double | 6.0   |
        | str % str       | false | '7' % '3'                        | double | 1.0   |
        | str + null      | false | '2' + NULL                       | double | NULL  |
        | str + int       | true  | '2' + CAST(1 AS INT)             | bigint | 3     |
        | int + str       | true  | CAST(1 AS INT) + '2'             | bigint | 3     |
        | str + tinyint   | true  | '2' + CAST(1 AS TINYINT)         | bigint | 3     |
        | str + bigint    | true  | '2' + CAST(1 AS BIGINT)          | bigint | 3     |
        | str + float     | true  | '2' + CAST(1 AS FLOAT)           | double | 3.0   |
        | str + double    | true  | '2' + CAST(1 AS DOUBLE)          | double | 3.0   |
        | str + decimal   | true  | '2' + CAST(1 AS DECIMAL(10,2))   | double | 3.0   |
        | str - int       | true  | '5' - CAST(1 AS INT)             | bigint | 4     |
        | str * int       | true  | '3' * CAST(2 AS INT)             | bigint | 6     |
        | str % int       | true  | '7' % CAST(3 AS INT)             | bigint | 1     |
        | int % str       | true  | CAST(7 AS INT) % '3'             | bigint | 1     |
        | str / int       | true  | '7' / CAST(2 AS INT)             | double | 3.5   |
        | frac str + dec  | true  | '2.5' + CAST(1 AS DECIMAL(10,2)) | double | 3.5   |
        | space str + int | true  | ' 2 ' + CAST(1 AS INT)           | bigint | 3     |

  Rule: the cast follows the mode -- a string that is not the target number raises only with ANSI on

    # With ANSI off a malformed string is NULL; with it on the SAME string raises, and a fraction
    # raises too when the partner is integral, because the target is BIGINT.
    Scenario Outline: a string that is not a number: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expression> AS v
        """
      Then query error (?i)cannot (be )?cast

      Examples:
        | case           | expression                |
        | frac str + int | '2.5' + CAST(1 AS INT)    |
        | frac str * int | '2.5' * CAST(2 AS INT)    |
        | bad str + int  | 'abc' + CAST(1 AS INT)    |
        | bad str + dbl  | 'abc' + CAST(1 AS DOUBLE) |

  Rule: a string subtracted with a datetime is read as that datetime

    # Only for `-`, and not symmetric: with ANSI off only `string - date` resolves (through
    # `SubtractDates`, whose implicit cast reads the string as a DATE); with ANSI on the string
    # becomes whichever DATE, TIMESTAMP or TIME stands beside it, in either order.
    Scenario Outline: a string as a datetime: <case> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(<expression>) AS t, CAST(<expression> AS STRING) AS v
        """
      Then query result
        | t      | v       |
        | <type> | <value> |

      Examples:
        | case         | ansi | expression                                                 | type                   | value                               |
        | str - ts     | true | '2024-01-15 06:00:00' - TIMESTAMP'2024-01-15 00:00:00'     | interval day to second | INTERVAL '0 06:00:00' DAY TO SECOND |
        | ts - str     | true | TIMESTAMP'2024-01-15 06:00:00' - '2024-01-15 00:00:00'     | interval day to second | INTERVAL '0 06:00:00' DAY TO SECOND |
        | str - ts_ntz | true | '2024-01-15 06:00:00' - TIMESTAMP_NTZ'2024-01-15 00:00:00' | interval day to second | INTERVAL '0 06:00:00' DAY TO SECOND |

    # The class matches; the declared field range does not -- Sail spells every day-time interval
    # DAY TO SECOND, because an Arrow `Duration` carries no fields. Same root as `date - date`
    # itself, and it goes with PR #2350.
    # TODO: the promoted datetime keeps Spark's field range only once an interval carries it
    #   (PR #2350); the verdict is already Spark's.
    @sail-bug
    Scenario Outline: a string as a datetime keeps Spark's field range: <case> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(<expression>) AS t
        """
      Then query result
        | t      |
        | <type> |

      Examples:
        | case       | ansi  | expression                      | type                    |
        | str - date | false | '2024-01-15' - DATE'2024-01-01' | interval day            |
        | str - date | true  | '2024-01-15' - DATE'2024-01-01' | interval day            |
        | date - str | true  | DATE'2024-01-15' - '2024-01-01' | interval day            |
        | str - time | true  | '06:00:00' - TIME'01:00:00'     | interval hour to second |
        | time - str | true  | TIME'06:00:00' - '01:00:00'     | interval hour to second |

  Rule: with ANSI off a string operand of any shape is read as a DATE beside one

    # Spark reads `string - date` through `SubtractDates` with an implicit cast of the string to a DATE
    # (`BinaryArithmeticWithDatetimeResolver.scala:142`), whatever expression the string comes from.
    # TODO: when that expression still has its own arguments to cast (`coalesce(NULL, '...')`,
    #   `concat('...', 16)`), the SQL analyzer promotes it to DOUBLE first and refuses the pair, while
    #   the DataFrame API and `element_at` resolve it -- a rule-ordering accident Sail does not model,
    #   so Sail resolves all of them rather than refuse a query Spark answers. `md5('x')` and
    #   `base64('x')` are the same case: their argument takes an implicit cast to BINARY, while
    #   `hex('x')` and `upper(...)` take a STRING as it is and resolve in both engines.
    @sail-bug
    Scenario Outline: <operand> minus a date is refused with ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <operand> - DATE'2024-01-15' AS v
        """
      Then query error (?i)cannot resolve

      Examples:
        | operand                                           |
        | coalesce(NULL, '2024-01-16')                      |
        | coalesce(DATE'2024-01-15', '2024-01-16')          |
        | nvl(NULL, '2024-01-16')                           |
        | ifnull('2024-01-16', DATE'2024-01-15')            |
        | if(true, '2024-01-16', NULL)                      |
        | CASE WHEN true THEN NULL ELSE '2024-01-16' END    |
        | nvl2(NULL, '2024-01-16', NULL)                    |
        | md5('x')                                          |
        | base64('x')                                       |
        | nullif('2', NULL)                                 |
        | least(NULL, '2024-01-16')                         |
        | greatest('2024-01-16', NULL)                      |
        | concat('2024-01-', 16)                            |

    Scenario Outline: <operand> minus a date resolves with ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT (<operand> - DATE'2024-01-15') IS NOT NULL AS resolved
        FROM (SELECT '2024-01-16' AS s)
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | operand                                         |
        | s                                               |
        | coalesce('2024-01-16', '2024-01-17')            |
        | coalesce(CAST(NULL AS STRING), '2024-01-16')    |
        | coalesce(s, s)                                  |
        | if(true, '2024-01-16', '2024-01-17')            |
        | CASE WHEN true THEN '2024-01-16' END            |
        | nvl2(NULL, '2024-01-16', '2024-01-17')          |
        | upper(coalesce(NULL, '2024-01-16'))             |
        | concat('2024-01-', '16')                        |
        | element_at(array('2024-01-16'), 1)              |
        | try_element_at(array('2024-01-16'), 1)          |

  Rule: a string shifted by an interval is read as a timestamp and written back as a string

    # `Cast(TimestampAddInterval(l, r), l.dataType)` (`BinaryArithmeticWithDatetimeResolver.scala`),
    # with the interval negated for `-`, and for the legacy calendar interval as much as the
    # day-time one. Only `-` needs the string on the left.
    Scenario Outline: a string shifted by an interval: <case> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(<expression>) AS t, CAST(<expression> AS STRING) AS v
        """
      Then query result
        | t      | v       |
        | <type> | <value> |

      Examples:
        | case           | ansi  | expression                                  | type     | value               |
        | str + calendar | false | '2024-01-15' + make_interval(0,0,0,1,0,0,0) | string   | 2024-01-16 00:00:00 |
        | calendar + str | false | make_interval(0,0,0,1,0,0,0) + '2024-01-15' | string   | 2024-01-16 00:00:00 |
        | str - calendar | false | '2024-01-15' - make_interval(0,0,0,1,0,0,0) | string   | 2024-01-14 00:00:00 |
        | str * calendar | false | '2' * make_interval(0,0,0,1,0,0,0)          | interval | 2 days              |
        | str - ival_d   | false | '2024-01-15' - INTERVAL '1' DAY             | string   | 2024-01-14 00:00:00 |
        | str + ival_d   | false | '2024-01-15' + INTERVAL '1' DAY             | string   | 2024-01-16 00:00:00 |
        | str + calendar | true  | '2024-01-15' + make_interval(0,0,0,1,0,0,0) | string   | 2024-01-16 00:00:00 |
        | calendar + str | true  | make_interval(0,0,0,1,0,0,0) + '2024-01-15' | string   | 2024-01-16 00:00:00 |
        | str - calendar | true  | '2024-01-15' - make_interval(0,0,0,1,0,0,0) | string   | 2024-01-14 00:00:00 |
        | str * calendar | true  | '2' * make_interval(0,0,0,1,0,0,0)          | interval | 2 days              |
        | str - ival_d   | true  | '2024-01-15' - INTERVAL '1' DAY             | string   | 2024-01-14 00:00:00 |
        | str + ival_d   | true  | '2024-01-15' + INTERVAL '1' DAY             | string   | 2024-01-16 00:00:00 |

  Rule: the pairs Spark still refuses stay refused

    # Promotion is not "anything goes": with ANSI on two strings, or a string and a NULL, have no
    # number to anchor the cast and are rejected; with ANSI off a string beside a TIMESTAMP or a
    # TIME is cast to DOUBLE and then rejected. These rows lock that half of the contract.
    Scenario Outline: a string pair Spark refuses: <case> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT <expression> AS v
        """
      Then query error (?i)cannot resolve

      Examples:
        | case         | ansi  | expression                                                 |
        | date - str   | false | DATE'2024-01-15' - '2024-01-01'                            |
        | str - ts     | false | '2024-01-15 06:00:00' - TIMESTAMP'2024-01-15 00:00:00'     |
        | ts - str     | false | TIMESTAMP'2024-01-15 06:00:00' - '2024-01-15 00:00:00'     |
        | str - ts_ntz | false | '2024-01-15 06:00:00' - TIMESTAMP_NTZ'2024-01-15 00:00:00' |
        | str - time   | false | '06:00:00' - TIME'01:00:00'                                |
        | time - str   | false | TIME'06:00:00' - '01:00:00'                                |
        | str + str    | true  | '1' + '2'                                                  |
        | str * str    | true  | '3' * '2'                                                  |
        | str % str    | true  | '7' % '3'                                                  |
        | str + null   | true  | '2' + NULL                                                 |

  Rule: a string divided by a number, or a number by a string, is promoted the same way

    # `/` is a `BinaryArithmetic`, so the same two rules apply. Its generic branch already cast a
    # string to DOUBLE, which hid the gap for most pairs -- but not beside a DECIMAL, not for a
    # malformed string with ANSI off, and not for `'2.5' / 2` with ANSI on, which Spark REFUSES
    # (the string goes to BIGINT) and Sail used to answer `1.25`.
    Scenario Outline: a string in a division: <case> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(<expression>) AS t, CAST(<expression> AS STRING) AS v
        """
      Then query result
        | t      | v       |
        | <type> | <value> |

      Examples:
        | case           | ansi  | expression                       | type   | value |
        | str / dec      | false | '2' / CAST(2 AS DECIMAL(10,2))   | double | 1.0   |
        | dec / str      | false | CAST(2 AS DECIMAL(10,2)) / '2'   | double | 1.0   |
        | str / int      | false | '7' / CAST(2 AS INT)             | double | 3.5   |
        | int / str      | false | CAST(7 AS INT) / '2'             | double | 3.5   |
        | frac str / int | false | '2.5' / CAST(2 AS INT)           | double | 1.25  |
        | frac str / dec | false | '2.5' / CAST(2 AS DECIMAL(10,2)) | double | 1.25  |
        | bad str / int  | false | 'abc' / CAST(2 AS INT)           | double | NULL  |
        | str / dbl      | false | '7' / CAST(2 AS DOUBLE)          | double | 3.5   |
        | str / str      | false | '7' / '2'                        | double | 3.5   |
        | str / dec      | true  | '2' / CAST(2 AS DECIMAL(10,2))   | double | 1.0   |
        | dec / str      | true  | CAST(2 AS DECIMAL(10,2)) / '2'   | double | 1.0   |
        | str / int      | true  | '7' / CAST(2 AS INT)             | double | 3.5   |
        | int / str      | true  | CAST(7 AS INT) / '2'             | double | 3.5   |
        | frac str / dec | true  | '2.5' / CAST(2 AS DECIMAL(10,2)) | double | 1.25  |
        | str / dbl      | true  | '7' / CAST(2 AS DOUBLE)          | double | 3.5   |

    Scenario Outline: a string in a division that is not a number raises with ANSI on: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expression> AS v
        """
      Then query error (?i)cannot (be )?cast

      Examples:
        | case           | expression             |
        | frac str / int | '2.5' / CAST(2 AS INT) |
        | bad str / int  | 'abc' / CAST(2 AS INT) |

  Rule: a string that scales an interval is read as a DOUBLE the way the mode reads it

    # `MultiplyDTInterval`, `MultiplyYMInterval` and their divisions take `NumericType`
    # (`intervalExpressions.scala:605,658,745,828`), `MultiplyInterval` and `DivideInterval` take
    # `DoubleType` (`:181`), through implicit casts, so a malformed string is NULL with ANSI off.
    Scenario Outline: an interval scaled by a malformed string is NULL with ANSI off: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(<expression> AS STRING) AS v
        """
      Then query result
        | v    |
        | NULL |

      Examples:
        | case           | expression                               |
        | day-time * str | INTERVAL '1' DAY * 'x'                   |
        | str * day-time | 'x' * INTERVAL '1' DAY                   |
        | day-time / str | INTERVAL '1' DAY / 'x'                   |
        | ym * str       | INTERVAL '1' MONTH * 'x'                 |
        | ym / str       | INTERVAL '1' MONTH / 'x'                 |
        | calendar * str | make_interval(0, 1, 0, 1, 0, 0, 0) * 'x' |
        | calendar / str | make_interval(0, 1, 0, 1, 0, 0, 0) / 'x' |
