Feature: arithmetic operands whose type is derived, vs Spark 4.2.0

  # The blind spot of `arithmetic_operand_rejection.feature`. That matrix enumerates 28 tokens,
  # and every one of its 12088 operands is a literal or a type constructor, so BOTH ENGINES
  # ALWAYS LAND IN THE SAME CELL. When the operand is an expression whose result type differs
  # between the engines, the same query lands in a different cell in each, and no cell of that
  # matrix can see it -- which is how `DATE + datediff(...)` was refused for a while.
  #
  # Found by brute force, 2026-09-12: the 910 executable examples in the function catalogue were
  # run through `typeof` on both engines (55 differed then, 51 now that `datediff`, `date_diff`
  # and `date - date` carry Spark's types), and the divergent expressions were then
  # combined with a DATE and an INT operand under all five operators (550 cells, 38 verdicts
  # differ). One scenario per root cause. Each row was measured on Spark first and then on Sail.

  Rule: a date offset that comes out of a function resolves

    # The regression this file exists for. The date offset guard is `DateAdd`'s own INT accept set,
    # so a function typed BIGINT where Spark types it INT would make Sail refuse a query Spark
    # answers once the value crosses a projection boundary. Green on both engines; it is the guard
    # against `regexp_count` or `datediff` drifting back to BIGINT.
    Scenario Outline: a date shifted by <case> resolves
      When query
        """
        SELECT CAST(DATE'2024-01-15' + <offset> AS STRING) AS result
        """
      Then query result
        | result     |
        | <expected> |

      Examples:
        | case              | offset                                       | expected   |
        | a regexp count    | regexp_count('aaa', 'a')                     | 2024-01-18 |
        | a date difference | datediff(DATE'2024-01-20', DATE'2024-01-15') | 2024-01-20 |

  Rule: a BINARY-returning function is a BINARY operand

    # Spark types `substr`/`substring`/`left`/`overlay` over a BINARY input as BINARY, and BINARY
    # is not an arithmetic operand, so it refuses at ANALYSIS. Sail used to cast the input to a
    # STRING, which is one: the query failed at runtime with ANSI on and ANSWERED `NULL` with it off,
    # once string promotion read the string with `try_cast`. Both modes, for that reason.
    # Sail reads that input as a STRING for now (see `binary_substring.feature`), so the operand is
    # recognised by shape and refused like Spark's BINARY.
    Scenario Outline: <case> is refused as an arithmetic operand with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(2 AS INT) <op> <operand> AS result
        """
      Then query error (?i)cannot resolve

      Examples:
        | case                | ansi  | op | operand                                                                  |
        | substr of a binary  | false | /  | substr(encode('Spark SQL', 'utf-8'), 5)                                  |
        | substr of a binary  | false | %  | substr(encode('Spark SQL', 'utf-8'), 5)                                  |
        | substring of binary | false | /  | substring(encode('Spark SQL', 'utf-8'), 5)                               |
        | left of a binary    | false | /  | left(encode('Spark SQL', 'utf-8'), 3)                                    |
        | overlay of a binary | false | /  | overlay(encode('Spark SQL', 'utf-8') PLACING encode('_','utf-8') FROM 6) |
        | substr of a binary  | true  | /  | substr(encode('Spark SQL', 'utf-8'), 5)                                  |
        | substr of a binary  | true  | %  | substr(encode('Spark SQL', 'utf-8'), 5)                                  |
        | substring of binary | true  | /  | substring(encode('Spark SQL', 'utf-8'), 5)                               |
        | left of a binary    | true  | /  | left(encode('Spark SQL', 'utf-8'), 3)                                    |
        | overlay of a binary | true  | /  | overlay(encode('Spark SQL', 'utf-8') PLACING encode('_','utf-8') FROM 6) |
        | substr of a binary  | false | +  | substr(encode('Spark SQL', 'utf-8'), 5)                                  |
        | left of a binary    | true  | *  | left(encode('Spark SQL', 'utf-8'), 3)                                    |
        | substring of binary | false | -  | substring(encode('Spark SQL', 'utf-8'), 5)                               |

    Scenario Outline: unary minus over <case> is refused with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT -<operand> AS result
        """
      Then query error (?i)cannot resolve

      Examples:
        | case               | ansi  | operand                                 |
        | substr of a binary | false | substr(encode('Spark SQL', 'utf-8'), 5) |
        | left of a binary   | true  | left(encode('Spark SQL', 'utf-8'), 3)   |

    # `negative(x)` is `UnaryMinus` too (`FunctionRegistry.scala:467`), and it is what PySpark's
    # `-col` calls, so it refuses the same operands as `-x`.
    Scenario Outline: negative over <case> is refused with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT negative(<operand>) AS result
        """
      Then query error (?i)cannot resolve

      Examples:
        | case               | ansi  | operand                                 |
        | substr of a binary | false | substr(encode('Spark SQL', 'utf-8'), 5) |
        | left of a binary   | true  | left(encode('Spark SQL', 'utf-8'), 3)   |
        | a date             | false | DATE'2024-01-15'                        |
        | a boolean          | true  | true                                    |

    # A cast the user writes around the input or the result of a binary `substr`/`left`/`overlay`
    # yields a STRING (`Substring.dataType = str.dataType`, `stringExpressions.scala:2309`), which
    # string promotion makes a number. Only the casts Sail inserts itself mark the BINARY shape.
    Scenario Outline: a STRING cast around <case> is an arithmetic operand with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT (<expression>) = <value> AS ok
        """
      Then query result
        | ok   |
        | true |

      Examples:
        | case                 | ansi  | expression                                              | value |
        | a substr result      | false | CAST(substr(X'3132', 1, 2) AS STRING) / 2               | 6     |
        | a substr result      | true  | CAST(substr(X'3132', 1, 2) AS STRING) / 2               | 6     |
        | a substr input       | false | substr(CAST(X'3132' AS STRING), 1, 2) + 1               | 13    |
        | a substring input    | true  | substring(CAST(X'3132' AS STRING), 1, 2) * 2            | 24    |
        | a left result        | false | CAST(left(X'3132', 1) AS STRING) * 2                    | 2     |
        | a left input         | true  | left(CAST(X'3132' AS STRING), 2) % 5                    | 2     |
        | an overlay input     | false | overlay(CAST(X'3132' AS STRING) PLACING '9' FROM 1) - 2 | 90    |
        | a substr try_cast    | true  | TRY_CAST(substr(X'3132', 1, 2) AS STRING) / 4           | 3     |
        | a unary minus result | false | -CAST(substr(X'3132', 1, 2) AS STRING)                  | -12   |
        | a unary plus input   | true  | +substr(CAST(X'3132' AS STRING), 1, 2)                  | 12    |
        | a negative result    | false | negative(CAST(left(X'3132', 2) AS STRING))              | -12   |

    Scenario Outline: a BINARY cast around <case> is still refused with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expression> AS result
        """
      Then query error (?i)cannot resolve

      Examples:
        | case              | ansi  | expression                              |
        | a substr result   | false | CAST(substr(X'3132', 1) AS BINARY) + 1  |
        | a divisor         | true  | 2 / substr(X'3132', 1)                  |
        | a left under a -  | false | -left(X'3132', 1)                       |

  Rule: a function returning an ARRAY is an ARRAY operand

    # `nvl`/`ifnull` are `Coalesce(Seq(left, right))` in Spark (`nullExpressions.scala:246`), so a
    # container stays a container and an arithmetic operator refuses it at analysis. DataFusion's
    # `nvl` coerced every ARRAY, MAP and STRUCT to a STRING -- even `nvl(array, array)` -- and a
    # STRING is an arithmetic operand: the query failed at runtime with ANSI on and ANSWERED `NULL`
    # with it off, once string promotion read the string with `try_cast`. Both modes, both names.
    Scenario Outline: an array from <function> is refused as an arithmetic operand with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(2 AS INT) / <function>(NULL, array('2')) AS result
        """
      Then query error (?i)cannot resolve

      Examples:
        | function | ansi  |
        | nvl      | false |
        | nvl      | true  |
        | ifnull   | false |
        | ifnull   | true  |

    # The root, asserted directly: the container keeps its type through `nvl`, with a NULL or not.
    Scenario Outline: <function> keeps a container's type: <case>
      When query
        """
        SELECT typeof(<function>(<left>, <right>)) AS t
        """
      Then query result
        | t      |
        | <type> |

      Examples:
        | function | case          | left                   | right                  | type               |
        | nvl      | NULL, array   | NULL                   | array('2')             | array<string>      |
        | nvl      | array, array  | array('1')             | array('2')             | array<string>      |
        | nvl      | map, map      | map('a', 1)            | map('b', 2)            | map<string,int>    |
        | nvl      | struct        | named_struct('a', 1)   | named_struct('a', 2)   | struct<a:int>      |
        | ifnull   | NULL, array   | NULL                   | array('2')             | array<string>      |
        | nvl      | a plain int   | CAST(NULL AS INT)      | 2                      | int                |

  Rule: make_date with a NULL argument is still a DATE

    # `MakeDate.dataType` is `DateType` even when an argument is NULL, so Spark refuses the
    # multiplication. Sail used to type the result VOID, so the guard never saw a date and the
    # query answered `NULL`.
    Scenario: a date built with a NULL argument is refused as an arithmetic operand
      When query
        """
        SELECT CAST(2 AS INT) * make_date(2019, 7, NULL) AS result
        """
      Then query error (?i)cannot resolve

  Rule: the result type of a function decides the cell it lands in

    # The root of every row above, asserted directly, in both directions: a type too wide refuses
    # an offset Spark takes (`regexp_instr`), a type too narrow takes one Spark refuses
    # (`bitmap_bit_position`, a BIGINT in Spark).
    Scenario Outline: <case>
      When query
        """
        SELECT typeof(<expression>) AS result
        """
      Then query result
        | result |
        | <type> |

      Examples:
        | case                            | expression               | type   |
        | regexp_instr returns an INT     | regexp_instr('abc', 'b') | int    |
        | bitmap_bit_position is a BIGINT | bitmap_bit_position(1)   | bigint |

    Scenario: a date shifted by a BIGINT-typed function is refused
      When query
        """
        SELECT DATE'2024-01-15' + bitmap_bit_position(1) AS result
        """
      Then query error (?i)cannot resolve

  Rule: a composed operand lands in the cell its own type names

    # Composition needs no matrix of its own: measured over 120 composed cells, a divergence
    # appears ONLY where the inner expression's result type already diverges (60 cells whose inner
    # type agrees produced 0), and not even always -- `float + decimal` differs in type yet stays
    # numeric, so the verdict holds. So the risk reduces to the result-type divergences already
    # pinned in `arithmetic_result_type.feature`; these three are that risk made observable.
    #
    # Spark's `date - date` is a day-time interval, so an INT added to it is refused. Sail keeps the
    # difference an INT day count (see the next Rules), and refuses it by the interval it stands for.
    Scenario: a date difference plus an INT is refused
      When query
        """
        SELECT (DATE'2024-01-15' - DATE'2024-01-01') + CAST(2 AS INT) AS result
        """
      Then query error (?i)cannot resolve

    # BinaryArithmeticWithDatetimeResolver.scala:68-71 keeps DATE only for DAY-only
    # intervals; HOUR promotes to TIMESTAMP, whose addition with INT is rejected.
    Scenario: a shifted date plus an INT is refused
      When query
        """
        SELECT (DATE'2024-01-15' + INTERVAL '25' HOUR) + CAST(2 AS INT) AS result
        """
      Then query error (?i)cannot resolve

    # With ANSI off, STRING minus the promoted TIMESTAMP must be rejected too.
    Scenario: a string minus a shifted date is refused with ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT '2' - (DATE'2024-01-15' + INTERVAL '25' HOUR) AS result
        """
      Then query error (?i)cannot resolve

    # TODO: the other direction, and the worse one: Sail REFUSES what Spark answers. `SubtractDates`
    #  yields a `DayTimeIntervalType(DAY)` (`datetimeExpressions.scala:3616`), so the outer operator
    #  has two intervals; Sail's difference is an INT day count, and an INT beside an interval is
    #  refused. Making it a `Duration` would fix the verdict and break the VALUE its consumers read:
    #  `Duration` carries no field range, so `CAST(d1 - d2 AS INT)` would answer seconds instead of
    #  days. Closing it needs the interval field metadata of PR #2350, so the two rows below
    #  are pinned rather than traded for a wrong value. They stand for the whole family: a date
    #  difference (or `NULL - date`) plus or minus a DAY/HOUR/DAY TO SECOND interval, a TIMESTAMP,
    #  a TIMESTAMP_NTZ or a TIME, on either side and in both ANSI modes, is refused where Spark
    #  answers (42 cells measured by the derived-operand lens of the arithmetic branch map).
    Scenario Outline: <case> resolves
      When query
        """
        SELECT CAST(<expression> AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                              | expression                                                        | result                          |
        | a date difference plus an interval | (DATE'2024-01-15' - DATE'2024-01-01') + INTERVAL '2' DAY          | INTERVAL '16' DAY               |
        | a timestamp plus a date difference | TIMESTAMP'2024-01-01 00:00:00' + (DATE'2024-01-15' - DATE'2024-01-01') | 2024-01-15 00:00:00        |

  Rule: the remainder of two untyped NULLs is a DOUBLE offset

    # Remainder's untyped NULL operands become DOUBLE NULLs through
    # TypeCoercionHelper.scala:571-578. DateAdd accepts only integral offsets.
    Scenario Outline: a date shifted by the remainder of two untyped NULLs is refused: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expression> AS result
        """
      Then query error (?i)cannot resolve

      Examples:
        | case            | expression                        |
        | offset on the right | DATE'2024-01-15' + (NULL % NULL) |
        | offset on the left  | (NULL % NULL) + DATE'2024-01-15' |

  Rule: a date difference is refused wherever Spark refuses its INTERVAL DAY

    # Spark types `date - date` (and `NULL - date`) as `DayTimeIntervalType(DAY)`
    # (`datetimeExpressions.scala:3616`); Sail keeps the day count as an INT (next Rule). The
    # difference carries that identity in the field metadata of the cast that builds it, and the
    # arithmetic guards reason on Spark's type to REFUSE: if an INTERVAL DAY in its place is refused,
    # the difference is refused. They never accept on that basis, so each twin below, a real INT,
    # keeps resolving.
    Scenario Outline: a date difference <expression> is refused with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expression> AS result
        """
      Then query error (?i)cannot resolve

      Examples:
        | ansi  | expression                                                                |
        | false | (DATE'2024-01-15' - DATE'2024-01-01') - '2'                               |
        | true  | (DATE'2024-01-15' - DATE'2024-01-01') - '2'                               |
        | false | '2' % (DATE'2024-01-15' - DATE'2024-01-01')                               |
        | true  | '2' % (DATE'2024-01-15' - DATE'2024-01-01')                               |
        | false | (DATE'2024-01-15' - DATE'2024-01-01') * INTERVAL '1' MONTH                |
        | true  | (DATE'2024-01-15' - DATE'2024-01-01') * INTERVAL '1' MONTH                |
        | false | INTERVAL '1' YEAR / (DATE'2024-01-15' - DATE'2024-01-01')                 |
        | true  | INTERVAL '1' YEAR / (DATE'2024-01-15' - DATE'2024-01-01')                 |
        | false | (NULL - DATE'2024-01-01') - '2'                                           |
        | true  | (NULL - DATE'2024-01-01') - '2'                                           |
        | false | (DATE'2024-01-15' - DATE'2024-01-01') + 1                                 |
        | true  | (DATE'2024-01-15' - DATE'2024-01-01') % 2                                 |

    Scenario Outline: a real INT beside the same operator resolves: <expression> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(<expression>) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | ansi  | expression                                                                |
        | false | (CAST(DATE'2024-01-15' AS INT) - CAST(DATE'2024-01-01' AS INT)) - '2'     |
        | false | datediff(DATE'2024-01-15', DATE'2024-01-01') - '2'                        |
        | true  | datediff(DATE'2024-01-15', DATE'2024-01-01') - '2'                        |
        | false | datediff(DATE'2024-01-15', DATE'2024-01-01') * INTERVAL '1' MONTH         |
        | true  | datediff(DATE'2024-01-15', DATE'2024-01-01') * INTERVAL '1' MONTH         |
        | false | CAST(DATE'2024-01-15' - DATE'2024-01-01' AS INT) - '2'                    |
        | true  | CAST(DATE'2024-01-15' - DATE'2024-01-01' AS INT) - '2'                    |
        | false | DATE'2024-01-15' + (DATE'2024-01-15' - DATE'2024-01-01')                  |
        | true  | (DATE'2024-01-15' - DATE'2024-01-01') * 2                                 |
        | false | (DATE'2024-01-15' - DATE'2024-01-01') / 2                                 |
        | true  | -(DATE'2024-01-15' - DATE'2024-01-01')                                    |

    # SubtractDates retains INTERVAL DAY through projections. An ordinary INT
    # (including datediff) must not inherit the identity from a same-named join column.
    Scenario Outline: projected date difference operands with ANSI <ansi> and caseSensitive <sensitive>: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      Given config spark.sql.caseSensitive = <sensitive>
      When query
        """
        <query>
        """
      Then query error (?i)cannot resolve

      Examples:
        | ansi | sensitive | case | query |
        | false | false | one projection | SELECT x - '2' AS result FROM (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x) |
        | false | false | two projections | SELECT x - '2' AS result FROM (SELECT x FROM (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x)) |
        | false | false | left join operand | SELECT a.x - '2' AS result FROM (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x) a CROSS JOIN (SELECT 8 AS x) b |
        | false | false | right join operand | SELECT b.x - '2' AS result FROM (SELECT 8 AS x) a CROSS JOIN (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x) b |
        | false | true | one projection | SELECT x - '2' AS result FROM (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x) |
        | false | true | two projections | SELECT x - '2' AS result FROM (SELECT x FROM (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x)) |
        | false | true | left join operand | SELECT a.x - '2' AS result FROM (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x) a CROSS JOIN (SELECT 8 AS x) b |
        | false | true | right join operand | SELECT b.x - '2' AS result FROM (SELECT 8 AS x) a CROSS JOIN (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x) b |
        | true | false | one projection | SELECT x - '2' AS result FROM (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x) |
        | true | false | two projections | SELECT x - '2' AS result FROM (SELECT x FROM (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x)) |
        | true | false | left join operand | SELECT a.x - '2' AS result FROM (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x) a CROSS JOIN (SELECT 8 AS x) b |
        | true | false | right join operand | SELECT b.x - '2' AS result FROM (SELECT 8 AS x) a CROSS JOIN (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x) b |
        | true | true | one projection | SELECT x - '2' AS result FROM (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x) |
        | true | true | two projections | SELECT x - '2' AS result FROM (SELECT x FROM (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x)) |
        | true | true | left join operand | SELECT a.x - '2' AS result FROM (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x) a CROSS JOIN (SELECT 8 AS x) b |
        | true | true | right join operand | SELECT b.x - '2' AS result FROM (SELECT 8 AS x) a CROSS JOIN (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x) b |

    Scenario Outline: projected integer controls with ANSI <ansi> and caseSensitive <sensitive>: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      Given config spark.sql.caseSensitive = <sensitive>
      When query
        """
        <query>
        """
      Then query result
        | result |
        | <value> |

      Examples:
        | ansi | sensitive | case | query | value |
        | false | false | integer join control | SELECT a.x - '2' AS result FROM (SELECT 8 AS x) a CROSS JOIN (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x) b | 6.0 |
        | false | false | datediff control | SELECT x - '2' AS result FROM (SELECT datediff(DATE'2024-01-15', DATE'2024-01-01') AS x) | 12.0 |
        | false | true | integer join control | SELECT a.x - '2' AS result FROM (SELECT 8 AS x) a CROSS JOIN (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x) b | 6.0 |
        | false | true | datediff control | SELECT x - '2' AS result FROM (SELECT datediff(DATE'2024-01-15', DATE'2024-01-01') AS x) | 12.0 |
        | true | false | integer join control | SELECT a.x - '2' AS result FROM (SELECT 8 AS x) a CROSS JOIN (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x) b | 6 |
        | true | false | datediff control | SELECT x - '2' AS result FROM (SELECT datediff(DATE'2024-01-15', DATE'2024-01-01') AS x) | 12 |
        | true | true | integer join control | SELECT a.x - '2' AS result FROM (SELECT 8 AS x) a CROSS JOIN (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x) b | 6 |
        | true | true | datediff control | SELECT x - '2' AS result FROM (SELECT datediff(DATE'2024-01-15', DATE'2024-01-01') AS x) | 12 |

  Rule: a date difference keeps the day count its consumers read

    # Spark casts a `DayTimeIntervalType(DAY)` to a number by its END field, in days
    # (`IntervalUtils.scala:921-928`). A `Duration` carries no field, and Sail reads one by seconds,
    # so a difference typed `Duration` cast to INT answered 1209600 for 14 days and refused SMALLINT,
    # `hash` and `try_sum`. Each row answered before the difference became a `Duration`.
    Scenario Outline: a date difference read by <case>
      When query
        """
        SELECT <expression> AS result
        """
      Then query result
        | result  |
        | <value> |

      Examples:
        | case                 | expression                                                                | value      |
        | a cast to INT        | CAST(DATE'2024-01-15' - DATE'2024-01-01' AS INT)                          | 14         |
        | a cast to SMALLINT   | CAST(DATE'2024-01-15' - DATE'2024-01-01' AS SMALLINT)                     | 14         |
        | hash                 | hash(DATE'2024-01-15' - DATE'2024-01-01') IS NOT NULL                     | true       |
        | try_sum              | try_sum(DATE'2024-01-15' - DATE'2024-01-01') IS NOT NULL                  | true       |
        | a date shifted by it | CAST(DATE'2024-01-20' + (DATE'2024-01-15' - DATE'2024-01-01') AS STRING)  | 2024-02-03 |

  Rule: what a date difference answers once it carries Spark's interval type

    # TODO: Spark's `date - date` is `DayTimeIntervalType(DAY)`, so it compares, adds and extracts as
    #  an interval. Sail keeps the difference an INT day count until an interval carries its field
    #  range (PR #2350): as a `Duration` it was read by seconds and `CAST(date - date AS INT)`
    #  answered 1209600. Each row below was refused on `main` too.
    Scenario: a date difference compares with an interval
      When query
        """
        SELECT (DATE'2024-01-15' - DATE'2024-01-01') < INTERVAL '15' DAY AS result
        """
      Then query result
        | result  |
        | true    |

    Scenario: a date difference extracts its days
      When query
        """
        SELECT extract(DAY FROM DATE'2024-01-15' - DATE'2024-01-01') AS result
        """
      Then query result
        | result |
        | 14     |

    Scenario: a date difference subtracts a finer interval
      When query
        """
        SELECT (DATE'2024-01-15' - DATE'2024-01-01') - INTERVAL '1' HOUR IS NOT NULL AS result
        """
      Then query result
        | result |
        | true   |

    # TODO: a day-time interval cast to a number is read by its end field in Spark
    #  (`IntervalUtils.scala:921-928`); a `Duration` carries no field, so Sail reads seconds. Already so
    #  on `main`.
    Scenario: a DAY interval cast to INT is its day count
      When query
        """
        SELECT CAST(INTERVAL '14' DAY AS INT) AS result
        """
      Then query result
        | result |
        | 14     |


  Rule: null remainder rejects nonintegral date offsets

    Scenario Outline: null remainder rejects nonintegral date offsets with ANSI <ansi>: <sql>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        <sql>
        """
      Then query error (?i)cannot resolve

      Examples:
        | ansi | sql |
        | false | SELECT DATE'2024-01-15' + (NULL % NULL) AS result |
        | false | SELECT (NULL % NULL) + DATE'2024-01-15' AS result |
        | false | SELECT DATE'2024-01-15' - (NULL % NULL) AS result |
        | true | SELECT DATE'2024-01-15' + (NULL % NULL) AS result |
        | true | SELECT (NULL % NULL) + DATE'2024-01-15' AS result |
        | true | SELECT DATE'2024-01-15' - (NULL % NULL) AS result |

  Rule: null remainder preserves valid arithmetic

    Scenario Outline: null remainder preserves valid arithmetic with ANSI <ansi>: <sql>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        <sql>
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | ansi | sql |
        | false | SELECT NULL % NULL AS result |
        | false | SELECT (a % b) + 1 AS result FROM (SELECT NULL AS a, NULL AS b) |
        | false | SELECT DATE'2024-01-15' + (CAST(NULL AS INT) % NULL) AS result |
        | true | SELECT NULL % NULL AS result |
        | true | SELECT (a % b) + 1 AS result FROM (SELECT NULL AS a, NULL AS b) |
        | true | SELECT DATE'2024-01-15' + (CAST(NULL AS INT) % NULL) AS result |

  Rule: arithmetic must not make an invalid numeric to TIME cast executable

    # Cast.scala:92-180,223-318 has no numeric-to-TIME conversion. Enabling TIME
    # arithmetic must not turn this rejection on main into an accepted query.
    Scenario Outline: a numeric cast to TIME is refused inside addition with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(1L AS TIME) + INTERVAL '1' HOUR AS result
        """
      Then query error (?i)cannot resolve|DATATYPE_MISMATCH

      Examples:
        | ansi  |
        | false |
        | true  |

  Rule: date shifts preserve DAY and promote finer intervals

    Scenario: date shift type and value from JVM case 2
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(DATE'2024-01-15' + INTERVAL '2' DAY) AS t, CAST(DATE'2024-01-15' + INTERVAL '2' DAY AS STRING) AS v
        """
      Then query result
        |t   |v         |
        |date|2024-01-17|

    Scenario: date shift type and value from JVM case 3
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(DATE'2024-01-15' + INTERVAL '25' HOUR) AS t, CAST(DATE'2024-01-15' + INTERVAL '25' HOUR AS STRING) AS v
        """
      Then query result
        |t        |v                  |
        |timestamp|2024-01-16 01:00:00|

    Scenario: date shift type and value from JVM case 4
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(INTERVAL '25' HOUR + DATE'2024-01-15') AS t, CAST(INTERVAL '25' HOUR + DATE'2024-01-15' AS STRING) AS v
        """
      Then query result
        |t        |v                  |
        |timestamp|2024-01-16 01:00:00|

    Scenario: date shift type and value from JVM case 5
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(DATE'2024-01-15' - INTERVAL '25' HOUR) AS t, CAST(DATE'2024-01-15' - INTERVAL '25' HOUR AS STRING) AS v
        """
      Then query result
        |t        |v                  |
        |timestamp|2024-01-13 23:00:00|

    Scenario: date shift type and value from JVM case 6
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(DATE'2024-01-15' - INTERVAL '2' DAY) AS t, CAST(DATE'2024-01-15' - INTERVAL '2' DAY AS STRING) AS v
        """
      Then query result
        |t   |v         |
        |date|2024-01-13|

    Scenario: date shift type and value from JVM case 9
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof(DATE'2024-01-15' + INTERVAL '2' DAY) AS t, CAST(DATE'2024-01-15' + INTERVAL '2' DAY AS STRING) AS v
        """
      Then query result
        |t   |v         |
        |date|2024-01-17|

    Scenario: date shift type and value from JVM case 10
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof(DATE'2024-01-15' + INTERVAL '25' HOUR) AS t, CAST(DATE'2024-01-15' + INTERVAL '25' HOUR AS STRING) AS v
        """
      Then query result
        |t        |v                  |
        |timestamp|2024-01-16 01:00:00|

    Scenario: date shift type and value from JVM case 11
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '25' HOUR + DATE'2024-01-15') AS t, CAST(INTERVAL '25' HOUR + DATE'2024-01-15' AS STRING) AS v
        """
      Then query result
        |t        |v                  |
        |timestamp|2024-01-16 01:00:00|

    Scenario: date shift type and value from JVM case 12
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof(DATE'2024-01-15' - INTERVAL '25' HOUR) AS t, CAST(DATE'2024-01-15' - INTERVAL '25' HOUR AS STRING) AS v
        """
      Then query result
        |t        |v                  |
        |timestamp|2024-01-13 23:00:00|

    Scenario: date shift type and value from JVM case 13
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof(DATE'2024-01-15' - INTERVAL '2' DAY) AS t, CAST(DATE'2024-01-15' - INTERVAL '2' DAY AS STRING) AS v
        """
      Then query result
        |t   |v         |
        |date|2024-01-13|
