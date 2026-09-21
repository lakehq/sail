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
    # difference an INT day count until a `Duration` carries its field range (see the next Rule), so
    # it answers.
    @sail-bug
    Scenario: a date difference plus an INT is refused
      When query
        """
        SELECT (DATE'2024-01-15' - DATE'2024-01-01') + CAST(2 AS INT) AS result
        """
      Then query error (?i)cannot resolve

    # The one left: Sail's inner type is DATE where Spark's is TIMESTAMP, so the outer `+` sees a
    # different operand in each engine and only Spark refuses the INT.
    @sail-bug
    Scenario: a shifted date plus an INT is refused
      When query
        """
        SELECT (DATE'2024-01-15' + INTERVAL '25' HOUR) + CAST(2 AS INT) AS result
        """
      Then query error (?i)cannot resolve

    # TODO: the other direction, and the worse one: Sail REFUSES what Spark answers. `SubtractDates`
    #  yields a `DayTimeIntervalType(DAY)` (`datetimeExpressions.scala:3616`), so the outer operator
    #  has two intervals; Sail's difference is an INT day count, and an INT beside an interval is
    #  refused. Making it a `Duration` would fix the verdict and break the VALUE its consumers read:
    #  `Duration` carries no field range, so `CAST(d1 - d2 AS INT)` would answer seconds instead of
    #  days. Closing it needs the interval field metadata of `fix/interval`, so the two rows below
    #  are pinned rather than traded for a wrong value. This is the only arithmetic pair left where
    #  Sail refuses what Spark accepts.
    @sail-bug
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
    #  range (`fix/interval`): as a `Duration` it was read by seconds and `CAST(date - date AS INT)`
    #  answered 1209600. Each row below was refused on `main` too.
    @sail-bug
    Scenario Outline: a date difference <case>
      When query
        """
        SELECT <expression> AS result
        """
      Then query result
        | result  |
        | <value> |

      Examples:
        | case                        | expression                                                                     | value |
        | compares with an interval   | (DATE'2024-01-15' - DATE'2024-01-01') < INTERVAL '15' DAY                      | true  |
        | extracts its days           | extract(DAY FROM DATE'2024-01-15' - DATE'2024-01-01')                          | 14    |
        | subtracts an hour           | (DATE'2024-01-15' - DATE'2024-01-01') - INTERVAL '1' HOUR IS NOT NULL          | true  |

    # TODO: a day-time interval cast to a number is read by its end field in Spark
    #  (`IntervalUtils.scala:921-928`); a `Duration` carries no field, so Sail reads seconds. Already so
    #  on `main`.
    @sail-bug
    Scenario: a DAY interval cast to INT is its day count
      When query
        """
        SELECT CAST(INTERVAL '14' DAY AS INT) AS result
        """
      Then query result
        | result |
        | 14     |

