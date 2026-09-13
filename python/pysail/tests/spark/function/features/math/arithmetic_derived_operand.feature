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
    # `date - date` yields a day-time interval in both engines now, so an INT added to it is
    # refused by both -- green, and the guard against the type drifting back.
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

    # The other direction, and the worse one: Sail REFUSED what Spark answers. It resolves now --
    # `date - date` yields an interval, so the outer `+` has two intervals to add -- and what is
    # left is the rendering: Sail prints `INTERVAL '16 00:00:00' DAY TO SECOND`, because
    # `Duration(Microsecond)` has one single spelling and the declared field range is not carried.
    # That is the interval metadata work of `fix/interval`, so the row stays tagged for the text.
    @sail-bug
    Scenario: a date difference plus an interval resolves
      When query
        """
        SELECT CAST((DATE'2024-01-15' - DATE'2024-01-01') + INTERVAL '2' DAY AS STRING) AS result
        """
      Then query result
        | result            |
        | INTERVAL '16' DAY |

