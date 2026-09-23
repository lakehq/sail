Feature: the common type of the branches of a CASE or an IF

  Rule: numeric branches widen the way Spark widens them

    # `CaseWhenCoercion` takes every branch to their common type (`TypeCoercion.scala`), and that
    # type is NOT the plain Arrow union of the two: a fractional beside a DECIMAL is a DOUBLE
    # (`TypeCoercionHelper.scala:194-195`), and an integral beside a FLOAT is a DOUBLE as well,
    # "to avoid potential precision loss" (`AnsiTypeCoercion.scala:113-123`).
    Scenario Outline: <case> is <type>
      When query
        """
        SELECT typeof(<expression>) AS t
        """
      Then query result
        | t      |
        | <type> |

      Examples:
        | case                       | expression                                                         | type          |
        | an int beside a bigint     | CASE WHEN true THEN -2147483648 ELSE 3000000000L END               | bigint        |
        | an int beside a decimal    | CASE WHEN true THEN 1 ELSE CAST(1.5 AS DECIMAL(10,1)) END          | decimal(11,1) |
        | a double beside a decimal  | CASE WHEN true THEN 0.1D ELSE CAST(2.25 AS DECIMAL(10,2)) END      | double        |
        | a float beside a decimal   | CASE WHEN true THEN CAST(0.1 AS FLOAT) ELSE CAST(2.25 AS DECIMAL(10,2)) END | double |
        | an int beside a float      | CASE WHEN true THEN 16777217 ELSE CAST(0.1 AS FLOAT) END           | double        |
        | a bigint beside a float    | if(true, 9007199254740993L, CAST(0.1 AS FLOAT))                    | double        |
        | a float beside a float     | CASE WHEN true THEN CAST(0.1 AS FLOAT) ELSE CAST(0.2 AS FLOAT) END | float         |
        | three branches             | CASE WHEN false THEN 1 WHEN false THEN 2L ELSE CAST(0.1 AS FLOAT) END | double     |

    # An integral that a FLOAT cannot hold is why Spark widens to a DOUBLE: 16777217 is the first
    # integer a FLOAT rounds, and a FLOAT branch would answer 16777216.
    Scenario: the widened branch keeps an integer a float would round
      When query
        """
        SELECT (CASE WHEN true THEN 16777217 ELSE CAST(0.1 AS FLOAT) END) = 16777217.0D AS kept
        """
      Then query result
        | kept |
        | true |

    # A DOUBLE a DECIMAL cannot hold is why the pair is a DOUBLE: as a DECIMAL(30,15) it overflows.
    Scenario: the widened branch keeps a double a decimal would overflow
      When query
        """
        SELECT (CASE WHEN true THEN 1e300D ELSE CAST(2.25 AS DECIMAL(10,2)) END) = 1e300D AS kept
        """
      Then query result
        | kept |
        | true |

  Rule: only ANSI widens an integral beside a FLOAT

    # The "if widerType == FloatType -> DoubleType" clause lives in `AnsiTypeCoercion.scala:117-121`
    # ONLY: the default mode uses `numericPrecedence` (`TypeCoercion.scala:89-92`), which keeps the
    # FLOAT. The fractional-beside-DECIMAL rule (`TypeCoercionHelper.scala:194-195`) is the same in
    # both modes.
    Scenario Outline: <case> is <type> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(<expression>) AS t
        """
      Then query result
        | t      |
        | <type> |

      Examples:
        | case                      | ansi  | expression                                                           | type   |
        | an int beside a float     | false | CASE WHEN true THEN 16777217 ELSE CAST(0.1 AS FLOAT) END             | float  |
        | an int beside a float     | true  | CASE WHEN true THEN 16777217 ELSE CAST(0.1 AS FLOAT) END             | double |
        | a bigint beside a float   | false | if(true, 9007199254740993L, CAST(0.1 AS FLOAT))                      | float  |
        | a bigint beside a float   | true  | if(true, 9007199254740993L, CAST(0.1 AS FLOAT))                      | double |
        | a smallint beside a float | false | if(true, CAST(2 AS SMALLINT), CAST(0.1 AS FLOAT))                    | float  |
        | three branches            | false | CASE WHEN false THEN 1 WHEN false THEN 2L ELSE CAST(0.1 AS FLOAT) END | float  |
        | a float beside a decimal  | false | CASE WHEN true THEN CAST(0.1 AS FLOAT) ELSE CAST(2.25 AS DECIMAL(10,2)) END | double |
        | an int beside a decimal   | false | CASE WHEN true THEN 1 ELSE CAST(1.5 AS DECIMAL(10,1)) END            | decimal(11,1) |

  Rule: branches with no common type are refused

    # TODO: `CaseWhenCoercion` finds no wider type for an INT beside a DATE or an ARRAY, so Spark
    #  refuses with `DATA_DIFF_TYPES` (`TypeCoercion.scala`). Sail resolves the expression and types
    #  it by its first branch. The numeric widening this PR added does not reach these pairs, which
    #  have no numeric common type; they need the rest of `findWiderCommonType`.
    @sail-bug
    Scenario Outline: <case> is refused
      When query
        """
        SELECT <expression> AS v
        """
      Then query error (?i)cannot resolve

      Examples:
        | case                       | expression                                        |
        | a CASE of an INT and a DATE | CASE WHEN true THEN 1 ELSE DATE'2024-01-01' END  |
        | a CASE of an INT and an ARRAY | CASE WHEN true THEN 1 ELSE array(1) END        |
        | an IF of an INT and a DATE  | if(true, 1, DATE'2024-01-01')                     |

  Rule: branches that are structs type only when their field names match

    # `CaseWhenCoercion` and `IfCoercion` take the branches to `findWiderCommonType`, which pairs
    # struct fields through the resolver and gives up when a name does not match or the counts differ
    # (`TypeCoercionHelper.scala:164-176`), so Spark refuses the branch set instead of keeping the
    # first branch's struct. The same rule `nvl` follows in `nvl_container_promotion.feature`.
    Scenario Outline: <case> is refused with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expression> AS v
        """
      Then query error (?i)cannot resolve

      Examples:
        | case                                | ansi  | expression                                                                     |
        | an IF of structs with other names   | false | if(true, named_struct('a', 1), named_struct('b', 2L))                          |
        | an IF of structs with other names   | true  | if(true, named_struct('a', 1), named_struct('b', 2L))                          |
        | a CASE of structs with other names  | false | CASE WHEN true THEN named_struct('a', 1) ELSE named_struct('b', 2L) END        |
        | a CASE with an extra field          | false | CASE WHEN true THEN named_struct('a', 1) ELSE named_struct('a', 2L, 'b', 3) END |
        | an IF of lists of such structs      | false | if(true, array(named_struct('a', 1)), array(named_struct('b', 2L)))            |
        | an IF of a struct and an int        | false | if(true, named_struct('a', 1), 2)                                              |

    # TODO: Spark widens the branches leaf by leaf, so two structs whose names match take the wider
    #  leaf: `if(true, named_struct('a', 1), named_struct('a', 2L))` is `struct<a:bigint>` there and
    #  `struct<a:int>` here, the first-branch typing this file fixes for the numeric branches.
    #  Closing it means widening containers across the branches, as `nvl` already does.
    @sail-bug
    Scenario Outline: <case> keeps the wider leaf
      When query
        """
        SELECT typeof(<expression>) AS t
        """
      Then query result
        | t             |
        | <type>        |

      Examples:
        | case                        | expression                                                              | type            |
        | an IF of widening structs   | if(true, named_struct('a', 1), named_struct('a', 2L))                   | struct<a:bigint> |
        | a CASE of widening structs  | CASE WHEN true THEN named_struct('a', 1) ELSE named_struct('a', 2L) END | struct<a:bigint> |

    Scenario Outline: <case> still resolves
      When query
        """
        SELECT <expression> IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case                              | expression                                                              |
        | an IF of structs with equal names | if(true, named_struct('a', 1), named_struct('a', 2L))                   |
        | structs differing only by case    | if(true, named_struct('a', 1), named_struct('A', 2L))                   |
        | a struct beside a NULL            | if(true, named_struct('a', 1), NULL)                                    |
