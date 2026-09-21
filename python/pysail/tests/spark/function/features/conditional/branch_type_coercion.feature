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
