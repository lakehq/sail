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

  Rule: STRING branches use Spark's scalar promotion

    # ANSI uses `AnsiStringPromotionTypeCoercion.findWiderTypeForString`: integral values become
    # BIGINT, fractional values (including DECIMAL) become DOUBLE, and other atomic peers remain
    # their own type. Legacy `stringPromotion` instead returns STRING for every atomic peer except
    # BOOLEAN and BINARY (`AnsiTypeCoercion.scala:143-147`, `TypeCoercion.scala:112-121`).
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
        | case                               | ansi  | expression                                                   | type   |
        | a CASE of an int and a string      | true  | CASE WHEN true THEN 1 ELSE '2' END                          | bigint |
        | an IF of an int and a string       | true  | if(true, 1, '2')                                            | bigint |
        | coalesce of an int and a string    | true  | coalesce(CAST(NULL AS INT), '2')                            | bigint |
        | an IF of a decimal and a string    | true  | if(true, '1', CAST(2 AS DECIMAL(10,2)))                     | double |
        | a CASE of an int and a string      | false | CASE WHEN true THEN 1 ELSE '2' END                          | string |

  Rule: decimal container promotion preserves Spark nested nullability

    # `widerDecimalType` calls `boundedPreferIntegralDigits`: when precision exceeds 38 it drops
    # fractional digits first, so DECIMAL(38,2) and DECIMAL(38,10) widen to DECIMAL(38,2).
    Scenario: an array of capped decimals preserves integral digits
      When query
        """
        SELECT typeof(array(CAST(1 AS DECIMAL(38,2)), CAST(1 AS DECIMAL(38,10)))) AS result_type
        """
      Then query result
        | result_type          |
        | array<decimal(38,2)> |

    # `findTypeForComplex` uses only child nullability and `Cast.forceNullable` for the common
    # type (`TypeCoercionHelper.scala:144-147`); widening these decimal elements is safe.
    Scenario: if keeps non-null decimal array elements when their common type widens
      When query
        """
        SELECT if(true, array(CAST(1 AS DECIMAL(12,2))), array(CAST(1 AS DECIMAL(14,4)))) AS value
        """
      Then query schema
        """
        root
         |-- value: array (nullable = false)
         |    |-- element: decimal(14,4) (containsNull = false)
        """

    Scenario: if widens timestamp and timestamp_ntz map keys to timestamp
      When query
        """
        SELECT CAST(
          if(
            true,
            map(to_timestamp('2020-01-01 00:00:00'), 1),
            map(to_timestamp_ntz('2020-01-01 00:00:00'), 2)
          ) AS STRING
        ) AS result
        """
      Then query result
        | result                     |
        | {2020-01-01 00:00:00 -> 1} |

  Rule: branches with no common type are refused

    # CaseWhenCoercion and IfCoercion leave incompatible branches unchanged;
    # conditionalExpressions.scala:83,224 rejects them even when one branch is unreachable.
    Scenario Outline: <case> is refused
      When query
        """
        SELECT <expression> AS v
        """
      Then query error (?i)cannot resolve

      Examples:
        | case                                  | expression                                         |
        | a CASE of an INT and a DATE           | CASE WHEN true THEN 1 ELSE DATE'2024-01-01' END    |
        | a CASE of an INT and an ARRAY         | CASE WHEN true THEN 1 ELSE array(1) END            |
        | an IF of an INT and a DATE            | if(true, 1, DATE'2024-01-01')                      |
        | an IF of maps with incompatible keys  | if(true, map('1', 1), map(2, 2))                   |
        | a CASE of maps with incompatible keys | CASE WHEN true THEN map('1', 1) ELSE map(2, 2) END |

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

    # Spark widens branches recursively, including matching struct leaves.
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

  Rule: conditional analysis rejects incompatible branches

    Scenario Outline: conditional analysis rejects incompatible branches with ANSI <ansi>: <expression>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT (<expression>) IS NOT NULL AS accepted
        """
      Then query error (?i)cannot resolve

      Examples:
        | ansi | expression |
        | true | if(true,1,DATE'2024-01-01') |
        | true | CASE WHEN false THEN 1 ELSE DATE'2024-01-01' END |
        | true | if(true,DATE'2024-01-01',1) |
        | true | CASE WHEN false THEN DATE'2024-01-01' ELSE 1 END |
        | true | if(true,1,array(1)) |
        | true | CASE WHEN false THEN 1 ELSE array(1) END |
        | true | if(true,array(1),1) |
        | true | CASE WHEN false THEN array(1) ELSE 1 END |
        | true | CASE WHEN true THEN 1 WHEN false THEN DATE'2024-01-01' ELSE 'x' END |
        | false | if(true,1,DATE'2024-01-01') |
        | false | CASE WHEN false THEN 1 ELSE DATE'2024-01-01' END |
        | false | if(true,DATE'2024-01-01',1) |
        | false | CASE WHEN false THEN DATE'2024-01-01' ELSE 1 END |
        | false | if(true,1,array(1)) |
        | false | CASE WHEN false THEN 1 ELSE array(1) END |
        | false | if(true,array(1),1) |
        | false | CASE WHEN false THEN array(1) ELSE 1 END |

  Rule: conditional analysis accepts compatible branches

    Scenario Outline: conditional analysis accepts compatible branches with ANSI <ansi>: <expression>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT (<expression>) IS NOT NULL AS accepted
        """
      Then query result
        | accepted |
        | true     |

      Examples:
        | ansi | expression |
        | true | if(true,1,NULL) |
        | true | if(true,array(1),NULL) |
        | true | if(true,DATE'2024-01-01',NULL) |
        | true | if(true,1,2L) |
        | false | if(true,1,NULL) |
        | false | if(true,array(1),NULL) |
        | false | if(true,DATE'2024-01-01',NULL) |
        | false | if(true,1,2L) |
        | false | CASE WHEN true THEN 1 WHEN false THEN DATE'2024-01-01' ELSE 'x' END |
