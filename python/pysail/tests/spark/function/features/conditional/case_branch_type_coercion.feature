Feature: CASE WHEN and IF type their result from every branch, not just the first

  # `CaseWhenCoercion` / `IfCoercion` (`TypeCoercionHelper.scala:498-515`, `:521-538`) cast every
  # `THEN` and the `ELSE` to `findWiderCommonType` before `dataType` merges them, so the declared
  # type never depends on branch order.
  #
  # Expected types and values captured from Spark Connect 4.2.0 (JVM, session timezone UTC).

  Rule: A conditional types from every branch

    # No FROM clause: also covers the constant-foldable path, typed with no input schema.
    Scenario Outline: <case> types as <result>
      When query
        """
        SELECT typeof(<expr>) AS t, CAST(<expr> AS STRING) AS v
        """
      Then query result
        | t        | v       |
        | <result> | <value> |

      Examples:
        | case                        | expr                                                          | result        | value  |
        | CASE decimal / int          | CASE WHEN true THEN CAST(1.5 AS DECIMAL(12,4)) ELSE 0 END     | decimal(14,4) | 1.5000 |
        | CASE int THEN / bigint ELSE | CASE WHEN true THEN CAST(1 AS INT) ELSE CAST(2 AS BIGINT) END | bigint        | 1      |
        | IF int / bigint             | IF(true, CAST(1 AS INT), CAST(2 AS BIGINT))                   | bigint        | 1      |

  Rule: The widest result branch decides the type, whatever position it is written in

    # The winning branch is neither the first nor the `ELSE`: type and value come from different
    # branches.
    Scenario Outline: three branches ordered <order> still type as bigint
      When query
        """
        SELECT
          typeof(CASE WHEN c = 1 THEN CAST(1 AS <first>)
                      WHEN c = 2 THEN CAST(2 AS <second>)
                      ELSE CAST(3 AS <third>) END) AS t,
          CAST(CASE WHEN c = 1 THEN CAST(1 AS <first>)
                    WHEN c = 2 THEN CAST(2 AS <second>)
                    ELSE CAST(3 AS <third>) END AS STRING) AS v
        FROM VALUES (2) AS t(c)
        """
      Then query result
        | t      | v |
        | bigint | 2 |

      Examples:
        | order            | first    | second | third    |
        | widest last      | SMALLINT | INT    | BIGINT   |
        | widest first     | BIGINT   | INT    | SMALLINT |
        | widest in middle | INT      | BIGINT | SMALLINT |

    Scenario Outline: a THEN of <then> against an ELSE of <els> yields <result>
      When query
        """
        SELECT typeof(CASE WHEN c THEN CAST(1 AS <then>) ELSE CAST(2 AS <els>) END) AS t,
               CAST(CASE WHEN c THEN CAST(1 AS <then>) ELSE CAST(2 AS <els>) END AS STRING) AS v
        FROM VALUES (false) AS t(c)
        """
      Then query result
        | t        | v       |
        | <result> | <value> |

      Examples:
        | then     | els      | result   | value |
        | INT      | BIGINT   | bigint   | 2     |
        | SMALLINT | INT      | int      | 2     |
        | TINYINT  | SMALLINT | smallint | 2     |
        | INT      | DOUBLE   | double   | 2.0   |
        | BIGINT   | DOUBLE   | double   | 2.0   |

    Scenario: the last WHEN branch widens the type when there is no ELSE
      When query
        """
        SELECT typeof(CASE WHEN c = 1 THEN CAST(1 AS INT)
                           WHEN c = 2 THEN CAST(2 AS BIGINT) END) AS t,
               CAST(CASE WHEN c = 1 THEN CAST(1 AS INT)
                         WHEN c = 2 THEN CAST(2 AS BIGINT) END AS STRING) AS v
        FROM VALUES (1) AS t(c)
        """
      Then query result
        | t      | v |
        | bigint | 1 |

    Scenario: a single branch with no ELSE keeps its own type
      When query
        """
        SELECT typeof(CASE WHEN c THEN CAST(1 AS INT) END) AS t,
               CAST(CASE WHEN c THEN CAST(1 AS INT) END AS STRING) AS v
        FROM VALUES (true) AS t(c)
        """
      Then query result
        | t   | v |
        | int | 1 |

    Scenario: the CASE operand form widens across branches too
      When query
        """
        SELECT typeof(CASE c WHEN 1 THEN CAST(1 AS INT) ELSE CAST(2 AS BIGINT) END) AS t,
               CAST(CASE c WHEN 1 THEN CAST(1 AS INT) ELSE CAST(2 AS BIGINT) END AS STRING) AS v
        FROM VALUES (1) AS t(c)
        """
      Then query result
        | t      | v |
        | bigint | 1 |

  Rule: An integer branch widens a decimal branch to the common decimal type

    # `Int -> decimal(10,0)` (`DecimalType.forType`), then
    # `widerDecimalType = (max(s1,s2) + max(p1-s1, p2-s2), max(s1,s2))`.
    Scenario Outline: <then> against an integer branch of <els> yields <result>
      When query
        """
        SELECT typeof(CASE WHEN c THEN CAST(1.5 AS <then>) ELSE <els> END) AS t,
               CAST(CASE WHEN c THEN CAST(1.5 AS <then>) ELSE <els> END AS STRING) AS v
        FROM VALUES (<taken>) AS t(c)
        """
      Then query result
        | t        | v       |
        | <result> | <value> |

      Examples:
        | then          | els | taken | result        | value  |
        | DECIMAL(12,4) | 0   | true  | decimal(14,4) | 1.5000 |
        | DECIMAL(12,4) | 0   | false | decimal(14,4) | 0.0000 |
        | DECIMAL(10,2) | 5   | true  | decimal(12,2) | 1.50   |
        | DECIMAL(10,2) | 5   | false | decimal(12,2) | 5.00   |
        | DECIMAL(5,2)  | 0   | true  | decimal(12,2) | 1.50   |
        | DECIMAL(5,2)  | 0   | false | decimal(12,2) | 0.00   |

  Rule: A float branch against a decimal branch widens to double

    # `findWiderTypeForDecimal` (`TypeCoercionHelper.scala:194`) answers DoubleType for
    # `(FractionalType, DecimalType)` in either order.
    Scenario Outline: <case> yields double
      When query
        """
        SELECT typeof(<expr>) AS t, CAST(<expr> AS STRING) AS v
        FROM VALUES (<taken>) AS t(c)
        """
      Then query result
        | t      | v       |
        | double | <value> |

      Examples:
        | case              | expr                                                                     | taken | value |
        | CASE double THEN  | CASE WHEN c THEN CAST(1.5 AS DOUBLE) ELSE CAST(2.5 AS DECIMAL(10,2)) END | true  | 1.5   |
        | CASE decimal THEN | CASE WHEN c THEN CAST(2.5 AS DECIMAL(10,2)) ELSE CAST(1.5 AS DOUBLE) END | true  | 2.5   |
        | CASE float THEN   | CASE WHEN c THEN CAST(1.5 AS FLOAT) ELSE CAST(2.5 AS DECIMAL(10,2)) END  | true  | 1.5   |
        | if double first   | if(c, CAST(1.5 AS DOUBLE), CAST(2.5 AS DECIMAL(10,2)))                   | true  | 1.5   |
        | if decimal first  | if(c, CAST(2.5 AS DECIMAL(10,2)), CAST(1.5 AS DOUBLE))                   | true  | 2.5   |

    # `decimal(30,15)` leaves only 15 integral digits and overflows where DOUBLE does not — on the
    # branch that is not even taken. Compared rather than rendered, to keep float formatting out.
    Scenario: a double branch keeps a magnitude that a decimal(30,15) could not hold
      When query
        """
        SELECT typeof(CASE WHEN c THEN CAST(2.5 AS DECIMAL(10,2)) ELSE CAST(1e20 AS DOUBLE) END) AS t,
               CAST(CASE WHEN c THEN CAST(2.5 AS DECIMAL(10,2)) ELSE CAST(1e20 AS DOUBLE) END
                    = CAST(1e20 AS DOUBLE) AS STRING) AS v
        FROM VALUES (false) AS t(c)
        """
      Then query result
        | t      | v    |
        | double | true |

  Rule: A decimal wider than 38 digits drops fractional digits, not integral ones

    # `boundedPreferIntegralDigits` (`DecimalType.scala:148-158`): past precision 38 Spark cuts the
    # SCALE so the integral digits survive.
    Scenario Outline: <then> against <els> yields <result>
      When query
        """
        SELECT typeof(CASE WHEN c THEN CAST(1 AS <then>) ELSE CAST(2 AS <els>) END) AS t,
               CAST(CASE WHEN c THEN CAST(1 AS <then>) ELSE CAST(2 AS <els>) END AS STRING) AS v
        FROM VALUES (true) AS t(c)
        """
      Then query result
        | t        | v       |
        | <result> | <value> |

      Examples:
        | then           | els            | result         | value        |
        | DECIMAL(38,10) | DECIMAL(38,0)  | decimal(38,0)  | 1            |
        | DECIMAL(38,0)  | DECIMAL(38,10) | decimal(38,0)  | 1            |
        | DECIMAL(38,20) | DECIMAL(38,0)  | decimal(38,0)  | 1            |
        | DECIMAL(38,10) | BIGINT         | decimal(38,10) | 1.0000000000 |

  Rule: IF unifies its two result branches the same way

    Scenario Outline: if with a <then> branch and an <els> branch yields <result>
      When query
        """
        SELECT typeof(if(c, CAST(1 AS <then>), CAST(2 AS <els>))) AS t,
               CAST(if(c, CAST(1 AS <then>), CAST(2 AS <els>)) AS STRING) AS v
        FROM VALUES (<taken>) AS t(c)
        """
      Then query result
        | t        | v       |
        | <result> | <value> |

      Examples:
        | then     | els    | taken | result | value |
        | INT      | BIGINT | true  | bigint | 1     |
        | INT      | BIGINT | false | bigint | 2     |
        | SMALLINT | INT    | true  | int    | 1     |
        | INT      | DOUBLE | true  | double | 1.0   |

    Scenario: if widens a decimal branch against an integer branch
      When query
        """
        SELECT typeof(if(c, CAST(1.5 AS DECIMAL(12,4)), 0)) AS t,
               CAST(if(c, CAST(1.5 AS DECIMAL(12,4)), 0) AS STRING) AS v
        FROM VALUES (true) AS t(c)
        """
      Then query result
        | t             | v      |
        | decimal(14,4) | 1.5000 |

  Rule: A NULL branch never decides the type

    Scenario Outline: a <null> THEN against a bigint ELSE stays bigint
      When query
        """
        SELECT typeof(CASE WHEN c THEN <null> ELSE CAST(2 AS BIGINT) END) AS t,
               CAST(CASE WHEN c THEN <null> ELSE CAST(2 AS BIGINT) END AS STRING) AS v
        FROM VALUES (false) AS t(c)
        """
      Then query result
        | t      | v |
        | bigint | 2 |

      Examples:
        | null              |
        | NULL              |
        | CAST(NULL AS INT) |

  Rule: Nested element types widen along with the branches

    Scenario Outline: <case> widens its element type to bigint
      When query
        """
        SELECT typeof(CASE WHEN c THEN <then> ELSE <els> END) AS t,
               CAST(CASE WHEN c THEN <then> ELSE <els> END AS STRING) AS v
        FROM VALUES (true) AS t(c)
        """
      Then query result
        | t        | v       |
        | <result> | <value> |

      Examples:
        | case   | then                              | els                                  | result             | value    |
        | array  | array(CAST(1 AS INT))             | array(CAST(2 AS BIGINT))             | array<bigint>      | [1]      |
        | struct | named_struct('a', CAST(1 AS INT)) | named_struct('a', CAST(2 AS BIGINT)) | struct<a:bigint>   | {1}      |
        | map    | map('k', CAST(1 AS INT))          | map('k', CAST(2 AS BIGINT))          | map<string,bigint> | {k -> 1} |

  Rule: The declared schema carries the widened type, for every scalar pair

    # Asserts the schema as the client receives it, not `typeof`. Nullable columns on purpose:
    # `CaseWhen` nullability is a separate, still-open gap.
    Scenario Outline: the schema of a <then> branch against a <els> branch is <result>
      When query
        """
        SELECT CASE WHEN c THEN a ELSE b END AS r
        FROM VALUES
          (true, CAST(1 AS <then>), CAST(2 AS <els>)),
          (false, CAST(NULL AS <then>), CAST(NULL AS <els>))
        AS t(c, a, b)
        """
      Then query schema
        """
        root
         |-- r: <result> (nullable = true)
        """

      Examples:
        | then          | els           | result        |
        | INT           | BIGINT        | long          |
        | SMALLINT      | INT           | integer       |
        | TINYINT       | BIGINT        | long          |
        | INT           | DOUBLE        | double        |
        | DECIMAL(12,4) | INT           | decimal(14,4) |
        | INT           | DECIMAL(12,4) | decimal(14,4) |
        | DECIMAL(10,2) | BIGINT        | decimal(22,2) |
        | DOUBLE        | DECIMAL(10,2) | double        |

  Rule: The declared schema widens inside arrays, maps and structs too

    # `typeof` renders no nullability, so `containsNull` / `valueContainsNull` need a schema tree.
    # The ELSE is omitted deliberately: with one, the open `CaseWhen` nullability gap would mask
    # the nested widening this Rule pins. Without it both engines agree on `true`.
    Scenario: an array branch widens its element type in the reported schema
      When query
        """
        SELECT CASE WHEN c THEN array(a) WHEN NOT c THEN array(b) END AS r
        FROM VALUES
          (true, CAST(1 AS INT), CAST(2 AS BIGINT)),
          (false, CAST(NULL AS INT), CAST(NULL AS BIGINT))
        AS t(c, a, b)
        """
      Then query schema
        """
        root
         |-- r: array (nullable = true)
         |    |-- element: long (containsNull = true)
        """

    Scenario: a map branch widens its value type in the reported schema
      When query
        """
        SELECT CASE WHEN c THEN map('k', a) WHEN NOT c THEN map('k', b) END AS r
        FROM VALUES
          (true, CAST(1 AS INT), CAST(2 AS BIGINT)),
          (false, CAST(NULL AS INT), CAST(NULL AS BIGINT))
        AS t(c, a, b)
        """
      Then query schema
        """
        root
         |-- r: map (nullable = true)
         |    |-- key: string
         |    |-- value: long (valueContainsNull = true)
        """

    Scenario: a struct branch widens its field type in the reported schema
      When query
        """
        SELECT CASE WHEN c THEN named_struct('f', a) WHEN NOT c THEN named_struct('f', b) END AS r
        FROM VALUES
          (true, CAST(1 AS INT), CAST(2 AS BIGINT)),
          (false, CAST(NULL AS INT), CAST(NULL AS BIGINT))
        AS t(c, a, b)
        """
      Then query schema
        """
        root
         |-- r: struct (nullable = true)
         |    |-- f: long (nullable = true)
        """

  Rule: The decimal rules apply inside arrays, maps and structs too

    # `findTypeForComplex` (`TypeCoercionHelper.scala:137-177`) recurses `findWiderTypeForTwo` into
    # the element, so the same two rules decide an element as a scalar branch.
    Scenario Outline: A <container> branch applies <rule> to its element type
      When query
        """
        SELECT typeof(if(c, <narrow>, <wide>)) AS t FROM VALUES (true) AS t(c)
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | container | rule            | narrow                                         | wide                                        | result                    |
        | array     | the 38 bound    | array(CAST(1.5 AS DECIMAL(20,15)))             | array(CAST(2 AS DECIMAL(30,0)))             | array<decimal(38,8)>      |
        | struct    | the 38 bound    | named_struct('f', CAST(1.5 AS DECIMAL(20,15))) | named_struct('f', CAST(2 AS DECIMAL(30,0))) | struct<f:decimal(38,8)>   |
        | map       | the 38 bound    | map('k', CAST(1.5 AS DECIMAL(20,15)))          | map('k', CAST(2 AS DECIMAL(30,0)))          | map<string,decimal(38,8)> |
        | array     | float to double | array(CAST(1.5 AS DOUBLE))                     | array(CAST(2.5 AS DECIMAL(10,2)))           | array<double>             |

  Rule: The decimal rules hold with ANSI disabled too

    # `findWiderTypeForDecimal` is reached from both dialects, so no decimal answer above is
    # ANSI-specific. Every other scenario runs on the session default, ANSI-on; these pin the rest.
    Scenario Outline: With ANSI disabled, <then> against <els> still yields <result>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(CASE WHEN c THEN CAST(1 AS <then>) ELSE CAST(2 AS <els>) END) AS t
        FROM VALUES (true) AS t(c)
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | then           | els           | result        |
        | INT            | BIGINT        | bigint        |
        | DECIMAL(12,4)  | INT           | decimal(14,4) |
        | DOUBLE         | DECIMAL(10,2) | double        |
        | DECIMAL(38,20) | DECIMAL(38,0) | decimal(38,0) |

  Rule: Struct branches pair their fields by name

    # `findTypeForComplex` (`TypeCoercionHelper.scala:163-176`) pairs struct fields positionally and
    # requires the names to resolve at every position, so Spark rejects any name mismatch. Sail is
    # more lenient: DataFusion pairs by NAME and emits them in the left branch's order. The element
    # rules must therefore either follow that pairing or stand aside — pairing positionally on top
    # of a by-name result would coerce two fields that are not the same column.
    Scenario: struct branches naming different fields are rejected
      When query
        """
        SELECT if(c, named_struct('a', CAST(1 AS INT)), named_struct('b', CAST(2 AS BIGINT))) AS r
        FROM VALUES (false) AS t(c)
        """
      Then query error .*(DATA_DIFF_TYPES|Unsupported CAST).*

    Scenario: struct branches naming the same field widen it
      When query
        """
        SELECT typeof(if(c, named_struct('a', CAST(1 AS INT)), named_struct('a', CAST(2 AS BIGINT)))) AS t
        FROM VALUES (false) AS t(c)
        """
      Then query result
        | t                |
        | struct<a:bigint> |

    # Spark rejects a reordering outright; Sail resolves it by name instead. Pinned so the values
    # cannot silently land under the wrong field name if the pairing is ever revisited.
    @sail-bug
    Scenario: struct branches listing the same fields in a different order are rejected
      When query
        """
        SELECT if(c, named_struct('a', CAST(1 AS INT), 'b', CAST(2 AS BIGINT)),
                     named_struct('b', CAST(3 AS BIGINT), 'a', CAST(4 AS INT))) AS r
        FROM VALUES (false) AS t(c)
        """
      Then query error .*DATA_DIFF_TYPES.*

    @sail-only
    Scenario: a reordered struct still resolves each field by its own name
      When query
        """
        SELECT CAST(if(c, named_struct('a', CAST(1 AS INT), 'b', CAST(2 AS BIGINT)),
                          named_struct('b', CAST(3 AS BIGINT), 'a', CAST(4 AS INT))) AS STRING) AS v
        FROM VALUES (false) AS t(c)
        """
      Then query result
        | v      |
        | {4, 3} |

  Rule: Decimal256 branches keep their width, at any depth

    # Spark decimals never exceed 38 digits, so `CAST(1 AS DECIMAL(50, 10))` is rejected outright and
    # there is no Spark answer to match — these are `@sail-only`. What they pin is INTERNAL: the
    # `Decimal256` exception is decided for the whole conditional, so a nested branch resolves the
    # same way a scalar one does. Narrowing either to DOUBLE would drop 20 significant digits.
    @sail-only
    Scenario Outline: <case> keeps the wider decimal
      When query
        """
        SELECT typeof(if(c, <narrow>, <wide>)) AS t FROM VALUES (true) AS t(c)
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case          | narrow                           | wide                       | result                |
        | a scalar pair | CAST(1 AS DECIMAL(50,10))        | CAST(1.5 AS DOUBLE)        | decimal(55,15)        |
        | inside array  | array(CAST(1 AS DECIMAL(50,10))) | array(CAST(1.5 AS DOUBLE)) | array<decimal(55,15)> |

  Rule: Branches with no common type are rejected instead of silently typed

    # Spark raises `DATATYPE_MISMATCH.DATA_DIFF_TYPES` (`conditionalExpressions.scala:220-227`);
    # Sail refuses during planning. The pattern matches either, since a bare `.*` would stay green
    # on a parse error or a dropped connection.
    Scenario: a boolean branch against an integer branch is rejected
      When query
        """
        SELECT CASE WHEN c THEN true ELSE 1 END AS r FROM VALUES (true) AS t(c)
        """
      Then query error .*(DATA_DIFF_TYPES|Failed to coerce then).*

    Scenario: if with a boolean branch against an integer branch is rejected
      When query
        """
        SELECT if(c, true, 1) AS r FROM VALUES (true) AS t(c)
        """
      Then query error .*(DATA_DIFF_TYPES|Failed to coerce then).*

  Rule: An integral branch against a float branch widens per dialect

    # ANSI's `findTightestCommonType` returns DoubleType for integral-meets-float
    # (`AnsiTypeCoercion.scala:113-124`); the non-ANSI path (`TypeCoercion.scala:88-92`) answers
    # plain FLOAT, which is what Sail answers — hence the explicit ANSI pin.
    #
    # Still open, and not specific to CASE: `coalesce`, `greatest`, `least`, `array`, `UNION ALL`,
    # `+` and `*` all answer `float` too, so this belongs in a shared numeric-coercion fix rather
    # than in the conditional builder.
    #
    # The ANSI-off counterpart, which Sail already gets right.
    Scenario: With ANSI disabled, an integral branch against a float branch stays float
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(CASE WHEN c THEN CAST(1 AS FLOAT) ELSE CAST(2147483647 AS INT) END) AS t
        FROM VALUES (false) AS t(c)
        """
      Then query result
        | t     |
        | float |

    @sail-bug
    Scenario Outline: <case> widens to double under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof(<expr>) AS t FROM VALUES (false) AS t(c)
        """
      Then query result
        | t      |
        | double |

      Examples:
        | case              | expr                                                               |
        | CASE with INT_MAX | CASE WHEN c THEN CAST(1 AS FLOAT) ELSE CAST(2147483647 AS INT) END |
        | if with INT_MAX   | if(c, CAST(1 AS FLOAT), CAST(2147483647 AS INT))                   |
        | CASE with 2^24+1  | CASE WHEN c THEN CAST(1 AS FLOAT) ELSE CAST(16777217 AS INT) END   |

    # The value half, kept separate: a strict xfail is satisfied by EITHER assertion failing, so a
    # combined scenario could not tell "Sail answers float" from "Sail rounds through float32", and
    # would stay green if only one of the two were ever fixed. Read back through DECIMAL(20,0) so
    # the assertion pins the number and not how each engine formats a float.
    @sail-bug
    Scenario Outline: <case> keeps an integral branch exact instead of rounding it through float32
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(CAST(<expr> AS DECIMAL(20,0)) AS STRING) AS v
        FROM VALUES (false) AS t(c)
        """
      Then query result
        | v       |
        | <value> |

      Examples:
        | case              | expr                                                               | value      |
        | CASE with INT_MAX | CASE WHEN c THEN CAST(1 AS FLOAT) ELSE CAST(2147483647 AS INT) END | 2147483647 |
        | if with INT_MAX   | if(c, CAST(1 AS FLOAT), CAST(2147483647 AS INT))                   | 2147483647 |
        | CASE with 2^24+1  | CASE WHEN c THEN CAST(1 AS FLOAT) ELSE CAST(16777217 AS INT) END   | 16777217   |
