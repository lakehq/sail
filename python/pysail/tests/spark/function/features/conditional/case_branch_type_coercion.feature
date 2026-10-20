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
        | DECIMAL(38,10) | BIGINT         | decimal(38,10) | 1.0000000000 |

    # Spark 3.5 keeps the scale and cuts the integral digits (`DecimalType.bounded`);
    # `boundedPreferIntegralDigits` is 4.0 and later.
    @spark-4
    Scenario Outline: <then> against <els> yields <result> past 38 digits
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

    # Asserts the schema as the client receives it, not `typeof`. Nullable columns on purpose, so
    # the rows pin the type and not the nullability.
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
    # The ELSE is omitted deliberately, so the CASE is nullable and the rows pin the nested
    # widening and not the nullability of the whole expression.
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
        | array     | float to double | array(CAST(1.5 AS DOUBLE))                     | array(CAST(2.5 AS DECIMAL(10,2)))           | array<double>             |

    @spark-4
    Scenario Outline: A <container> branch applies the 38 bound to its element type
      When query
        """
        SELECT typeof(if(c, <narrow>, <wide>)) AS t FROM VALUES (true) AS t(c)
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | container | narrow                                         | wide                                        | result                    |
        | array     | array(CAST(1.5 AS DECIMAL(20,15)))             | array(CAST(2 AS DECIMAL(30,0)))             | array<decimal(38,8)>      |
        | struct    | named_struct('f', CAST(1.5 AS DECIMAL(20,15))) | named_struct('f', CAST(2 AS DECIMAL(30,0))) | struct<f:decimal(38,8)>   |
        | map       | map('k', CAST(1.5 AS DECIMAL(20,15)))          | map('k', CAST(2 AS DECIMAL(30,0)))          | map<string,decimal(38,8)> |

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

    @spark-4
    Scenario: With ANSI disabled, a decimal past 38 digits still keeps its integral digits
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(CASE WHEN c THEN CAST(1 AS DECIMAL(38,20)) ELSE CAST(2 AS DECIMAL(38,0)) END) AS t
        FROM VALUES (true) AS t(c)
        """
      Then query result
        | t             |
        | decimal(38,0) |

  Rule: Struct branches pair their fields by name

    # `findTypeForComplex` (`TypeCoercionHelper.scala:163-176`) pairs struct fields positionally and
    # requires the names to resolve at every position, so Spark rejects any name mismatch. Sail
    # pairs them the same way instead of leaving it to DataFusion, which pairs by NAME.
    Scenario: struct branches naming different fields are rejected
      When query
        """
        SELECT if(c, named_struct('a', CAST(1 AS INT)), named_struct('b', CAST(2 AS BIGINT))) AS r
        FROM VALUES (false) AS t(c)
        """
      Then query error .*DATA_DIFF_TYPES.*

    Scenario: struct branches naming the same field widen it
      When query
        """
        SELECT typeof(if(c, named_struct('a', CAST(1 AS INT)), named_struct('a', CAST(2 AS BIGINT)))) AS t
        FROM VALUES (false) AS t(c)
        """
      Then query result
        | t                |
        | struct<a:bigint> |

    # A reordering is a name mismatch at every position, so it is rejected too; pinned so the
    # values cannot silently land under the wrong field name.
    Scenario: struct branches listing the same fields in a different order are rejected
      When query
        """
        SELECT if(c, named_struct('a', CAST(1 AS INT), 'b', CAST(2 AS BIGINT)),
                     named_struct('b', CAST(3 AS BIGINT), 'a', CAST(4 AS INT))) AS r
        FROM VALUES (false) AS t(c)
        """
      Then query error .*DATA_DIFF_TYPES.*

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

  Rule: Datetime branches keep Spark's timestamp flavor

    Scenario Outline: <case> yields <result>
      When query
        """
        SELECT typeof(<expr>) AS t FROM VALUES (false) AS t(c)
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case      | expr                                                                                     | result        |
        | NTZ, DATE | if(c, CAST('2020-01-02 03:04:05' AS TIMESTAMP_NTZ), DATE '2020-01-01')                   | timestamp_ntz |
        | LTZ, NTZ  | if(c, CAST('2020-01-02 03:04:05' AS TIMESTAMP_LTZ), CAST('2020-01-01' AS TIMESTAMP_NTZ)) | timestamp     |

    # `findWiderDateTimeType` (`TypeCoercionHelper.scala:243-255`) answers TimestampType for
    # DATE against an LTZ timestamp.
    Scenario: a zoned timestamp against a date keeps its zone
      When query
        """
        SELECT typeof(if(c, CAST('2020-01-02 03:04:05' AS TIMESTAMP_LTZ), DATE '2020-01-01')) AS t
        FROM VALUES (false) AS t(c)
        """
      Then query result
        | t         |
        | timestamp |

  Rule: Struct branches whose field names differ only in case

    # Spark's resolver is case-insensitive under the default `spark.sql.caseSensitive = false`, so
    # `A` and `a` are the same field and the branches merge. DataFusion's struct cast matches
    # names case-SENSITIVELY, so the values are carried by Sail's positional rename adapter. The
    # type and the values are asserted apart.
    Scenario: the declared type merges them
      When query
        """
        SELECT typeof(if(c, named_struct('A', CAST(1 AS INT)), named_struct('a', CAST(2 AS BIGINT)))) AS t
        FROM VALUES (false) AS t(c)
        """
      Then query result
        | t                |
        | struct<A:bigint> |

    Scenario: the value survives the rename
      When query
        """
        SELECT CAST(if(c, named_struct('A', CAST(1 AS INT)), named_struct('a', CAST(2 AS BIGINT))) AS STRING) AS v
        FROM VALUES (false) AS t(c)
        """
      Then query result
        | v   |
        | {2} |

  Rule: Branches with no common type are rejected instead of silently typed

    # Spark raises `DATATYPE_MISMATCH.DATA_DIFF_TYPES` (`conditionalExpressions.scala:220-227`);
    # Sail refuses during planning. The pattern matches either, since a bare `.*` would stay green
    # on a parse error or a dropped connection.
    Scenario: a boolean branch against an integer branch is rejected
      When query
        """
        SELECT CASE WHEN c THEN true ELSE 1 END AS r FROM VALUES (true) AS t(c)
        """
      Then query error .*DATA_DIFF_TYPES.*

    Scenario: if with a boolean branch against an integer branch is rejected
      When query
        """
        SELECT if(c, true, 1) AS r FROM VALUES (true) AS t(c)
        """
      Then query error .*DATA_DIFF_TYPES.*

  Rule: An integral branch against a float branch widens per dialect

    # ANSI's `findTightestCommonType` returns DoubleType for integral-meets-float
    # (`AnsiTypeCoercion.scala:113-124`); the non-ANSI path (`TypeCoercion.scala:88-92`) answers
    # plain FLOAT. Both dialects are pinned because the answer genuinely differs between them.
    #
    # The ANSI-off counterpart. `coalesce`, `greatest`, `least`, `array`, `UNION ALL`, `+` and `*`
    # still answer `float` under ANSI, so they need the same rule in a shared numeric-coercion fix.
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

    # The value half, kept separate from the type. Read back through DECIMAL(20,0) so the
    # assertion pins the number and not how each engine formats a float.
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

  # The Rules below hold what the branch typing changes beyond the type itself: queries that ran
  # before and must keep running, and the Spark rules the typing has to follow
  # (`CaseWhen` / `If` in `conditionalExpressions.scala`, `TypeCoercionHelper.scala:137-255`).
  # Measured on Spark 4.2.0 (JVM, Spark Connect, session timezone UTC unless a scenario pins it).

  Rule: A struct renamed to the merged field names keeps its values at every depth

    # `needs_struct_field_rename` only compared the first level of names, so a name that differs in
    # case one level down was left to DataFusion's name-matched struct cast, which fills the
    # renamed slot with NULL.
    Scenario Outline: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expr> AS r FROM VALUES (<c>) AS t(c)
        """
      Then query result
        | r        |
        | <result> |

      Examples:
        | case                                | ansi  | c     | expr                                                                                                                         | result |
        | two levels, ELSE taken              | true  | false | if(c, named_struct('x', named_struct('A', 1, 'b', 2)), named_struct('x', named_struct('a', 3L, 'b', 4L))).x.A                | 3      |
        | two levels, ELSE taken, other field | true  | false | if(c, named_struct('x', named_struct('A', 1, 'b', 2)), named_struct('x', named_struct('a', 3L, 'b', 4L))).x.b                | 4      |
        | two levels, THEN taken              | true  | true  | if(c, named_struct('x', named_struct('A', 1, 'b', 2)), named_struct('x', named_struct('a', 3L, 'b', 4L))).x.A                | 1      |
        | two levels, ANSI off                | false | false | if(c, named_struct('x', named_struct('A', 1, 'b', 2)), named_struct('x', named_struct('a', 3L, 'b', 4L))).x.A                | 3      |
        | three levels, ELSE taken            | true  | false | if(c, named_struct('x', named_struct('y', named_struct('A', 1))), named_struct('x', named_struct('y', named_struct('a', 2L)))).x.y.A | 2      |
        | struct inside an array              | true  | false | if(c, array(named_struct('A', 1)), array(named_struct('a', 2L)))[0].A                                                        | 2      |
        | struct inside a map value           | true  | false | if(c, map('k', named_struct('A', 1)), map('k', named_struct('a', 2L)))['k'].A                                                | 2      |
        | CASE, three branches                | true  | false | CASE WHEN c THEN named_struct('x', named_struct('A', 1)) WHEN NOT c THEN named_struct('x', named_struct('a', 2L)) ELSE named_struct('x', named_struct('A', 3)) END.x.A | 2 |

  Rule: A branch that is not taken is never evaluated, even when its cast could fail

    # Spark widens the taken branch and leaves the other one alone, so a string that is not a
    # number only raises when its branch is the one selected.
    Scenario Outline: <case> does not evaluate the untaken string branch
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS r FROM VALUES (true) AS t(c)
        """
      Then query result
        | r        |
        | <result> |

      Examples:
        | case                   | expr                                                     | result |
        | CASE int / string      | CASE WHEN c THEN 1 ELSE 'abc' END                        | 1      |
        | IF int / string        | IF(c, CAST(1 AS INT), 'abc')                             | 1      |
        | CASE bigint / string   | CASE WHEN c THEN CAST(1 AS BIGINT) ELSE 'abc' END        | 1      |
        | CASE double / string   | CASE WHEN c THEN CAST(1 AS DOUBLE) ELSE 'abc' END        | 1.0    |
        | CASE decimal / string  | CASE WHEN c THEN CAST(1 AS DECIMAL(10,2)) ELSE 'abc' END | 1.0    |
        | string THEN / int ELSE | CASE WHEN NOT c THEN 'abc' ELSE 1 END                    | 1      |

    # The string only turns into a constant after planning (a subquery projection, a foldable
    # function), where DataFusion would raise the cast error whether or not the branch is taken.
    Scenario Outline: <case> does not evaluate an untaken string that folds to a constant
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS r FROM (SELECT <c> AS c, 'abc' AS s)
        """
      Then query result
        | r |
        | 1 |

      Examples:
        | case                    | c    | expr                                       |
        | column of a subquery    | true | CASE WHEN c THEN 1 ELSE s END              |
        | IF with a folded string | true | IF(c, CAST(1 AS INT), s)                   |
        | foldable concat         | true | CASE WHEN c THEN 1 ELSE concat('a', 'bc') END |
        | upper of a literal      | true | CASE WHEN c THEN 1 ELSE upper(s) END       |

    Scenario Outline: <case> raises when the folded string is the branch taken
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS r FROM (SELECT <c> AS c, 'abc' AS s)
        """
      Then query error (?s).*CAST_INVALID_INPUT.*

      Examples:
        | case                 | c     | expr                                          |
        | column of a subquery | false | CASE WHEN c THEN 1 ELSE s END                 |
        | foldable concat      | false | CASE WHEN c THEN 1 ELSE concat('a', 'bc') END |

    Scenario Outline: <case> raises when the string branch is the one taken
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS r FROM VALUES (false) AS t(c)
        """
      Then query error (?s).*CAST_INVALID_INPUT.*

      Examples:
        | case              | expr                              |
        | CASE int / string | CASE WHEN c THEN 1 ELSE 'abc' END |
        | IF int / string   | IF(c, CAST(1 AS INT), 'abc')      |

    Scenario: a string that is a number is widened exactly, with surrounding blanks trimmed
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN 1 ELSE ' 5 ' END AS r FROM VALUES (false) AS t(c)
        """
      Then query result
        | r |
        | 5 |

  Rule: A map key that the merge would have to cast to a nullable type is rejected

    # `findTypeForComplex` refuses a key cast whose `Cast.forceNullable` is true, because a null
    # map key is illegal (`TypeCoercionHelper.scala:149-153`); `DateType` to anything but a zoned
    # timestamp forces nullable (`Cast.scala:441-442`).
    Scenario Outline: a DATE map key against a TIMESTAMP_NTZ map key is rejected with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CASE WHEN c THEN map(DATE'2020-01-01', 1)
                    ELSE map(CAST('2020-01-01 00:00:00' AS TIMESTAMP_NTZ), 1) END AS r
        FROM VALUES (true) AS t(c)
        """
      Then query error .*DATA_DIFF_TYPES.*

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario: a string map key against an int map key is rejected under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN map('1', 1) ELSE map(1, 1) END AS r FROM VALUES (true) AS t(c)
        """
      Then query error .*DATA_DIFF_TYPES.*

    Scenario: a string map key against an int map key becomes a string key without ANSI
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(CASE WHEN c THEN map('1', 1) ELSE map(1, 1) END) AS t FROM VALUES (true) AS t(c)
        """
      Then query result
        | t                |
        | map<string,int>  |

    Scenario: an int map key widens to the wider key
      When query
        """
        SELECT typeof(CASE WHEN c THEN map(1, 1) ELSE map(CAST(2 AS BIGINT), 1) END) AS t
        FROM VALUES (true) AS t(c)
        """
      Then query result
        | t                 |
        | map<bigint,int>   |

  Rule: A string against an interval follows Spark's string promotion

    # Calendar intervals are not an `AtomicType`, so no promotion applies in either dialect
    # (`CalendarIntervalType.scala:36`). Day-time and year-month intervals are atomic: ANSI
    # rejects them against a string (`AnsiStringPromotionTypeCoercion.scala:100`), the legacy
    # dialect promotes the interval to a string.
    Scenario Outline: <case> is rejected
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CASE WHEN c THEN 'a' ELSE <interval> END AS r FROM VALUES (<c>) AS t(c)
        """
      Then query error .*DATA_DIFF_TYPES.*

      Examples:
        | case                       | ansi  | c     | interval          |
        | calendar interval, ANSI on | true  | true  | make_interval(1)  |
        | calendar interval, ANSI on | true  | false | make_interval(1)  |
        | calendar interval, ANSI off | false | true  | make_interval(1)  |
        | calendar interval, ANSI off | false | false | make_interval(1)  |
        | day-time, ANSI on          | true  | true  | INTERVAL '1' DAY  |
        | day-time, ANSI on          | true  | false | INTERVAL '1' DAY  |
        | year-month, ANSI on        | true  | true  | INTERVAL '1' YEAR |
        | year-month, ANSI on        | true  | false | INTERVAL '1' YEAR |

    Scenario Outline: <case> promotes the interval to a string with ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CASE WHEN c THEN 'a' ELSE <interval> END AS r FROM VALUES (<c>) AS t(c)
        """
      Then query result
        | r        |
        | <result> |

      Examples:
        | case                     | c     | interval          | result            |
        | day-time, THEN taken     | true  | INTERVAL '1' DAY  | a                 |
        | day-time, ELSE taken     | false | INTERVAL '1' DAY  | INTERVAL '1' DAY  |
        | year-month, THEN taken   | true  | INTERVAL '1' YEAR | a                 |
        | year-month, ELSE taken   | false | INTERVAL '1' YEAR | INTERVAL '1' YEAR |

  Rule: A string against another type follows the dialect's string promotion

    # Non-ANSI `stringPromotion` (`TypeCoercion.scala:112-120`) turns the other side into a string
    # unless it is BINARY or BOOLEAN; ANSI `findWiderTypeForString`
    # (`AnsiStringPromotionTypeCoercion.scala:92-108`) turns the string into the other side, with
    # INTEGRAL to BIGINT and FRACTIONAL / DECIMAL to DOUBLE. The string is a number where the
    # other type needs one, so the taken branch is always castable.
    Scenario Outline: <str> against <other> types as <result> with ANSI <ansi>, string first
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(CASE WHEN c THEN <str> ELSE <other> END) AS t FROM VALUES (true) AS t(c)
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | ansi  | str                     | other                                   | result        |
        | true  | '1'                     | CAST(1 AS INT)                          | bigint        |
        | true  | '1'                     | CAST(1 AS BIGINT)                       | bigint        |
        | true  | '1'                     | CAST(1 AS DOUBLE)                       | double        |
        | true  | '1'                     | CAST(1 AS DECIMAL(10,2))                | double        |
        | true  | '1'                     | CAST(1 AS DECIMAL(10,0))                | double        |
        | true  | '1'                     | CAST(1 AS DECIMAL(38,0))                | double        |
        | true  | '1'                     | true                                    | boolean       |
        | true  | '1'                     | X'01'                                   | binary        |
        | false | '1'                     | CAST(1 AS INT)                          | string        |
        | false | '1'                     | CAST(1 AS BIGINT)                       | string        |
        | false | '1'                     | CAST(1 AS DOUBLE)                       | string        |
        | false | '1'                     | CAST(1 AS DECIMAL(10,2))                | string        |
        | false | '2020-01-01'            | DATE'2020-01-01'                        | string        |
        | false | '2020-01-01 00:00:00'   | TIMESTAMP'2020-01-01 00:00:00'          | string        |
        | false | '2020-01-01 00:00:00'   | TIMESTAMP_NTZ'2020-01-01 00:00:00'      | string        |

    Scenario Outline: <str> against <other> types as <result> with ANSI <ansi>, string last
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(CASE WHEN c THEN <other> ELSE <str> END) AS t FROM VALUES (true) AS t(c)
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | ansi  | str                     | other                                   | result        |
        | true  | '1'                     | CAST(1 AS INT)                          | bigint        |
        | true  | '1'                     | CAST(1 AS BIGINT)                       | bigint        |
        | true  | '1'                     | CAST(1 AS DOUBLE)                       | double        |
        | true  | '1'                     | CAST(1 AS DECIMAL(10,2))                | double        |
        | true  | '1'                     | CAST(1 AS DECIMAL(10,0))                | double        |
        | true  | '1'                     | CAST(1 AS DECIMAL(38,0))                | double        |
        | true  | '1'                     | true                                    | boolean       |
        | true  | '2020-01-01'            | DATE'2020-01-01'                        | date          |
        | true  | '2020-01-01 00:00:00'   | TIMESTAMP'2020-01-01 00:00:00'          | timestamp     |
        | true  | '2020-01-01 00:00:00'   | TIMESTAMP_NTZ'2020-01-01 00:00:00'      | timestamp_ntz |
        | false | '1'                     | CAST(1 AS INT)                          | string        |
        | false | '1'                     | CAST(1 AS DOUBLE)                       | string        |
        | false | '2020-01-01'            | DATE'2020-01-01'                        | string        |
        | false | '2020-01-01 00:00:00'   | TIMESTAMP'2020-01-01 00:00:00'          | string        |

    Scenario Outline: a string against <other> has no common type without ANSI
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CASE WHEN c THEN <str> ELSE <other> END AS r FROM VALUES (true) AS t(c)
        """
      Then query error .*DATA_DIFF_TYPES.*

      Examples:
        | str | other |
        | '1' | true  |
        | '1' | X'01' |

  Rule: Struct branches fold in written order, and the left branch names the fields

    # Spark only moves a string-typed branch first for CASE, only outside ANSI, and only for a
    # string or an array of strings (`hasStringType`, `TypeCoercion.scala:326-331`). IF never
    # reorders (`TypeCoercionHelper.scala:521-531`).
    Scenario Outline: the fields of an IF take the left branch's names with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(if(c, named_struct('A', 1), named_struct('a', 'x'))) AS t FROM VALUES (true) AS t(c)
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | ansi  | result          |
        | false | struct<A:string> |
        | true  | struct<A:bigint> |

    Scenario Outline: three struct branches with no common fold are rejected with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(CASE WHEN k = 1 THEN named_struct('a', DATE'2020-01-01')
                           WHEN k = 2 THEN named_struct('a', 1)
                           ELSE named_struct('a', 'x') END) AS t
        FROM VALUES (1) AS t(k)
        """
      Then query error .*DATA_DIFF_TYPES.*

      Examples:
        | ansi  |
        | false |
        | true  |

  Rule: Struct field names pair the way Spark's resolver does

    Scenario: a field name that differs only by a non-ASCII letter pairs case-insensitively
      When query
        """
        SELECT typeof(if(c, named_struct('É', 1), named_struct('é', 2L))) AS t FROM VALUES (true) AS t(c)
        """
      Then query result
        | t                |
        | struct<É:bigint> |

    Scenario: the same names with caseSensitive on are different fields
      Given config spark.sql.caseSensitive = true
      When query
        """
        SELECT if(c, named_struct('A', 1), named_struct('a', 2L)) AS r FROM VALUES (true) AS t(c)
        """
      Then query error .*DATA_DIFF_TYPES.*

    Scenario: structs with a different number of fields are rejected
      When query
        """
        SELECT if(c, named_struct('a', 1), named_struct('a', 1, 'b', 2)) AS r FROM VALUES (true) AS t(c)
        """
      Then query error .*DATA_DIFF_TYPES.*

  Rule: A condition must be a BOOLEAN, and the value types are checked first only for CASE

    # `If.checkInputDataTypes` looks at the predicate first (`conditionalExpressions.scala:70`);
    # `CaseWhen.checkInputDataTypes` looks at the value types first and only then at each
    # condition (`conditionalExpressions.scala:205-230`). An untyped NULL predicate of an IF is
    # cast to BOOLEAN by `IfCoercion` (`TypeCoercionHelper.scala:532-535`); a CASE keeps it and
    # is rejected.
    Scenario Outline: <case> is rejected as UNEXPECTED_INPUT_TYPE
      When query
        """
        SELECT <expr> AS r
        """
      Then query error .*UNEXPECTED_INPUT_TYPE.*

      Examples:
        | case                                  | expr                              |
        | IF with a string predicate            | if('a', true, 1)                  |
        | IF with a string predicate, same types | if('a', 1, 2)                    |
        | CASE with an int condition            | CASE WHEN 1 THEN 1 ELSE 2 END     |
        | CASE with a NULL condition            | CASE WHEN NULL THEN 1 ELSE 2 END  |
        | CASE with an int second condition     | CASE WHEN true THEN 1 WHEN 1 THEN 2 ELSE 3 END |

    Scenario: an IF with an untyped NULL predicate takes the ELSE and still widens
      When query
        """
        SELECT if(NULL, 1, 2L) AS r, typeof(if(NULL, 1, 2L)) AS t
        """
      Then query result
        | r | t      |
        | 2 | bigint |

    Scenario: a CASE with values of different types reports the value types before the condition
      When query
        """
        SELECT CASE WHEN 1 THEN true ELSE 1 END AS r
        """
      Then query error .*DATA_DIFF_TYPES.*

  Rule: The rejection names the function and renders Spark's type names

    # `DataTypeMismatch("DATA_DIFF_TYPES", Map("functionName" -> toSQLId(prettyName), "dataType" ->
    # dataTypes.map(toSQLType)))` (`conditionalExpressions.scala:220-227`); `CaseWhen` is
    # `casewhen` and `If` is `if`.
    Scenario Outline: <case> says which function and which types
      When query
        """
        SELECT <expr> AS r FROM VALUES (true) AS t(c)
        """
      Then query error (?s).*DATA_DIFF_TYPES.*Input to .<fn>. should all be the same type, but it's \["<first>", "<second>"\].*

      Examples:
        | case              | expr                       | fn       | first   | second |
        | CASE bool / int   | CASE WHEN c THEN true ELSE 1 END | casewhen | BOOLEAN | INT    |
        | IF bool / int     | if(c, true, 1)             | if       | BOOLEAN | INT    |
        | CASE int / bool   | CASE WHEN c THEN 1 ELSE true END | casewhen | INT     | BOOLEAN |

  Rule: The declared nullability follows Spark

    # `CaseWhen.nullable` is `branches.exists(_._2.nullable) || elseValue.forall(_.nullable)`
    # (`conditionalExpressions.scala:191-200`) and `If.nullable` is
    # `trueValue.nullable || falseValue.nullable` (`:55`). A branch cast is part of it:
    # `Cast.nullable = child.nullable || Cast.forceNullable(from, to)` (`Cast.scala:630-631`), and
    # `forceNullable` is true for a DATE to anything but a zoned timestamp (`Cast.scala:441-442`)
    # and for a string that must be parsed. Typed literals and columns carry the branches, so no
    # explicit `CAST` contributes nullability of its own.
    Scenario Outline: <case> is <nullability> in the reported schema
      When query
        """
        SELECT <expr> AS r FROM VALUES (true) AS t(c)
        """
      Then query schema
        """
        root
         |-- r: <type> (nullable = <nullable>)
        """

      Examples:
        | case                               | expr                                                                          | type          | nullable | nullability  |
        | CASE of non-null branches          | CASE WHEN c THEN CAST(1 AS INT) ELSE CAST(2 AS BIGINT) END                    | long          | false    | not nullable |
        | IF of non-null branches            | if(c, CAST(1 AS INT), CAST(2 AS BIGINT))                                      | long          | false    | not nullable |
        | CASE without an ELSE               | CASE WHEN c THEN CAST(1 AS INT) END                                           | integer       | true     | nullable     |
        | CASE with a NULL ELSE              | CASE WHEN c THEN CAST(1 AS INT) ELSE NULL END                                 | integer       | true     | nullable     |
        | CASE with a nullable middle branch | CASE WHEN c THEN 1 WHEN NOT c THEN CAST(NULL AS INT) ELSE 2 END               | integer       | true     | nullable     |
        | a literal TRUE then a NULL ELSE    | CASE WHEN true THEN CAST(1 AS INT) ELSE CAST(NULL AS INT) END                 | integer       | false    | not nullable |
        | a literal TRUE and no ELSE         | CASE WHEN true THEN CAST(1 AS INT) END                                        | integer       | false    | not nullable |
        | a literal TRUE after a WHEN        | CASE WHEN c THEN 1 WHEN true THEN 2 ELSE CAST(NULL AS INT) END                | integer       | false    | not nullable |
        | a NULL branch after a literal TRUE | CASE WHEN true THEN CAST(1 AS INT) WHEN c THEN CAST(NULL AS INT) END          | integer       | false    | not nullable |
        | a NULL branch before the TRUE      | CASE WHEN c THEN CAST(NULL AS INT) WHEN true THEN 2 ELSE CAST(NULL AS INT) END | integer      | true     | nullable     |
        | a literal TRUE that widens         | CASE WHEN true THEN CAST(1 AS INT) ELSE CAST(2 AS BIGINT) END                 | long          | false    | not nullable |
        | IF of a DATE and a TIMESTAMP_NTZ   | if(c, DATE'2020-01-01', TIMESTAMP_NTZ'2020-01-02 00:00:00')                   | timestamp_ntz | true     | nullable     |
        | CASE of a DATE and a TIMESTAMP_NTZ | CASE WHEN c THEN DATE'2020-01-01' ELSE TIMESTAMP_NTZ'2020-01-02 00:00:00' END | timestamp_ntz | true     | nullable     |
        | CASE of a DATE and a TIMESTAMP     | CASE WHEN c THEN DATE'2020-01-01' ELSE TIMESTAMP'2020-01-02 00:00:00' END     | timestamp     | false    | not nullable |

    Scenario: a string parsed as a number makes the conditional nullable under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN '1' ELSE 2 END AS r FROM VALUES (true) AS t(c)
        """
      Then query schema
        """
        root
         |-- r: long (nullable = true)
        """

    # The branches are typed literals, so the only source of a nullable element is the merge.
    Scenario Outline: <case> reports containsNull = <contains_null>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CASE WHEN c THEN array(a) ELSE array(b) END AS r
        FROM VALUES (true, <a>, <b>) AS t(c, a, b)
        """
      Then query schema
        """
        root
         |-- r: array (nullable = false)
         |    |-- element: <type> (containsNull = <contains_null>)
        """

      Examples:
        | case                              | ansi  | a                       | b                                  | type          | contains_null |
        | int against bigint                | true  | CAST(1 AS INT)          | CAST(2 AS BIGINT)                  | long          | false         |
        | DATE against TIMESTAMP_NTZ        | true  | DATE'2020-01-01'        | TIMESTAMP_NTZ'2020-01-02 00:00:00' | timestamp_ntz | true          |
        | DATE against TIMESTAMP_NTZ, ANSI off | false | DATE'2020-01-01'     | TIMESTAMP_NTZ'2020-01-02 00:00:00' | timestamp_ntz | true          |
        | DATE against TIMESTAMP            | true  | DATE'2020-01-01'        | TIMESTAMP'2020-01-02 00:00:00'     | timestamp     | false         |
        | decimal gaining scale             | true  | CAST(1.5 AS DECIMAL(10,0)) | CAST(1.5 AS DECIMAL(10,2))      | decimal(12,2) | false         |
        | decimal gaining scale, ANSI off   | false | CAST(1.5 AS DECIMAL(10,0)) | CAST(1.5 AS DECIMAL(10,2))      | decimal(12,2) | false         |
        | decimal of the same range         | true  | CAST(1.5 AS DECIMAL(5,2)) | CAST(1.5 AS DECIMAL(7,4))        | decimal(7,4)  | false         |

  Rule: A nested ANSI string cast keeps the nullability of its input

    # `Cast.nullable` adds `forceNullable` only for the atomic STRING to X pair, so an array, map
    # or struct that is cast element by element is nullable exactly when its input is
    # (`Cast.scala:427-448`, `:630`). The parsed element is nullable (`containsNull`, `valueContainsNull`).
    Scenario: A non-null array of strings merged with an array of bigint is a non-null array
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN array('1') ELSE array(CAST(2 AS BIGINT)) END AS r
        FROM VALUES (false) AS t(c)
        """
      Then query schema
        """
        root
         |-- r: array (nullable = false)
         |    |-- element: long (containsNull = true)
        """

    Scenario: A non-null map of strings merged with a map of bigint is a non-null map
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN map('k', '1') ELSE map('k', CAST(2 AS BIGINT)) END AS r
        FROM VALUES (false) AS t(c)
        """
      Then query schema
        """
        root
         |-- r: map (nullable = false)
         |    |-- key: string
         |    |-- value: long (valueContainsNull = true)
        """

    Scenario: A non-null struct of strings merged with a struct of bigint is a non-null struct
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN struct('1' AS a) ELSE struct(CAST(2 AS BIGINT) AS a) END AS r
        FROM VALUES (false) AS t(c)
        """
      Then query schema
        """
        root
         |-- r: struct (nullable = false)
         |    |-- a: long (nullable = true)
        """

    Scenario: A nullable array of strings merged with an array of bigint stays nullable
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN a ELSE array(CAST(2 AS BIGINT)) END AS r
        FROM VALUES (false, array('1')), (true, CAST(NULL AS ARRAY<STRING>)) AS t(c, a)
        """
      Then query schema
        """
        root
         |-- r: array (nullable = true)
         |    |-- element: long (containsNull = true)
        """

    Scenario: A string merged with a bigint is nullable because the parse can fail
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN '1' ELSE CAST(2 AS BIGINT) END AS r FROM VALUES (false) AS t(c)
        """
      Then query schema
        """
        root
         |-- r: long (nullable = true)
        """

  Rule: Datetime branches are read in the session zone

    # `findWiderDateTimeType` widens to the zoned timestamp, so an unzoned value is read as local
    # time of the session zone (`TypeCoercionHelper.scala:243-255`). The values depend on the
    # zone, so it is pinned.
    Scenario Outline: <case> in <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT CAST(<expr> AS STRING) AS r FROM VALUES (<c>) AS t(c)
        """
      Then query result
        | r        |
        | <result> |

      Examples:
        | case                              | zone                | c     | expr                                                                                                  | result              |
        | NTZ THEN against TIMESTAMP ELSE   | America/Los_Angeles | true  | CASE WHEN c THEN TIMESTAMP_NTZ'2024-03-09 12:00:00' ELSE TIMESTAMP'2024-03-09 12:00:00+00:00' END     | 2024-03-09 12:00:00 |
        | TIMESTAMP THEN against NTZ ELSE   | America/Los_Angeles | false | CASE WHEN c THEN TIMESTAMP'2024-03-09 12:00:00+00:00' ELSE TIMESTAMP_NTZ'2024-03-09 12:00:00' END     | 2024-03-09 12:00:00 |
        | DATE THEN against TIMESTAMP ELSE  | America/Los_Angeles | true  | CASE WHEN c THEN DATE'2024-03-09' ELSE TIMESTAMP'2024-03-09 12:00:00+00:00' END                        | 2024-03-09 00:00:00 |
        | IF of NTZ and TIMESTAMP           | Asia/Tokyo          | true  | if(c, TIMESTAMP_NTZ'2024-03-09 12:00:00', TIMESTAMP'2024-03-09 12:00:00+00:00')                        | 2024-03-09 12:00:00 |
        | NTZ in the DST gap, LA            | America/Los_Angeles | true  | CASE WHEN c THEN TIMESTAMP_NTZ'2024-03-10 02:30:00' ELSE TIMESTAMP'2024-03-09 12:00:00+00:00' END     | 2024-03-10 03:30:00 |
        | NTZ in the DST overlap, LA        | America/Los_Angeles | true  | CASE WHEN c THEN TIMESTAMP_NTZ'2024-11-03 01:30:00' ELSE TIMESTAMP'2024-03-09 12:00:00+00:00' END     | 2024-11-03 01:30:00 |

  Rule: A nested value is rendered like Spark when its branch widens to a string

    # TODO(known gap): Arrow casts the elements of a nested timestamp to a string itself and writes
    #   `2020-01-01T00:00:00Z`; Spark writes `2020-01-01 00:00:00`. The explicit
    #   `CAST(array(<timestamp>) AS ARRAY<STRING>)` has the same gap, and `main` cannot run this
    #   query at all (it raises a ParseException on 'x').
    @sail-bug
    Scenario: an array of timestamps against an array of strings keeps Spark's timestamp text
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CASE WHEN c THEN array(TIMESTAMP'2020-01-01 00:00:00') ELSE array('x') END AS r
        FROM VALUES (true) AS t(c)
        """
      Then query result
        | r                     |
        | [2020-01-01 00:00:00] |

    Scenario: an array of dates against an array of strings keeps Spark's date text
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CASE WHEN c THEN array(DATE'2020-01-01') ELSE array('x') END AS r
        FROM VALUES (true) AS t(c)
        """
      Then query result
        | r            |
        | [2020-01-01] |

  Rule: The declared nullability is Spark's, not the nullability DataFusion can prove

    # `CaseWhen.nullable` is `branches.exists(_._2.nullable) || elseValue.forall(_.nullable)` and
    # looks at no condition (`conditionalExpressions.scala:191-203`), while DataFusion drops a
    # nullable branch whose condition it can show never holds.
    Scenario Outline: <case> stays nullable
      When query
        """
        SELECT <expr> AS r FROM VALUES (CAST(1 AS INT)), (CAST(NULL AS INT)) AS t(a)
        """
      Then query schema
        """
        root
         |-- r: integer (nullable = true)
        """

      Examples:
        | case                                  | expr                                              |
        | a CASE that tests the column for NULL | CASE WHEN a IS NOT NULL THEN a ELSE 0 END         |
        | an IF that tests the column for NULL  | if(a IS NOT NULL, a, 0)                           |
        | a branch behind a false literal       | CASE WHEN false THEN a ELSE 1 END                 |
        | a nullable branch behind a NULL test  | CASE WHEN a IS NULL THEN 1 ELSE a END             |

  Rule: A branch that is not taken is not evaluated, nested values included

    Scenario Outline: <case> does not evaluate the untaken nested string under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS r FROM VALUES (true) AS t(c)
        """
      Then query result
        | r        |
        | <result> |

      Examples:
        | case                | expr                                                    | result |
        | array of strings    | CASE WHEN c THEN array(1) ELSE array('a') END           | [1]    |
        | struct of strings   | CASE WHEN c THEN named_struct('a', 1) ELSE named_struct('a', 'x') END | {1} |
        | map of strings      | CASE WHEN c THEN map('k', 1) ELSE map('k', 'x') END     | {k -> 1} |
        | IF with arrays      | IF(c, array(1), array('a'))                             | [1]    |

    Scenario Outline: <case> raises when the nested string is the branch taken
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS r FROM VALUES (false) AS t(c)
        """
      Then query error (?s).*CAST_INVALID_INPUT.*

      Examples:
        | case              | expr                                                                  |
        | array of strings  | CASE WHEN c THEN array(1) ELSE array('a') END                         |
        | struct of strings | CASE WHEN c THEN named_struct('a', 1) ELSE named_struct('a', 'x') END |
        | map of strings    | CASE WHEN c THEN map('k', 1) ELSE map('k', 'x') END                   |

  Rule: The rejection reads like Spark's message

    # `DataTypeMismatch` templates and `QueryErrorsBase.ordinalNumber` (`:70-75`): only first,
    # second and third are words, every later index is `<n>th`. The type list is the one of the
    # branches as written, before any string promotion.
    Scenario: the fourth condition is called 4th
      When query
        """
        SELECT CASE WHEN true THEN 1 WHEN true THEN 2 WHEN true THEN 3 WHEN 4 THEN 4 END AS r
        """
      Then query error (?s).*UNEXPECTED_INPUT_TYPE.*The 4th parameter requires the "BOOLEAN" type.*

    Scenario: an untyped NULL predicate does not hide a type mismatch between the values
      When query
        """
        SELECT IF(NULL, 1, ARRAY(1)) AS r
        """
      Then query error (?s).*UNEXPECTED_INPUT_TYPE.*"VOID".*

    Scenario: the type list names the branches as written
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CASE WHEN id = 1 THEN DATE'2020-01-01' WHEN id = 2 THEN 'x' ELSE ARRAY(1) END AS r
        FROM range(1, 3)
        """
      Then query error (?s).*DATA_DIFF_TYPES.*\["DATE", "STRING", "ARRAY<INT>"\].*

    Scenario: a struct type is written with its fields
      When query
        """
        SELECT CASE WHEN true THEN CAST(NULL AS STRUCT<a: INT, b: STRING>) ELSE array(1) END AS r
        """
      Then query error (?s).*DATA_DIFF_TYPES.*"STRUCT<a: INT, b: STRING>", "ARRAY<INT>".*

    Scenario: the malformed value is quoted and the message stops where Spark's does
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN 1 ELSE 'abc' END AS r FROM VALUES (false) AS t(c)
        """
      Then query error (?s)(?!.*If necessary set).*CAST_INVALID_INPUT.*The value 'abc' of the type "STRING" cannot be cast to "BIGINT" because it is malformed\. Correct the value as per the syntax, or change its target type\. Use `try_cast` to tolerate malformed input and return NULL instead\.

    Scenario: a quote inside the malformed value is escaped
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN 1 ELSE concat('it', chr(39), 's') END AS r FROM VALUES (false) AS t(c)
        """
      Then query error (?s).*CAST_INVALID_INPUT.*The value 'it\\'s' of the type.*

  Rule: Struct field names fold the way Java's equalsIgnoreCase does

    # `String.equalsIgnoreCase` compares one char at a time (upper case, then lower case), so a
    # letter whose upper-case form is several chars stays itself (`ß` is not `ss`) and `İ` meets `i`
    # through its simple lower-case mapping (`analysis/package.scala:40`).
    Scenario: sharp s does not pair with a double s
      When query
        """
        SELECT typeof(if(c, named_struct('ß', 1), named_struct('ss', 2L))) AS t FROM VALUES (true) AS t(c)
        """
      Then query error .*DATA_DIFF_TYPES.*

    Scenario: a dotted capital I pairs with a lower-case i
      When query
        """
        SELECT typeof(if(c, named_struct('İ', 1), named_struct('i', 2L))) AS t FROM VALUES (true) AS t(c)
        """
      Then query result
        | t                |
        | struct<İ:bigint> |

  Rule: A string that becomes a number or a boolean is read with Spark's grammar

    # `UTF8String.toLongExact` (`UTF8String.java:1665-1671`), `Double.parseDouble` plus the special
    # literals (`Cast.scala:456-466`, `:1156-1170`), `StringUtils.isTrueString` / `isFalseString`
    # (`StringUtils.scala:72-76`) and `Decimal.fromStringANSI` (`Decimal.scala:629-670`). Arrow's own
    # parsers disagree at the edges, so these rows pin the edges.
    Scenario Outline: <case> reads as <result>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN <string> ELSE <other> END AS r FROM VALUES (true) AS t(c)
        """
      Then query result
        | r        |
        | <result> |

      Examples:
        | case                           | string                  | other                | result |
        | yes                            | 'yes'                   | true                 | true   |
        | upper case Y                   | 'Y'                     | true                 | true   |
        | blanks around true             | ' true '                | false                | true   |
        | the digit zero                 | '0'                     | true                 | false  |
        | a control char before true     | concat(chr(1), 'true')  | false                | true   |
        | blanks around a bigint         | ' 5 '                   | CAST(2 AS BIGINT)    | 5      |
        | a plus sign                    | '+1'                    | CAST(2 AS BIGINT)    | 1      |
        | a vertical tab after a bigint  | concat('1', chr(11))    | CAST(2 AS BIGINT)    | 1      |
        | a control char before a bigint | concat(chr(1), '7')     | CAST(2 AS BIGINT)    | 7      |
        | a double with the d suffix     | '1d'                    | CAST(2 AS DOUBLE)    | 1.0    |
        | a double with the F suffix     | '1.5F'                  | CAST(2 AS DOUBLE)    | 1.5    |
        | blanks around a double         | ' 1.5 '                 | CAST(2 AS DOUBLE)    | 1.5    |
        | an infinity                    | '-inf'                  | CAST(2 AS DOUBLE)    | -Infinity |
        | a NaN                          | 'nan'                   | CAST(2 AS DOUBLE)    | NaN    |
        | a NaN in upper case            | 'NAN'                   | CAST(2 AS DOUBLE)    | NaN    |
        | a signed NaN spelled as Java   | '-NaN'                  | CAST(2 AS DOUBLE)    | NaN    |
        | Infinity spelled as Java       | 'Infinity'              | CAST(2 AS DOUBLE)    | Infinity |

    Scenario Outline: <case> is rejected as malformed
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN <string> ELSE <other> END AS r FROM VALUES (true) AS t(c)
        """
      Then query error (?s).*CAST_INVALID_INPUT.*

      Examples:
        | case                              | string                   | other             |
        | on                                | 'on'                     | true              |
        | a truncated true                  | 'tru'                    | true              |
        | of                                | 'of'                     | true              |
        | a non-breaking space after true   | concat('true', chr(160)) | true              |
        | maybe                             | 'maybe'                  | true              |
        | an empty string as a boolean      | ''                       | true              |
        | a fraction as a bigint            | '1.5'                    | CAST(2 AS BIGINT) |
        | an exponent as a bigint           | '1e3'                    | CAST(2 AS BIGINT) |
        | a hexadecimal bigint              | '0x1'                    | CAST(2 AS BIGINT) |
        | a bigint past the range           | '99999999999999999999'   | CAST(2 AS BIGINT) |
        | a non-breaking space after a bigint | concat('1', chr(160))  | CAST(2 AS BIGINT) |
        | a plus sign before nan            | '+nan'                   | CAST(2 AS DOUBLE) |
        | a minus sign before nan           | '-nan'                   | CAST(2 AS DOUBLE) |

    Scenario Outline: a string against a decimal and an integer reads as <result>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN <string> WHEN NOT c THEN 1 ELSE CAST(1.5 AS DECIMAL(5,2)) END AS r
        FROM VALUES (true) AS t(c)
        """
      Then query result
        | r        |
        | <result> |

      Examples:
        | string    | result |
        | '1.555'   | 1.56   |
        | '-1.555'  | -1.56  |
        | ' 1.5 '   | 1.50   |
        | '1e2'     | 100.00 |

    Scenario: a string past the decimal range is out of range, not malformed
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN '99999999999999999999999' WHEN NOT c THEN 1 ELSE CAST(1.5 AS DECIMAL(5,2)) END AS r
        FROM VALUES (true) AS t(c)
        """
      Then query error (?s).*NUMERIC_VALUE_OUT_OF_RANGE.*

  Rule: A string with more than 38 integral digits is out of the supported range

    # `Decimal.fromStringANSI` rejects it before rounding or fitting the target precision
    # (`Decimal.scala:660-661`, `DataTypeErrors.scala:52-54`); a string that has at most 38
    # integral digits but does not fit the target raises `NUMERIC_VALUE_OUT_OF_RANGE` instead.
    Scenario Outline: <case> is out of the supported range for <target>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN <string> WHEN NOT c THEN 1 ELSE CAST(1 AS <target>) END AS r
        FROM VALUES (true) AS t(c)
        """
      Then query error (?s).*NUMERIC_OUT_OF_SUPPORTED_RANGE.*

      Examples:
        | case                                  | string                                      | target        |
        | the largest 128-bit value rounded up  | '170141183460469231731687303715884105727.5' | DECIMAL(38,0) |
        | the same value for a narrow target    | '170141183460469231731687303715884105727.5' | DECIMAL(22,2) |
        | a negative one                        | '-170141183460469231731687303715884105727.5' | DECIMAL(38,0) |
        | thirty-nine digits                    | '100000000000000000000000000000000000000'   | DECIMAL(38,0) |
        | an exponent that adds the digits      | '1e39'                                      | DECIMAL(22,2) |

    Scenario Outline: <case> is out of range for <target>, not unsupported
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN <string> WHEN NOT c THEN 1 ELSE CAST(1 AS <target>) END AS r
        FROM VALUES (true) AS t(c)
        """
      Then query error (?s).*NUMERIC_VALUE_OUT_OF_RANGE.*

      Examples:
        | case                              | string                                     | target        |
        | thirty-eight digits, narrow type  | '99999999999999999999999999999999999999'   | DECIMAL(22,2) |
        | twenty-three digits               | '99999999999999999999999'                  | DECIMAL(22,2) |

    Scenario: thirty-eight integral digits fit a DECIMAL(38,0)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN '99999999999999999999999999999999999999' WHEN NOT c THEN 1
                    ELSE CAST(1 AS DECIMAL(38,0)) END AS r
        FROM VALUES (true) AS t(c)
        """
      Then query result
        | r                                      |
        | 99999999999999999999999999999999999999 |

    # `BigDecimal` gives zero a precision of 1, so a zero with a large exponent has more than 38
    # integral digits too (`Decimal.scala:660-661`).
    Scenario Outline: the zero <string> is out of the supported range
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN '<string>' WHEN NOT c THEN 1 ELSE CAST(1.5 AS DECIMAL(5,2)) END AS r
        FROM VALUES (true) AS t(c)
        """
      Then query error (?s).*NUMERIC_OUT_OF_SUPPORTED_RANGE.*

      Examples:
        | string       |
        | 0e100        |
        | 0.0e100      |
        | 0e2000000000 |

    Scenario Outline: the zero <string> is read as 0.00
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN '<string>' WHEN NOT c THEN 1 ELSE CAST(1.5 AS DECIMAL(5,2)) END AS r
        FROM VALUES (true) AS t(c)
        """
      Then query result
        | r    |
        | 0.00 |

      Examples:
        | string                                      |
        | 0                                           |
        | -0                                          |
        | 0e-5                                        |
        | 0000000000000000000000000000000000000000000 |

  Rule: A condition that is not a BOOLEAN is named by its SQL, in a message that names the expression

    # `toSQLExpr` of the offending condition is the `inputSql` of `UNEXPECTED_INPUT_TYPE`, and the
    # header quotes the SQL of the whole conditional (`conditionalExpressions.scala:70-89`, `:205-220`;
    # `error-conditions.json:1884-1887`).
    Scenario Outline: <case> names the condition and the expression
      When query
        """
        SELECT <expr> AS r FROM range(1)
        """
      Then query error (?s).*UNEXPECTED_INPUT_TYPE.*Cannot resolve "<sql>" due to data type mismatch.*however "<input>" has the type "<type>".*

      Examples:
        | case                   | expr                          | sql                           | input    | type    |
        | an int literal         | CASE WHEN 1 THEN 1 ELSE 2 END | CASE WHEN 1 THEN 1 ELSE 2 END | 1        | INT     |
        | a column               | CASE WHEN id THEN 1 ELSE 2 END | CASE WHEN id THEN 1 ELSE 2 END | id     | BIGINT  |
        | an expression          | CASE WHEN id + 1 THEN 1 ELSE 2 END | CASE WHEN .id . 1. THEN 1 ELSE 2 END | .id . 1. | BIGINT |
        | an IF with a string    | if('a', true, 1)              | .IF.a, true, 1..           | a        | STRING  |

  Rule: Nothing after a literal TRUE condition is evaluated

    # `CaseWhen.nullable` stops at the first literal TRUE (`conditionalExpressions.scala:191-198`),
    # and nothing after it can run, so a cast that would fail there never does.
    Scenario Outline: <case> returns the value of the literal TRUE branch
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS r FROM VALUES (false) AS t(c)
        """
      Then query result
        | r        |
        | <result> |

      Examples:
        | case                                | expr                                                          | result |
        | an ELSE that cannot be cast         | CASE WHEN true THEN 1 ELSE CAST('abc' AS INT) END             | 1      |
        | a later WHEN that cannot be cast    | CASE WHEN true THEN 1 WHEN c THEN CAST('abc' AS INT) END      | 1      |
        | a TRUE after a WHEN that is false   | CASE WHEN c THEN 1 WHEN true THEN 2 ELSE CAST('abc' AS INT) END | 2    |

  Rule: A boolean string is trimmed byte by byte, like `trimAll`

    # `UTF8String.trimAll` removes single-byte whitespace and control characters only
    # (`UTF8String.java:1000-1016`, used by `StringUtils.scala:103-105`), so a multi-byte space such
    # as U+2003 is not trimmed. The characters are built from their UTF-8 bytes because Spark's
    # `chr(n)` is `chr(n % 256)`.
    Scenario Outline: a boolean with <case> around it is malformed
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN concat(<char>, 'true', <char>) ELSE false END AS r FROM VALUES (true) AS t(c)
        """
      Then query error (?s).*CAST_INVALID_INPUT.*

      Examples:
        | case                    | char                                |
        | an ogham space mark     | decode(unhex('E19A80'), 'UTF-8')    |
        | an em space             | decode(unhex('E28083'), 'UTF-8')    |
        | a line separator        | decode(unhex('E280A8'), 'UTF-8')    |
        | an ideographic space    | decode(unhex('E38080'), 'UTF-8')    |
        | a next line control     | chr(133)                            |
        | a C1 control            | chr(159)                            |
        | a non-breaking space    | chr(160)                            |

    Scenario Outline: a boolean with <case> around it is read
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN concat(<char>, 'true', <char>) ELSE false END AS r FROM VALUES (true) AS t(c)
        """
      Then query result
        | r    |
        | true |

      Examples:
        | case                    | char     |
        | a tab                   | chr(9)   |
        | a unit separator        | chr(31)  |
        | a space                 | chr(32)  |
        | a delete                | chr(127) |

  Rule: A decimal out of range is reported with the number Spark read

    # `Decimal.toPrecision` reports the parsed `BigDecimal`, not the text (`Decimal.scala:363-377`):
    # an exponent is expanded and a plus sign dropped.
    @spark-4
    Scenario Outline: <case> is reported as <value>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN c THEN '<string>' WHEN NOT c THEN 1 ELSE CAST(1.5 AS DECIMAL(22,2)) END AS r
        FROM VALUES (true) AS t(c)
        """
      Then query error (?s).*NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION.  <value> cannot be represented as Decimal.22, 2..*

      Examples:
        | case                    | string                    | value                           |
        | an exponent             | 1e30                      | 1000000000000000000000000000000 |
        | a plus sign             | +99999999999999999999999  | 99999999999999999999999         |
        | a fraction              | 123456789012345678901234.5 | 123456789012345678901234.5     |
        | a negative exponent     | 5e23                      | 500000000000000000000000        |

