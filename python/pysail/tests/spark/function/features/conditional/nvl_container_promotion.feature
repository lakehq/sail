Feature: nvl over containers whose leaves need a promotion

  Rule: nvl of two containers never fails where Spark types them

    # `findTypeForComplex` rejects MAP key pairs whose implicit cast can return NULL
    # (`TypeCoercionHelper.scala:149-155`). `Cast.forceNullable` distinguishes TIMESTAMP from
    # TIMESTAMP_NTZ, so DATE must not silently coerce this key pair (`Cast.scala:439-443`).
    Scenario: nvl refuses map keys of date and timestamp_ntz
      When query
        """
        SELECT nvl(
          map(DATE'2024-01-01', 1),
          map(TIMESTAMP_NTZ'2024-01-01 00:00:00', 2)
        ) AS value
        """
      Then query error (?i)DATA_DIFF_TYPES|Cannot automatically convert

    # The MAP-key safety check applies to keys only. Spark still widens DATE map values to
    # TIMESTAMP (`TypeCoercion.scala:94`), so rejecting this pair would turn a valid query into a
    # regression.
    Scenario: nvl widens map values from date to timestamp
      When query
        """
        SELECT CAST(nvl(map('a', DATE'2020-01-01'), map('a', TIMESTAMP'2020-01-01 00:00:00')) AS STRING) AS result
        """
      Then query result
        | result                     |
        | {a -> 2020-01-01 00:00:00} |

    # Two day-time ranges widen to their covering range, rather than to the default DAY TO SECOND
    # (`TypeCoercion.scala:96-99`).
    Scenario: nvl widens map day-time interval values to their covering range
      When query
        """
        SELECT CAST(nvl(map('a', INTERVAL '1' DAY), map('a', INTERVAL '2' HOUR)) AS STRING) AS result
        """
      Then query result
        | result                             |
        | {a -> INTERVAL '1 00' DAY TO HOUR} |

    # `Nvl` is `Coalesce(Seq(left, right))` (`nullExpressions.scala:246`), and `coalesce` widens two
    # containers leaf by leaf (`TypeCoercionHelper.scala:141`, string promotion included,
    # `TypeCoercion.scala:168`), so every pair below is answered by Spark.
    Scenario Outline: nvl of <case> is answered with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <fn>(<left>, <right>) IS NOT NULL AS answered
        """
      Then query result
        | answered |
        | true     |

      Examples:
        | case                     | ansi  | fn     | left                                  | right                                 |
        | an int and a string list | false | nvl    | array(1)                              | array('a')                            |
        | an int and a string list | true  | ifnull | array(1)                              | array('a')                            |
        | a date and a string list | false | nvl    | array(DATE'2024-01-01')               | array('2024-01-02')                   |
        | a date and a string list | true  | ifnull | array(DATE'2024-01-01')               | array('2024-01-02')                   |

    # Spark widens map values recursively, just as it widens list elements and struct fields.
    Scenario Outline: nvl of <case> is answered with ANSI <ansi> despite the map values
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <fn>(<left>, <right>) IS NOT NULL AS answered
        """
      Then query result
        | answered |
        | true     |

      Examples:
        | case                     | ansi  | fn     | left                                  | right                                 |
        | a date and a string map  | false | nvl    | map('k', DATE'2024-01-01')            | map('k', '2024-01-02')                |
        | a date and a string map  | true  | ifnull | map('k', DATE'2024-01-01')            | map('k', '2024-01-02')                |

    # `findWiderTypeForTwo` recurses into a list and a struct, widening leaf by leaf and keeping the
    # left side's field names (`TypeCoercionHelper.scala:141`), so a list of structs whose leaves
    # widen and two structs whose names differ only by case both type.
    Scenario Outline: nvl of <case> is answered with ANSI <ansi> despite the structs
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <fn>(<left>, <right>) IS NOT NULL AS answered
        """
      Then query result
        | answered |
        | true     |

      Examples:
        | case                           | ansi  | fn     | left                        | right                        |
        | a list of widening structs     | false | nvl    | array(named_struct('a', 1)) | array(named_struct('a', 2L)) |
        | a list of widening structs     | true  | ifnull | array(named_struct('a', 1)) | array(named_struct('a', 2L)) |
        | structs differing only by case | false | nvl    | named_struct('a', 1)        | named_struct('A', 2)         |
        | structs differing only by case | true  | ifnull | named_struct('a', 1)        | named_struct('A', 2)         |

    # The promoted container remains an ARRAY, which arithmetic refuses as Spark does.
    Scenario: nvl of an int and a string list is an ARRAY, refused as an arithmetic operand
      When query
        """
        SELECT 2 / nvl(array(1), array('a')) AS result
        """
      Then query error (?i)cannot resolve

  Rule: two structs type only when their field names match

    Scenario: ANSI promotion of a string array records the nullable cast element
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT if(true, array('1'), array(2)) AS value
        """
      Then query schema
        """
        root
         |-- value: array (nullable = false)
         |    |-- element: long (containsNull = true)
        """

    # `findTypeForComplex` adds `Cast.forceNullable` to the nested flag. Spark's decimal target
    # loses fractional digits here, so `canNullSafeCastToDecimal` is false (`Cast.scala:445`).
    Scenario: decimal array promotion records a nullable narrowing decimal cast
      When query
        """
        SELECT if(true, array(CAST(1 AS DECIMAL(20,10))), array(CAST(1 AS DECIMAL(38,30)))) AS value
        """
      Then query schema
        """
        root
         |-- value: array (nullable = false)
         |    |-- element: decimal(38,28) (containsNull = true)
        """

    # `findWiderTypeForDecimal` first turns BIGINT into DECIMAL(20,0), then bounds the common
    # DECIMAL(50,30) to DECIMAL(38,18) (`TypeCoercionHelper.scala:190-193`).
    Scenario: integral and decimal arrays preserve integral digits when their common decimal caps
      When query
        """
        SELECT if(true, array(CAST(1 AS BIGINT)), array(CAST(1 AS DECIMAL(38,30)))) AS value
        """
      Then query schema
        """
        root
         |-- value: array (nullable = false)
         |    |-- element: decimal(38,18) (containsNull = true)
        """

    Scenario: a narrowing decimal cast makes an array element nullable on its own
      When query
        """
        SELECT array(CAST(1 AS DECIMAL(38,30))) AS value
        """
      Then query schema
        """
        root
         |-- value: array (nullable = false)
         |    |-- element: decimal(38,30) (containsNull = true)
        """

    # Every DECIMAL is a Spark `FractionalType`, so a cast to an integral type can turn a valid
    # finite value into NULL. `Cast.forceNullable` therefore marks the ARRAY element nullable
    # (`Cast.scala:445-446`).
    Scenario: a decimal cast to tinyint makes an array element nullable
      When query
        """
        SELECT array(CAST(CAST(99999 AS DECIMAL(5,0)) AS TINYINT)) AS value
        """
      Then query schema
        """
        root
         |-- value: array (nullable = false)
         |    |-- element: byte (containsNull = true)
        """

    Scenario: a bigint array has non-null elements before conditional promotion
      When query
        """
        SELECT array(CAST(1 AS BIGINT)) AS value
        """
      Then query schema
        """
        root
         |-- value: array (nullable = false)
         |    |-- element: long (containsNull = false)
        """

    Scenario: ANSI promotion refuses a map key cast that could produce NULL
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT nvl(map('1', 1), map(2, 2)) AS value
        """
      Then query error (?i)DATA_DIFF_TYPES

    Scenario: IF reports Spark's data-difference error template
      When query
        """
        SELECT if(true, 1, DATE'2024-01-01') AS value
        """
      Then query error (?i)Input to `if` should all be the same type, but it's \["INT", "DATE"\]

    Scenario: CASE reports Spark's analyzed function name in a data-difference error
      When query
        """
        SELECT CASE WHEN true THEN 1 ELSE DATE'2024-01-01' END AS value
        """
      Then query error (?i)Input to `casewhen` should all be the same type, but it's \["INT", "DATE"\]

    # `CaseWhen.checkInputDataTypes` contributes the complete SQL expression to the surrounding
    # analysis error, while the function parameter remains `casewhen`
    # (`conditionalExpressions.scala:220-228`).
    Scenario: CASE names its whole expression in a data-difference error
      When query
        """
        SELECT CASE WHEN true THEN array(1) ELSE 1 END AS value
        """
      Then query error (?i)Cannot resolve "CASE WHEN true THEN array[(]1[)] ELSE 1 END"

    Scenario: NVL reports its coalesce replacement in a data-difference error
      When query
        """
        SELECT nvl(1, DATE'2024-01-01') AS value
        """
      Then query error (?i)Input to `coalesce` should all be the same type

    # Spark fills DATATYPE_MISMATCH's `<sqlExpr>` parameter with the complete resolved
    # expression, including nvl's coalesce replacement (`error-conditions.json`).
    Scenario Outline: <fn> names the whole expression in its data-difference error
      When query
        """
        SELECT <expression> AS value
        """
      Then query error (?i)Cannot resolve "<rendered>"

      Examples:
        | fn       | expression                    | rendered                                 |
        | coalesce | coalesce(1, DATE'2024-01-01') | coalesce[(]1, DATE '2024-01-01'[)]       |
        | nvl      | nvl(1, DATE'2024-01-01')      | coalesce[(]1, DATE '2024-01-01'[)]       |
        | if       | if(true, 1, DATE'2024-01-01') | [(]IF[(]true, 1, DATE '2024-01-01'[)][)] |

    Scenario: case-sensitive ANSI nvl refuses struct fields with different case
      Given config spark.sql.ansi.enabled = true
      Given config spark.sql.caseSensitive = true
      When query
        """
        SELECT nvl(named_struct('A', 1), named_struct('a', 2)) AS value
        """
      Then query error (?i)DATA_DIFF_TYPES

    # Spark's default resolver is Unicode case-insensitive, not ASCII-only
    # (`SQLConf.resolver`, `caseSensitiveAnalysis`).
    Scenario: case-insensitive nvl widens struct fields with Unicode case variants
      Given config spark.sql.caseSensitive = false
      When query
        """
        SELECT CAST(nvl(named_struct('Ä', 1), named_struct('ä', 2)) AS STRING) AS result
        """
      Then query result
        | result |
        | {1}    |

    # `findTypeForComplex` pairs struct fields through `SQLConf.get.resolver` and returns None when a
    # pair of names does not match (`TypeCoercionHelper.scala:164-176`), so Spark refuses the pair
    # instead of renaming it; the resolver is case-insensitive by default, and an extra field is a
    # mismatch too.
    Scenario Outline: nvl of <case> is refused with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <fn>(<left>, <right>) AS v
        """
      Then query error (?i)cannot resolve

      Examples:
        | case                            | ansi  | fn       | left                        | right                                |
        | structs with different names    | false | nvl      | named_struct('a', 1)        | named_struct('b', 2L)                |
        | structs with different names    | true  | ifnull   | named_struct('a', 1)        | named_struct('b', 2L)                |
        | a struct with an extra field    | false | nvl      | named_struct('a', 1)        | named_struct('a', 2L, 'b', 3)        |
        | lists of structs with different names | false | nvl | array(named_struct('a', 1))  | array(named_struct('b', 2L))         |
        | lists of structs with different names | true  | nvl | array(named_struct('a', 1))  | array(named_struct('b', 2L))         |
        | a struct beside an int          | false | nvl      | named_struct('a', 1)        | 2                                    |
