@cast
Feature: cast output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to cast yields the schema Spark declares
      When query
        """
        SELECT cast('10' as int) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Decimal to double rounds once, from the exact value

    # Spark widens with `Decimal.toDouble` = `toBigDecimal.doubleValue`
    # (Decimal.scala:245): ONE correctly-rounded step from the exact decimal.
    # Sail computes `unscaled as f64 / 10^scale`, which rounds twice — once
    # converting the unscaled integer (inexact above 2^53) and again dividing by
    # an inexact power of ten. The two agree while the unscaled value fits 53
    # bits and drift apart beyond it, so the divergence is data-dependent rather
    # than type-dependent: DECIMAL(20,2) is fine, DECIMAL(38,2) is not.
    #
    # Found while porting `percentile_approx`, where the widening error
    # propagated into the returned quantile. That function now widens decimals
    # itself; this is the underlying CAST, still divergent.

    @sail-bug
    Scenario Outline: decimal to double keeps the exactly-rounded value: <case>
      When query
        """
        SELECT CAST(CAST(<literal> AS DECIMAL(38,<scale>)) AS DOUBLE) AS result
        """
      Then query result
        | result     |
        | <expected> |

      Examples:
        | case                                  | literal                                 | scale | expected              |
        | a wide integral part loses two digits | 123456789012345678.90                   | 2     | 1.2345678901234568E17 |
        | an exact one comes back below one     | 1                                       | 37    | 1.0                   |
        | a long fraction loses its last digit  | 1.23456789012345678901                  | 20    | 1.2345678901234567    |
        | the smallest DECIMAL(38,37) step      | 0.0000000000000000000000000000000000001 | 37    | 1.0E-37               |

    # The scenario above asserts the *rendered* value, so it folds two separate
    # divergences into one: the rounding, and Sail printing `1.2345678901234568e+17`
    # where Spark prints `1.2345678901234568E17`. This one asserts the value --
    # two doubles compare `=` only when their bits agree -- so it isolates the
    # rounding and goes green when that half is fixed, whatever the rendering does.
    #
    # The reference is the decimal literal itself parsed as a double, not a
    # precomputed constant: string -> double is correctly rounded in both engines,
    # so `CAST(d AS DOUBLE)` must land on exactly the double that parsing the same
    # text gives. That is the whole specification, with no magic numbers.
    @sail-bug
    Scenario Outline: decimal to double lands on the correctly rounded double: <case>
      When query
        """
        SELECT CAST(CAST(<literal> AS DECIMAL(38,<scale>)) AS DOUBLE)
                 = CAST('<literal>' AS DOUBLE) AS matches
        """
      Then query result
        | matches |
        | true    |

      Examples:
        | case                                  | literal                                 | scale |
        | a wide integral part loses two digits | 123456789012345678.90                   | 2     |
        | an exact one comes back below one     | 1                                       | 37    |
        | a long fraction loses its last digit  | 1.23456789012345678901                  | 20    |
        | the smallest DECIMAL(38,37) step      | 0.0000000000000000000000000000000000001 | 37    |

  Rule: Decimal to float rounds once too

    # Spark narrows with `Decimal.toFloat` = `toBigDecimal.floatValue`
    # (Decimal.scala:247), one correctly-rounded step straight to `float`. Sail
    # rounds to `double` first and then to `float`, and the two roundings
    # disagree with a single one: a decimal just above a float midpoint can
    # collapse onto that midpoint in `double`, and half-even then sends it the
    # wrong way.
    #
    # This is a SEPARATE divergence from the double one above -- fixing the
    # `double` widening leaves these untouched, because the second rounding is
    # what breaks them. The inputs below are exactly the ones where
    # `BigDecimal.floatValue()` and `(float) BigDecimal.doubleValue()` disagree.
    # Reference is the same literal parsed as a float, for the same reason.

    @sail-bug
    Scenario Outline: decimal to float lands on the correctly rounded float: <case>
      When query
        """
        SELECT CAST(CAST(<literal> AS DECIMAL(38,<scale>)) AS FLOAT)
                 = CAST('<literal>' AS FLOAT) AS matches
        """
      Then query result
        | matches |
        | true    |

      Examples:
        | case                                  | literal                          | scale |
        | a midpoint reached from just above    | 13631072.500000000514758830      | 18    |
        | a tiny excess over an exact integer   | 72073620.000000000000000582908005 | 24   |
        | the same, negative                    | -32733169.00000000000957536840   | 20    |

    # Control: inside the exactly representable range the two paths already
    # agree, so these must pass with or without the fix. `10^k` is exact in a
    # float only up to k = 10 -- `5^10` is the largest power of five that fits
    # the 24-bit significand -- so the guard is much tighter than for a double.
    Scenario Outline: decimal to float is exact inside the representable range: <case>
      When query
        """
        SELECT CAST(CAST(<literal> AS DECIMAL(38,<scale>)) AS FLOAT)
                 = CAST('<literal>' AS FLOAT) AS matches
        """
      Then query result
        | matches |
        | true    |

      Examples:
        | case                        | literal      | scale |
        | a small scaled value        | 3.14159      | 5     |
        | the last exact power of ten | 0.0000000001 | 10    |
        | a negative one             | -12345.678   | 3     |
