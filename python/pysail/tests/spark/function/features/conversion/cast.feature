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
    # Asserted by EQUALITY against the expected double, not by rendering. Each
    # of these is off by exactly one ULP, and Sail also prints doubles
    # differently from Spark (see the rule below) — comparing the printed form
    # would conflate the two, and this test would stay red after the value is
    # fixed.
    #
    # Found while porting `percentile_approx`, where the widening error
    # propagated into the returned quantile. That function now widens decimals
    # itself; this is the underlying CAST, still divergent.

    @sail-bug
    Scenario Outline: decimal to double is off by one ULP: <case>
      When query
        """
        SELECT CAST(CAST(<literal> AS DECIMAL(38,<scale>)) AS DOUBLE) = <expected> AS matches
        """
      Then query result
        | matches |
        | true    |

      Examples:
        | case                                  | literal                                 | scale | expected              |
        | a wide integral part loses two digits | 123456789012345678.90                   | 2     | 1.2345678901234568E17 |
        | an exact one comes back below one     | 1                                       | 37    | 1.0D                  |
        | a long fraction loses its last digit  | 1.23456789012345678901                  | 20    | 1.2345678901234567D   |
        | the smallest DECIMAL(38,37) step      | 0.0000000000000000000000000000000000001 | 37    | 1.0E-37               |

  Rule: Doubles print the way Spark prints them

    # Independent of any value bug: for doubles both engines hold BIT-IDENTICALLY
    # (`collect` agrees), Spark renders `1.0E17` and Sail renders `1e17` —
    # uppercase exponent and a `.0` mantissa versus lowercase and none. Only the
    # printed form differs, so these use the rendered value on purpose; the
    # equality checks in the rule above stay rendering-independent.

    @sail-bug
    Scenario Outline: a double renders with an uppercase exponent: <case>
      When query
        """
        SELECT CAST(<literal> AS DOUBLE) AS result
        """
      Then query result
        | result     |
        | <expected> |

      Examples:
        | case              | literal | expected |
        | a large magnitude | 1e17    | 1.0E17   |
        | a scaled mantissa | 1.5e20  | 1.5E20   |
        | a small magnitude | 1e-37   | 1.0E-37  |
