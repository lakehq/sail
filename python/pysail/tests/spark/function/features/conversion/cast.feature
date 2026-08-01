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
    #
    # Fixed upstream in https://github.com/apache/arrow-rs/pull/10509. Drop the
    # @sail-bug tag once Sail picks up an arrow release carrying it — the
    # assertions are equality-based, so they flip to green on the version bump
    # with no edit.

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
        | case                      | literal                    | expected    |
        | a large magnitude         | 1e17                       | 1.0E17      |
        | a scaled mantissa         | 1.5e20                     | 1.5E20      |
        | a small magnitude         | 1e-37                      | 1.0E-37     |
        | a float past the 1e7 mark | CAST(16777216 AS FLOAT)    | 1.6777216E7 |

  Rule: Decimal to float narrows in one step

    # The float32 path had the same double-rounding shape as the double one —
    # compute an f64, then narrow with `as f32` — and it is fixed by the same
    # upstream change, https://github.com/apache/arrow-rs/pull/10509.
    #
    # These agree today: f32's coarser precision absorbs the f64 error for every
    # case that could be constructed, including the 2^24 boundary. They are here
    # as regression cover for the arrow bump, which rewrites this path.
    #
    # Asserted by equality, not by rendering: the VALUES match bit for bit
    # (`5bdb4da6`, `4b800000`) while the printed forms do not — Spark prints
    # `1.6777216E7`, Sail prints `16777216.0`. That is the separate display gap
    # in the rule below, and comparing text would conflate the two.

    Scenario Outline: decimal to float: <case>
      When query
        """
        SELECT CAST(CAST(<literal> AS DECIMAL(38,<scale>)) AS FLOAT) = <expected> AS matches
        """
      Then query result
        | matches |
        | true    |

      Examples:
        | case                              | literal               | scale | expected            |
        | a wide integral part              | 123456789012345678.90 | 2     | CAST(1.23456791E17 AS FLOAT) |
        | an exact one at maximum scale     | 1                     | 37    | CAST(1.0 AS FLOAT)  |
        | a value inside the exact range    | 12345.67              | 2     | CAST(12345.67 AS FLOAT) |
        | the 2^24 float precision boundary | 16777217              | 0     | CAST(16777216 AS FLOAT) |
