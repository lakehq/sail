Feature: Range of suffixed numeric literals

  # Spark 4.2.0 checks every suffixed numeric literal against the exact range of its type in the
  # parser (AstBuilder, INVALID_NUMERIC_LITERAL_RANGE). The check is not ANSI-gated.

  Rule: The limits of each integral type are valid literals

    Scenario Outline: the maximum of <case>
      When query
        """
        SELECT <literal> AS result, typeof(<literal>) AS type
        """
      Then query result
        | result   | type   |
        | <result> | <type> |

      Examples:
        | case     | literal              | result              | type     |
        | TINYINT  | 127Y                 | 127                 | tinyint  |
        | SMALLINT | 32767S               | 32767               | smallint |
        | INT      | 2147483647           | 2147483647          | int      |
        | BIGINT   | 9223372036854775807L | 9223372036854775807 | bigint   |

    # Sail parses the digits as a positive number and negates it afterwards, so the positive
    # magnitude of each minimum does not fit: with a suffix the literal is rejected, and without
    # one `-2147483648` is widened to BIGINT.
    @sail-bug
    Scenario Outline: the minimum of <case>
      When query
        """
        SELECT <literal> AS result, typeof(<literal>) AS type
        """
      Then query result
        | result   | type   |
        | <result> | <type> |

      Examples:
        | case     | literal               | result               | type     |
        | TINYINT  | -128Y                 | -128                 | tinyint  |
        | SMALLINT | -32768S               | -32768               | smallint |
        | INT      | -2147483648           | -2147483648          | int      |
        | BIGINT   | -9223372036854775808L | -9223372036854775808 | bigint   |

  # Same verdict in Sail, different message: `invalid argument: tinyint: 128`, which also drops the
  # sign of a negative literal (`-129Y` is reported as `129`).
  Rule: An out-of-range literal is rejected

    @sail-bug
    Scenario Outline: an out-of-range literal: <case>
      When query
        """
        SELECT <literal> AS result
        """
      Then query error \[INVALID_NUMERIC_LITERAL_RANGE\] Numeric literal <shown> is outside the valid range for <type>

      Examples:
        | case                               | literal                | shown                | type     |
        | TINYINT above the maximum          | 128Y                   | 128                  | tinyint  |
        | TINYINT below the minimum          | -129Y                  | -129                 | tinyint  |
        | SMALLINT above the maximum         | 32768S                 | 32768                | smallint |
        | SMALLINT below the minimum         | -32769S                | -32769               | smallint |
        | BIGINT above the maximum           | 9223372036854775808L   | 9223372036854775808  | bigint   |
        | BIGINT below the minimum           | -9223372036854775809L  | -9223372036854775809 | bigint   |
        | FLOAT far above the maximum        | 3.5E38F                | 3.5E38               | float    |
        | DOUBLE far above the maximum       | 1.8E308D               | 1.8E308              | double   |
        | DOUBLE far below the minimum       | -1.8E308D              | -1.8E308             | double   |
        | unsuffixed exponent past DOUBLE    | 1.8E308                | 1.8E308              | double   |

  # Spark compares the exact decimal value of the literal with Float.MaxValue / Double.MaxValue.
  # `3.4028235E38` is slightly above Float.MaxValue (3.4028234663852886E38), so Spark rejects it.
  # Sail parses it into an f32, which rounds to the maximum, and only complains once the result
  # becomes infinite, so it accepts these three literals.
  Rule: A literal just past the maximum is rejected even though it would round to it

    Scenario: the largest FLOAT written with enough digits is accepted
      When query
        """
        SELECT 3.4028234E38F = CAST('3.4028235E38' AS FLOAT) AS result
        """
      Then query result
        | result |
        | true   |

    @sail-bug
    Scenario Outline: a literal past the maximum: <case>
      When query
        """
        SELECT <literal> AS result
        """
      Then query error \[INVALID_NUMERIC_LITERAL_RANGE\] Numeric literal <shown> is outside the valid range for <type>

      Examples:
        | case                                  | literal                 | shown                  | type   |
        | FLOAT rounding up to the maximum      | 3.4028235E38F           | 3.4028235E38           | float  |
        | FLOAT rounding down to the minimum    | -3.4028235E38F          | -3.4028235E38          | float  |
        | DOUBLE rounding down to the maximum   | 1.7976931348623158E308D | 1.7976931348623158E308 | double |

    @sail-bug
    Scenario: the range check does not depend on ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 3.4028235E38F AS result
        """
      Then query error \[INVALID_NUMERIC_LITERAL_RANGE\]

  Rule: A literal below the smallest magnitude underflows to zero instead of raising

    Scenario Outline: an underflowing literal: <case>
      When query
        """
        SELECT <literal> AS result
        """
      Then query result
        | result |
        | 0.0    |

      Examples:
        | case                     | literal  |
        | FLOAT below subnormals   | 1.0E-46F |
        | DOUBLE below subnormals  | 1.0E-400D |
        | unsuffixed tiny exponent | 1.0E-400 |
