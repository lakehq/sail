Feature: bitmap position functions never raise with ANSI off

  Rule: the implicit BIGINT cast follows the ANSI flag

    # `inputTypes = Seq(LongType)` under `ImplicitCastInputTypes` (`bitmapExpressions.scala:43-48,76-81`):
    # the argument is `Cast(child, LongType)`, which with ANSI off reads a malformed string as NULL
    # (`Cast.scala:889-891`) instead of raising.
    Scenario Outline: <fn> of a malformed string <argument> is NULL with ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <fn>(<argument>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | fn                   | argument              |
        | bitmap_bit_position  | 'abc'                 |
        | bitmap_bit_position  | ''                    |
        | bitmap_bucket_number | 'abc'                 |
        | bitmap_bucket_number | '9223372036854775808' |

    Scenario Outline: <fn> of <argument> past a BIGINT does not raise with ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <fn>(<argument>) IS NULL OR <fn>(<argument>) IS NOT NULL AS answered
        """
      Then query result
        | answered |
        | true     |

      Examples:
        | fn                   | argument                |
        | bitmap_bit_position  | 1E30                    |
        | bitmap_bit_position  | CAST('NaN' AS DOUBLE)   |
        | bitmap_bucket_number | 99999999999999999999BD  |

    # TODO: Spark's non-ANSI cast saturates a DOUBLE past a BIGINT, reads NaN as 0 and wraps a
    #  DECIMAL (`Cast.scala:886-905`); Sail's `try_cast` reads them as NULL.
    Scenario: bitmap_bit_position of a DOUBLE past a BIGINT reads it saturated with ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT bitmap_bit_position(1E30) AS result
        """
      Then query result
        | result |
        | 32766  |

    Scenario Outline: bitmap positions saturate floating-point inputs with ANSI off: <type>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT bitmap_bit_position(CAST(v AS <type>)) AS position,
               bitmap_bucket_number(CAST(v AS <type>)) AS bucket
        FROM VALUES ('NaN'), ('Infinity'), ('-Infinity'), ('1E30'), ('-1E30'),
                    ('1.9'), ('-1.9'), (CAST(NULL AS STRING)) AS t(v)
        """
      Then query result
        | position | bucket           |
        | 0        | 0                |
        | 32766    | 281474976710656  |
        | 0        | -281474976710656 |
        | 32766    | 281474976710656  |
        | 0        | -281474976710656 |
        | 0        | 1                |
        | 1        | 0                |
        | NULL     | NULL             |

      Examples:
        | type   |
        | DOUBLE |
        | FLOAT  |
