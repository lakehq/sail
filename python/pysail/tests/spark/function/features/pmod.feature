@pmod
Feature: pmod (positive modulo) honors ANSI mode and Spark semantics

  # Spark `pmod(a, b)` returns the positive remainder of `a / b` (always in
  # range `[0, |b|)`), unlike `mod` which preserves the sign of `a`.
  # Under `spark.sql.ansi.enabled = true`, a zero divisor raises
  # REMAINDER_BY_ZERO; under ANSI=false it returns NULL.
  #
  # Sail delegates to `datafusion_spark::SparkPmod` which reads
  # `config_options.execution.enable_ansi_mode` at runtime. If the Sail
  # session never propagates the ANSI flag to that config, the ANSI=true
  # error path is dead — `pmod(x, 0)` silently returns NULL.

  Rule: Basic behavior — positive remainder regardless of dividend sign

    Scenario: positive dividend
      When query
        """
        SELECT pmod(10, 3) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: negative dividend returns positive remainder
      When query
        """
        SELECT pmod(-7, 3) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: negative divisor — pmod still uses |b| domain
      When query
        """
        SELECT pmod(10, -3) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: exact multiple gives zero
      When query
        """
        SELECT pmod(-15, 5) AS result
        """
      Then query result
        | result |
        | 0      |

  Rule: NULL operands propagate

    Scenario: NULL dividend returns NULL
      When query
        """
        SELECT pmod(CAST(NULL AS INT), 3) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL divisor returns NULL
      When query
        """
        SELECT pmod(10, CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Divide by zero under ANSI on errors

    Scenario: pmod by 0 errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT pmod(10, 0) AS result
        """
      Then query error .*

    Scenario: pmod negative dividend by 0 errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT pmod(-7, 0) AS result
        """
      Then query error .*

  Rule: Divide by zero under ANSI off returns NULL

    Scenario: pmod by 0 returns NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT pmod(10, 0) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: pmod per-row zero divisor nulls only that row
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT pmod(a, b) AS result FROM VALUES
          (0, 10, 3),
          (1, -7, 0),
          (2, 15, 4)
        AS t(id, a, b) ORDER BY id
        """
      Then query result
        | result |
        | 1      |
        | NULL   |
        | 3      |
