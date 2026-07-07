@arithmetic_overflow
Feature: ANSI overflow semantics for the +, -, * operators

  # Under spark.sql.ansi.enabled=true an integral overflow raises
  # ARITHMETIC_OVERFLOW; under ANSI off it wraps two's complement (integrals) or
  # returns NULL (decimals). Paired ANSI on/off per scenario.

  Rule: Integer addition overflow
    Scenario: integer add overflow raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(2147483647 AS INT) + CAST(1 AS INT) AS result
        """
      Then query error (?i)overflow

    Scenario: integer add overflow wraps under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(2147483647 AS INT) + CAST(1 AS INT) AS result
        """
      Then query result
        | result      |
        | -2147483648 |

  Rule: Integer subtraction overflow
    Scenario: integer subtract overflow raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(-2147483648 AS INT) - CAST(1 AS INT) AS result
        """
      Then query error (?i)overflow

    Scenario: integer subtract overflow wraps under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(-2147483648 AS INT) - CAST(1 AS INT) AS result
        """
      Then query result
        | result     |
        | 2147483647 |

  Rule: Integer multiplication overflow
    Scenario: integer multiply overflow raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(2147483647 AS INT) * CAST(2 AS INT) AS result
        """
      Then query error (?i)overflow

    Scenario: integer multiply overflow wraps under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(2147483647 AS INT) * CAST(2 AS INT) AS result
        """
      Then query result
        | result |
        | -2     |

  Rule: Non-finite doubles pass through in every mode (Inf/NaN are not overflow)
    # Float Inf/NaN is a valid IEEE 754 result, not an overflow, so `+ - *` on
    # doubles never raises/NULLs — identical under ANSI on and off.
    Scenario: infinity plus infinity is infinity (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST('Infinity' AS DOUBLE) + CAST('Infinity' AS DOUBLE) AS result
        """
      Then query result
        | result   |
        | Infinity |

    Scenario: infinity plus infinity is infinity (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('Infinity' AS DOUBLE) + CAST('Infinity' AS DOUBLE) AS result
        """
      Then query result
        | result   |
        | Infinity |

    Scenario: infinity minus infinity is NaN (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST('Infinity' AS DOUBLE) - CAST('Infinity' AS DOUBLE) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: infinity minus infinity is NaN (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('Infinity' AS DOUBLE) - CAST('Infinity' AS DOUBLE) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: infinity times zero is NaN (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST('Infinity' AS DOUBLE) * CAST(0.0 AS DOUBLE) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: infinity times zero is NaN (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('Infinity' AS DOUBLE) * CAST(0.0 AS DOUBLE) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: NaN propagates through addition
      When query
        """
        SELECT CAST('NaN' AS DOUBLE) + CAST(2.0 AS DOUBLE) AS result
        """
      Then query result
        | result |
        | NaN    |

  Rule: Decimal overflow raises under ANSI on, NULL under ANSI off
    Scenario: decimal add overflow raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(9e37 AS DECIMAL(38,0)) + CAST(9e37 AS DECIMAL(38,0)) AS result
        """
      Then query error (?i)overflow

    Scenario: decimal add overflow returns NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(9e37 AS DECIMAL(38,0)) + CAST(9e37 AS DECIMAL(38,0)) AS result
        """
      Then query result
        | result |
        | NULL   |
