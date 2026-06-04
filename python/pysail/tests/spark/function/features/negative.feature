@negative
Feature: unary minus (negative) honors ANSI overflow semantics

  Rule: Negating the minimum integral value overflows

    Scenario: negate INT_MIN errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT -CAST(-2147483648 AS INT) AS result
        """
      Then query error (?i)overflow

    Scenario: negate INT_MIN wraps under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT -CAST(-2147483648 AS INT) AS result
        """
      Then query result
        | result      |
        | -2147483648 |

    Scenario: negate BIGINT_MIN errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT -CAST(-9223372036854775808 AS BIGINT) AS result
        """
      Then query error (?i)overflow

    Scenario: negate BIGINT_MIN wraps under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT -CAST(-9223372036854775808 AS BIGINT) AS result
        """
      Then query result
        | result               |
        | -9223372036854775808 |

    Scenario: negate SMALLINT_MIN errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT -CAST(-32768 AS SMALLINT) AS result
        """
      Then query error (?i)overflow

    Scenario: negate SMALLINT_MIN wraps under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT -CAST(-32768 AS SMALLINT) AS result
        """
      Then query result
        | result |
        | -32768 |

    Scenario: negate TINYINT_MIN errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT -CAST(-128 AS TINYINT) AS result
        """
      Then query error (?i)overflow

    Scenario: negate TINYINT_MIN wraps under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT -CAST(-128 AS TINYINT) AS result
        """
      Then query result
        | result |
        | -128   |

  Rule: Ordinary negation is unaffected by ANSI mode

    Scenario: negate a positive integer
      When query
        """
        SELECT -CAST(5 AS INT) AS result
        """
      Then query result
        | result |
        | -5     |

    Scenario: double negation returns the original value
      When query
        """
        SELECT -(-CAST(5 AS INT)) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: negate a double
      When query
        """
        SELECT -CAST(1.5 AS DOUBLE) AS result
        """
      Then query result
        | result |
        | -1.5   |

    Scenario: negate a decimal
      When query
        """
        SELECT -CAST(1.50 AS DECIMAL(10,2)) AS result
        """
      Then query result
        | result |
        | -1.50  |

    Scenario: negate NULL returns NULL
      When query
        """
        SELECT -CAST(NULL AS INT) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Floating-point negation never overflows

    Scenario: negate a float
      When query
        """
        SELECT -CAST(1.5 AS FLOAT) AS result
        """
      Then query result
        | result |
        | -1.5   |

    Scenario: negate positive zero yields negative zero
      When query
        """
        SELECT -CAST(0.0 AS DOUBLE) AS result
        """
      Then query result
        | result |
        | -0.0   |

    Scenario: negate NaN returns NaN
      When query
        """
        SELECT -CAST('NaN' AS DOUBLE) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: negate Infinity returns negative Infinity
      When query
        """
        SELECT -CAST('Infinity' AS DOUBLE) AS result
        """
      Then query result
        | result    |
        | -Infinity |

    Scenario: negate a float column negates each row
      When query
        """
        SELECT id, -v AS result FROM VALUES
          (0, CAST(1.5 AS DOUBLE)),
          (1, CAST(-2.5 AS DOUBLE)),
          (2, CAST(0.0 AS DOUBLE)),
          (3, CAST(NULL AS DOUBLE))
        AS t(id, v) ORDER BY id
        """
      Then query result
        | id | result |
        | 0  | -1.5   |
        | 1  | 2.5    |
        | 2  | -0.0   |
        | 3  | NULL   |

    Scenario: negate NaN is NaN by predicate
      When query
        """
        SELECT isnan(-CAST('NaN' AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: negate Infinity equals negative infinity by predicate
      When query
        """
        SELECT -CAST('Infinity' AS DOUBLE) = CAST('-Infinity' AS DOUBLE) AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Day-time interval negation

    Scenario: negate a day-time interval
      When query
        """
        SELECT -(INTERVAL '5' SECOND) = INTERVAL '-5' SECOND AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: negate a negative interval
      When query
        """
        SELECT -(INTERVAL '-3' DAY) = INTERVAL '3' DAY AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: negative function negates an interval
      When query
        """
        SELECT negative(INTERVAL '5' SECOND) = INTERVAL '-5' SECOND AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: double negation of an interval is the original
      When query
        """
        SELECT -(-(INTERVAL '5' SECOND)) = INTERVAL '5' SECOND AS result
        """
      Then query result
        | result |
        | true   |

  Rule: String arguments coerce to double

    Scenario: negate a numeric string coerces to double
      When query
        """
        SELECT negative('1.5') AS result
        """
      Then query result
        | result |
        | -1.5   |

    Scenario: negate a negative numeric string
      When query
        """
        SELECT negative('-3') AS result
        """
      Then query result
        | result |
        | 3.0    |

    Scenario: negate a non-numeric string returns NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT negative('abc') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: negate a non-numeric string errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT negative('abc') AS result
        """
      Then query error (?i)cast

  Rule: Negating the maximum decimal overflows its precision

    # @sail-bug: Spark promotes `-DECIMAL(38,0)` to DECIMAL(39,0), which exceeds
    # the max precision, so negating the maximum value raises
    # NUMERIC_VALUE_OUT_OF_RANGE. Sail keeps DECIMAL(38,0) and returns the value.
    @sail-bug
    Scenario: negate the maximum DECIMAL(38,0) errors
      When query
        """
        SELECT -CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) AS result
        """
      Then query error (?i)out.of.range
