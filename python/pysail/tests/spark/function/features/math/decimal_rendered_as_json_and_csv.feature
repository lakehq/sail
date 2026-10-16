Feature: to_json and to_csv render a decimal the way Spark does

  # `to_json` and `to_csv` are renderers of their own: neither goes through `show` nor
  # through `CAST(... AS STRING)`, so a decimal can print right in every lens of
  # `decimal_stored_and_printed.feature` and still be written wrong here. A decimal carries
  # its scale, so DECIMAL(10,2) holding 1.5 is written `1.50`, and a value below 1e-6 is
  # written with an exponent because `BigDecimal.toString` uses one.
  # The two tiny-decimal rows are a deliberate pair, not a duplicate: `CAST(... AS STRING)`
  # renders a decimal plainly only under ANSI (`Cast.useDecimalPlainString` is `ansiEnabled`,
  # `Cast.scala:682`), and the pair pins that these two renderers do NOT follow it -- both
  # write the exponent form whichever way ANSI is set.
  # Measured on the Spark 4.2 JVM over Spark Connect.

  Rule: to_json renders a decimal the way Spark does

    @sail-bug
    Scenario: to_json of a decimal keeps its scale
      When query
        """
        SELECT to_json(named_struct('v', CAST(1.5 AS DECIMAL(10,2)))) AS result
        """
      Then query result collected
        | result |
        | {"v":1.50} |

    @sail-bug
    Scenario: to_json of a negative zero decimal
      When query
        """
        SELECT to_json(named_struct('v', CAST(-0.00 AS DECIMAL(5,2)))) AS result
        """
      Then query result collected
        | result |
        | {"v":0.00} |

    Scenario: to_json of a decimal with scale zero
      When query
        """
        SELECT to_json(named_struct('v', CAST(42 AS DECIMAL(10,0)))) AS result
        """
      Then query result collected
        | result |
        | {"v":42} |

    @sail-bug
    Scenario: to_json of a decimal division
      When query
        """
        SELECT to_json(named_struct('v', CAST(1 AS DECIMAL(10,0)) / CAST(3 AS DECIMAL(10,0)))) AS result
        """
      Then query result collected
        | result |
        | {"v":0.33333333333} |

    @sail-bug
    Scenario: to_json of a decimal product
      When query
        """
        SELECT to_json(named_struct('v', CAST(1.5 AS DECIMAL(10,2)) * CAST(2.5 AS DECIMAL(10,2)))) AS result
        """
      Then query result collected
        | result |
        | {"v":3.7500} |

    @sail-bug
    Scenario: to_json of a decimal sum
      When query
        """
        SELECT to_json(named_struct('v', CAST(1.5 AS DECIMAL(10,2)) + CAST(2.5 AS DECIMAL(10,2)))) AS result
        """
      Then query result collected
        | result |
        | {"v":4.00} |

    @sail-bug
    Scenario: to_json of a wide decimal
      When query
        """
        SELECT to_json(named_struct('v', CAST('12345678901234567890.123' AS DECIMAL(38,10)))) AS result
        """
      Then query result collected
        | result |
        | {"v":12345678901234567890.1230000000} |

    @sail-bug
    Scenario: to_json of a decimal at full precision
      When query
        """
        SELECT to_json(named_struct('v', CAST('1.2345678901234567890123456789012345678' AS DECIMAL(38,37)))) AS result
        """
      Then query result collected
        | result |
        | {"v":1.2345678901234567890123456789012345678} |

    Scenario: to_json of a decimal cast to double
      When query
        """
        SELECT to_json(named_struct('v', CAST(CAST(0.1 AS DECIMAL(38,18)) AS DOUBLE))) AS result
        """
      Then query result collected
        | result |
        | {"v":0.1} |

    @sail-bug
    Scenario: to_json of a tiny decimal with ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_json(named_struct('v', CAST(0.0000000001 AS DECIMAL(38,18)))) AS result
        """
      Then query result collected
        | result |
        | {"v":1.00000000E-10} |

    @sail-bug
    Scenario: to_json of a tiny decimal with ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_json(named_struct('v', CAST(0.0000000001 AS DECIMAL(38,18)))) AS result
        """
      Then query result collected
        | result |
        | {"v":1.00000000E-10} |

    Scenario: to_json of a rounded decimal cast
      When query
        """
        SELECT to_json(named_struct('v', CAST(2.345 AS DECIMAL(10,2)))) AS result
        """
      Then query result collected
        | result |
        | {"v":2.35} |

  Rule: to_csv renders a decimal the way Spark does

    Scenario: to_csv of a decimal keeps its scale
      When query
        """
        SELECT to_csv(named_struct('v', CAST(1.5 AS DECIMAL(10,2)))) AS result
        """
      Then query result collected
        | result |
        | 1.50 |

    Scenario: to_csv of a negative zero decimal
      When query
        """
        SELECT to_csv(named_struct('v', CAST(-0.00 AS DECIMAL(5,2)))) AS result
        """
      Then query result collected
        | result |
        | 0.00 |

    Scenario: to_csv of a decimal with scale zero
      When query
        """
        SELECT to_csv(named_struct('v', CAST(42 AS DECIMAL(10,0)))) AS result
        """
      Then query result collected
        | result |
        | 42 |

    @sail-bug
    Scenario: to_csv of a decimal division
      When query
        """
        SELECT to_csv(named_struct('v', CAST(1 AS DECIMAL(10,0)) / CAST(3 AS DECIMAL(10,0)))) AS result
        """
      Then query result collected
        | result |
        | 0.33333333333 |

    Scenario: to_csv of a decimal product
      When query
        """
        SELECT to_csv(named_struct('v', CAST(1.5 AS DECIMAL(10,2)) * CAST(2.5 AS DECIMAL(10,2)))) AS result
        """
      Then query result collected
        | result |
        | 3.7500 |

    Scenario: to_csv of a decimal sum
      When query
        """
        SELECT to_csv(named_struct('v', CAST(1.5 AS DECIMAL(10,2)) + CAST(2.5 AS DECIMAL(10,2)))) AS result
        """
      Then query result collected
        | result |
        | 4.00 |

    Scenario: to_csv of a wide decimal
      When query
        """
        SELECT to_csv(named_struct('v', CAST('12345678901234567890.123' AS DECIMAL(38,10)))) AS result
        """
      Then query result collected
        | result |
        | 12345678901234567890.1230000000 |

    Scenario: to_csv of a decimal at full precision
      When query
        """
        SELECT to_csv(named_struct('v', CAST('1.2345678901234567890123456789012345678' AS DECIMAL(38,37)))) AS result
        """
      Then query result collected
        | result |
        | 1.2345678901234567890123456789012345678 |

    Scenario: to_csv of a decimal cast to double
      When query
        """
        SELECT to_csv(named_struct('v', CAST(CAST(0.1 AS DECIMAL(38,18)) AS DOUBLE))) AS result
        """
      Then query result collected
        | result |
        | 0.1 |

    @sail-bug
    Scenario: to_csv of a tiny decimal with ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_csv(named_struct('v', CAST(0.0000000001 AS DECIMAL(38,18)))) AS result
        """
      Then query result collected
        | result |
        | 1.00000000E-10 |

    @sail-bug
    Scenario: to_csv of a tiny decimal with ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_csv(named_struct('v', CAST(0.0000000001 AS DECIMAL(38,18)))) AS result
        """
      Then query result collected
        | result |
        | 1.00000000E-10 |

    Scenario: to_csv of a rounded decimal cast
      When query
        """
        SELECT to_csv(named_struct('v', CAST(2.345 AS DECIMAL(10,2)))) AS result
        """
      Then query result collected
        | result |
        | 2.35 |
