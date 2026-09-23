Feature: a decimal stores and prints what Spark does

  # Four lenses, kept apart: the value stored behind the column (read off Arrow, with no
  # renderer in between), the type the query publishes, what `show` prints and what
  # `CAST(... AS STRING)` prints. A float that is stored right can still print wrong: Spark
  # renders it with Java's `Float.toString` / `Double.toString` (`ToStringBase.scala:185`),
  # which switches to an exponent below 1e-3 and from 1e7 on. Asserting only the printed form
  # cannot tell a wrong VALUE from a right value printed wrong.
  # A decimal carries its scale, so `1.5` cast to DECIMAL(10,2) stores and prints `1.50`.
  # `CAST(... AS STRING)` renders it plainly only when ANSI is on -- `Cast.useDecimalPlainString`
  # is `ansiEnabled` (`Cast.scala:682`) -- while `show` goes through `ToPrettyString`, which
  # overrides it to `true` (`ToPrettyString.scala:50`) and is always plain. So a decimal below
  # 1e-6 prints two different ways in two lenses of the same row.
  # Measured on the Spark 4.2 JVM over Spark Connect.

  Rule: a decimal literal

    Scenario Outline: a decimal literal: <case>
      When query
        """
        SELECT <expr> AS v
        """
      Then stored and printed result
        | stored   | type   | shown   | cast   |
        | <stored> | <type> | <shown> | <cast> |

      Examples:
        | case                        | expr                                                              | stored                                  | type           | shown                                   | cast                                    |
        | a decimal keeps its scale   | CAST(1.5 AS DECIMAL(10,2))                                        | 1.50                                    | decimal(10,2)  | 1.50                                    | 1.50                                    |
        | a negative zero decimal     | CAST(-0.00 AS DECIMAL(5,2))                                       | 0.00                                    | decimal(5,2)   | 0.00                                    | 0.00                                    |
        | a decimal with scale zero   | CAST(42 AS DECIMAL(10,0))                                         | 42                                      | decimal(10,0)  | 42                                      | 42                                      |
        | a wide decimal              | CAST('12345678901234567890.123' AS DECIMAL(38,10))                | 12345678901234567890.1230000000         | decimal(38,10) | 12345678901234567890.1230000000         | 12345678901234567890.1230000000         |
        | a decimal at full precision | CAST('1.2345678901234567890123456789012345678' AS DECIMAL(38,37)) | 1.2345678901234567890123456789012345678 | decimal(38,37) | 1.2345678901234567890123456789012345678 | 1.2345678901234567890123456789012345678 |
        | a rounded decimal cast      | CAST(2.345 AS DECIMAL(10,2))                                      | 2.35                                    | decimal(10,2)  | 2.35                                    | 2.35                                    |

  Rule: a decimal Spark widens

    Scenario Outline: a decimal Spark widens: <case>
      When query
        """
        SELECT <expr> AS v
        """
      Then stored and printed result
        | stored   | type   | shown   | cast   |
        | <stored> | <type> | <shown> | <cast> |

      Examples:
        | case                     | expr                                                    | stored | type          | shown  | cast   |
        | a decimal product        | CAST(1.5 AS DECIMAL(10,2)) * CAST(2.5 AS DECIMAL(10,2)) | 3.7500 | decimal(21,4) | 3.7500 | 3.7500 |
        | a decimal sum            | CAST(1.5 AS DECIMAL(10,2)) + CAST(2.5 AS DECIMAL(10,2)) | 4.00   | decimal(11,2) | 4.00   | 4.00   |
        | a decimal cast to double | CAST(CAST(0.1 AS DECIMAL(38,18)) AS DOUBLE)             | 0.1    | double        | 0.1    | 0.1    |

  Rule: a decimal division

    @sail-bug
    Scenario Outline: a decimal division: <case>
      When query
        """
        SELECT <expr> AS v
        """
      Then stored and printed result
        | stored   | type   | shown   | cast   |
        | <stored> | <type> | <shown> | <cast> |

      Examples:
        | case               | expr                                                | stored        | type           | shown         | cast          |
        | a decimal division | CAST(1 AS DECIMAL(10,0)) / CAST(3 AS DECIMAL(10,0)) | 0.33333333333 | decimal(21,11) | 0.33333333333 | 0.33333333333 |

  Rule: a decimal below 1e-6 with ANSI on

    Scenario Outline: a decimal below 1e-6 with ANSI on: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS v
        """
      Then stored and printed result
        | stored   | type   | shown   | cast   |
        | <stored> | <type> | <shown> | <cast> |

      Examples:
        | case                        | expr                                 | stored         | type           | shown                | cast                 |
        | a tiny decimal with ANSI on | CAST(0.0000000001 AS DECIMAL(38,18)) | 1.00000000E-10 | decimal(38,18) | 0.000000000100000000 | 0.000000000100000000 |

  Rule: a decimal below 1e-6 with ANSI off

    @sail-bug
    Scenario Outline: a decimal below 1e-6 with ANSI off: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expr> AS v
        """
      Then stored and printed result
        | stored   | type   | shown   | cast   |
        | <stored> | <type> | <shown> | <cast> |

      Examples:
        | case                         | expr                                 | stored         | type           | shown                | cast           |
        | a tiny decimal with ANSI off | CAST(0.0000000001 AS DECIMAL(38,18)) | 1.00000000E-10 | decimal(38,18) | 0.000000000100000000 | 1.00000000E-10 |
