Feature: a float stores and prints what Spark does

  # Four lenses, kept apart: the value stored behind the column (read off Arrow, with no
  # renderer in between), the type the query publishes, what `show` prints and what
  # `CAST(... AS STRING)` prints. A float that is stored right can still print wrong: Spark
  # renders it with Java's `Float.toString` / `Double.toString` (`ToStringBase.scala:185`),
  # which switches to an exponent below 1e-3 and from 1e7 on. Asserting only the printed form
  # cannot tell a wrong VALUE from a right value printed wrong.
  # Measured on the Spark 4.2 JVM over Spark Connect.

  Rule: a value inside the range Java prints plainly

    Scenario Outline: a value inside the range Java prints plainly: <case>
      When query
        """
        SELECT <expr> AS v
        """
      Then stored and printed result
        | stored   | type   | shown   | cast   |
        | <stored> | <type> | <shown> | <cast> |

      Examples:
        | case                                       | expr                   | stored                | type   | shown               | cast                |
        | a float                                    | CAST(0.1 AS FLOAT)     | 0.10000000149011612   | float  | 0.1                 | 0.1                 |
        | a float just below the exponent threshold  | CAST(9999999 AS FLOAT) | 9999999.0             | float  | 9999999.0           | 9999999.0           |
        | a float at the small threshold             | CAST(0.001 AS FLOAT)   | 0.0010000000474974513 | float  | 0.001               | 0.001               |
        | a whole float                              | CAST(1 AS FLOAT)       | 1.0                   | float  | 1.0                 | 1.0                 |
        | a negative zero float                      | CAST(-0.0 AS FLOAT)    | 0.0                   | float  | 0.0                 | 0.0                 |
        | a double                                   | 0.1D                   | 0.1                   | double | 0.1                 | 0.1                 |
        | a double sum that does not round           | 0.1D + 0.2D            | 0.30000000000000004   | double | 0.30000000000000004 | 0.30000000000000004 |
        | a double just below the exponent threshold | 9999999.0D             | 9999999.0             | double | 9999999.0           | 9999999.0           |
        | a double at the small threshold            | 0.001D                 | 0.001                 | double | 0.001               | 0.001               |
        | a whole double                             | 1.0D                   | 1.0                   | double | 1.0                 | 1.0                 |
        | a negative zero double                     | -0.0D                  | -0.0                  | double | -0.0                | -0.0                |

  Rule: a value Java prints with an exponent

    @sail-bug
    Scenario Outline: a value Java prints with an exponent: <case>
      When query
        """
        SELECT <expr> AS v
        """
      Then stored and printed result
        | stored   | type   | shown   | cast   |
        | <stored> | <type> | <shown> | <cast> |

      Examples:
        | case                               | expr                          | stored                  | type   | shown                  | cast                   |
        | a float at the exponent threshold  | CAST(10000000 AS FLOAT)       | 10000000.0              | float  | 1.0E7                  | 1.0E7                  |
        | a float below the small threshold  | CAST(0.0001 AS FLOAT)         | 9.999999747378752e-05   | float  | 1.0E-4                 | 1.0E-4                 |
        | the largest float                  | CAST(3.4028234E38 AS FLOAT)   | 3.4028234663852886e+38  | float  | 3.4028235E38           | 3.4028235E38           |
        | the smallest normal float          | CAST(1.17549435E-38 AS FLOAT) | 1.1754943508222875e-38  | float  | 1.17549435E-38         | 1.17549435E-38         |
        | a double at the exponent threshold | 1.0E7D                        | 10000000.0              | double | 1.0E7                  | 1.0E7                  |
        | a double below the small threshold | 1.0E-4D                       | 0.0001                  | double | 1.0E-4                 | 1.0E-4                 |
        | the largest double                 | 1.7976931348623157E308D       | 1.7976931348623157e+308 | double | 1.7976931348623157E308 | 1.7976931348623157E308 |

  Rule: a value that is not a number

    Scenario Outline: a value that is not a number: <case>
      When query
        """
        SELECT <expr> AS v
        """
      Then stored and printed result
        | stored   | type   | shown   | cast   |
        | <stored> | <type> | <shown> | <cast> |

      Examples:
        | case         | expr                  | stored | type   | shown | cast |
        | a float NaN  | CAST('NaN' AS FLOAT)  | nan    | float  | NaN   | NaN  |
        | a double NaN | CAST('NaN' AS DOUBLE) | nan    | double | NaN   | NaN  |

  Rule: an infinite value

    @sail-bug
    Scenario Outline: an infinite value: <case>
      When query
        """
        SELECT <expr> AS v
        """
      Then stored and printed result
        | stored   | type   | shown   | cast   |
        | <stored> | <type> | <shown> | <cast> |

      Examples:
        | case              | expr                       | stored | type   | shown     | cast      |
        | a float Infinity  | CAST('Infinity' AS FLOAT)  | inf    | float  | Infinity  | Infinity  |
        | a float -Infinity | CAST('-Infinity' AS FLOAT) | -inf   | float  | -Infinity | -Infinity |
        | a double Infinity | CAST('Infinity' AS DOUBLE) | inf    | double | Infinity  | Infinity  |
