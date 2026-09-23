Feature: to_json and to_csv render a float the way Spark does

  # `to_json` and `to_csv` are renderers of their own: neither goes through `show` nor
  # through `CAST(... AS STRING)`, so a float can print right in every lens of
  # `float_stored_and_printed.feature` and still be written wrong here. Spark renders a
  # float with Java's `Float.toString` / `Double.toString` (`ToStringBase.scala:185`), which
  # switches to an exponent below 1e-3 and from 1e7 on, and always keeps a fractional part.
  # Measured on the Spark 4.2 JVM over Spark Connect.

  Rule: to_json renders a float the way Spark does

    @sail-bug
    Scenario: to_json of a float
      When query
        """
        SELECT to_json(named_struct('v', CAST(0.1 AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | {"v":0.1} |

    Scenario: to_json of a float just below the exponent threshold
      When query
        """
        SELECT to_json(named_struct('v', CAST(9999999 AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | {"v":9999999.0} |

    @sail-bug
    Scenario: to_json of a float at the exponent threshold
      When query
        """
        SELECT to_json(named_struct('v', CAST(10000000 AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | {"v":1.0E7} |

    @sail-bug
    Scenario: to_json of a float at the small threshold
      When query
        """
        SELECT to_json(named_struct('v', CAST(0.001 AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | {"v":0.001} |

    @sail-bug
    Scenario: to_json of a float below the small threshold
      When query
        """
        SELECT to_json(named_struct('v', CAST(0.0001 AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | {"v":1.0E-4} |

    Scenario: to_json of a whole float
      When query
        """
        SELECT to_json(named_struct('v', CAST(1 AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | {"v":1.0} |

    Scenario: to_json of a negative zero float
      When query
        """
        SELECT to_json(named_struct('v', CAST(-0.0 AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | {"v":0.0} |

    Scenario: to_json of a float NaN
      When query
        """
        SELECT to_json(named_struct('v', CAST('NaN' AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | {"v":"NaN"} |

    Scenario: to_json of a float Infinity
      When query
        """
        SELECT to_json(named_struct('v', CAST('Infinity' AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | {"v":"Infinity"} |

    Scenario: to_json of a float -Infinity
      When query
        """
        SELECT to_json(named_struct('v', CAST('-Infinity' AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | {"v":"-Infinity"} |

    @sail-bug
    Scenario: to_json of the largest float
      When query
        """
        SELECT to_json(named_struct('v', CAST(3.4028234E38 AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | {"v":3.4028235E38} |

    @sail-bug
    Scenario: to_json of the smallest normal float
      When query
        """
        SELECT to_json(named_struct('v', CAST(1.17549435E-38 AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | {"v":1.17549435E-38} |

    Scenario: to_json of a double
      When query
        """
        SELECT to_json(named_struct('v', 0.1D)) AS result
        """
      Then query result collected
        | result |
        | {"v":0.1} |

    Scenario: to_json of a double sum that does not round
      When query
        """
        SELECT to_json(named_struct('v', 0.1D + 0.2D)) AS result
        """
      Then query result collected
        | result |
        | {"v":0.30000000000000004} |

    Scenario: to_json of a double just below the exponent threshold
      When query
        """
        SELECT to_json(named_struct('v', 9999999.0D)) AS result
        """
      Then query result collected
        | result |
        | {"v":9999999.0} |

    @sail-bug
    Scenario: to_json of a double at the exponent threshold
      When query
        """
        SELECT to_json(named_struct('v', 1.0E7D)) AS result
        """
      Then query result collected
        | result |
        | {"v":1.0E7} |

    Scenario: to_json of a double at the small threshold
      When query
        """
        SELECT to_json(named_struct('v', 0.001D)) AS result
        """
      Then query result collected
        | result |
        | {"v":0.001} |

    @sail-bug
    Scenario: to_json of a double below the small threshold
      When query
        """
        SELECT to_json(named_struct('v', 1.0E-4D)) AS result
        """
      Then query result collected
        | result |
        | {"v":1.0E-4} |

    Scenario: to_json of a whole double
      When query
        """
        SELECT to_json(named_struct('v', 1.0D)) AS result
        """
      Then query result collected
        | result |
        | {"v":1.0} |

    Scenario: to_json of a negative zero double
      When query
        """
        SELECT to_json(named_struct('v', -0.0D)) AS result
        """
      Then query result collected
        | result |
        | {"v":-0.0} |

    Scenario: to_json of a double NaN
      When query
        """
        SELECT to_json(named_struct('v', CAST('NaN' AS DOUBLE))) AS result
        """
      Then query result collected
        | result |
        | {"v":"NaN"} |

    Scenario: to_json of a double Infinity
      When query
        """
        SELECT to_json(named_struct('v', CAST('Infinity' AS DOUBLE))) AS result
        """
      Then query result collected
        | result |
        | {"v":"Infinity"} |

    @sail-bug
    Scenario: to_json of the largest double
      When query
        """
        SELECT to_json(named_struct('v', 1.7976931348623157E308D)) AS result
        """
      Then query result collected
        | result |
        | {"v":1.7976931348623157E308} |

  Rule: to_csv renders a float the way Spark does

    Scenario: to_csv of a float
      When query
        """
        SELECT to_csv(named_struct('v', CAST(0.1 AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | 0.1 |

    @sail-bug
    Scenario: to_csv of a float just below the exponent threshold
      When query
        """
        SELECT to_csv(named_struct('v', CAST(9999999 AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | 9999999.0 |

    @sail-bug
    Scenario: to_csv of a float at the exponent threshold
      When query
        """
        SELECT to_csv(named_struct('v', CAST(10000000 AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | 1.0E7 |

    Scenario: to_csv of a float at the small threshold
      When query
        """
        SELECT to_csv(named_struct('v', CAST(0.001 AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | 0.001 |

    @sail-bug
    Scenario: to_csv of a float below the small threshold
      When query
        """
        SELECT to_csv(named_struct('v', CAST(0.0001 AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | 1.0E-4 |

    @sail-bug
    Scenario: to_csv of a whole float
      When query
        """
        SELECT to_csv(named_struct('v', CAST(1 AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | 1.0 |

    @sail-bug
    Scenario: to_csv of a negative zero float
      When query
        """
        SELECT to_csv(named_struct('v', CAST(-0.0 AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | 0.0 |

    Scenario: to_csv of a float NaN
      When query
        """
        SELECT to_csv(named_struct('v', CAST('NaN' AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | NaN |

    Scenario: to_csv of a float Infinity
      When query
        """
        SELECT to_csv(named_struct('v', CAST('Infinity' AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | Infinity |

    Scenario: to_csv of a float -Infinity
      When query
        """
        SELECT to_csv(named_struct('v', CAST('-Infinity' AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | -Infinity |

    @sail-bug
    Scenario: to_csv of the largest float
      When query
        """
        SELECT to_csv(named_struct('v', CAST(3.4028234E38 AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | 3.4028235E38 |

    @sail-bug
    Scenario: to_csv of the smallest normal float
      When query
        """
        SELECT to_csv(named_struct('v', CAST(1.17549435E-38 AS FLOAT))) AS result
        """
      Then query result collected
        | result |
        | 1.17549435E-38 |

    Scenario: to_csv of a double
      When query
        """
        SELECT to_csv(named_struct('v', 0.1D)) AS result
        """
      Then query result collected
        | result |
        | 0.1 |

    Scenario: to_csv of a double sum that does not round
      When query
        """
        SELECT to_csv(named_struct('v', 0.1D + 0.2D)) AS result
        """
      Then query result collected
        | result |
        | 0.30000000000000004 |

    @sail-bug
    Scenario: to_csv of a double just below the exponent threshold
      When query
        """
        SELECT to_csv(named_struct('v', 9999999.0D)) AS result
        """
      Then query result collected
        | result |
        | 9999999.0 |

    @sail-bug
    Scenario: to_csv of a double at the exponent threshold
      When query
        """
        SELECT to_csv(named_struct('v', 1.0E7D)) AS result
        """
      Then query result collected
        | result |
        | 1.0E7 |

    Scenario: to_csv of a double at the small threshold
      When query
        """
        SELECT to_csv(named_struct('v', 0.001D)) AS result
        """
      Then query result collected
        | result |
        | 0.001 |

    @sail-bug
    Scenario: to_csv of a double below the small threshold
      When query
        """
        SELECT to_csv(named_struct('v', 1.0E-4D)) AS result
        """
      Then query result collected
        | result |
        | 1.0E-4 |

    @sail-bug
    Scenario: to_csv of a whole double
      When query
        """
        SELECT to_csv(named_struct('v', 1.0D)) AS result
        """
      Then query result collected
        | result |
        | 1.0 |

    @sail-bug
    Scenario: to_csv of a negative zero double
      When query
        """
        SELECT to_csv(named_struct('v', -0.0D)) AS result
        """
      Then query result collected
        | result |
        | -0.0 |

    Scenario: to_csv of a double NaN
      When query
        """
        SELECT to_csv(named_struct('v', CAST('NaN' AS DOUBLE))) AS result
        """
      Then query result collected
        | result |
        | NaN |

    Scenario: to_csv of a double Infinity
      When query
        """
        SELECT to_csv(named_struct('v', CAST('Infinity' AS DOUBLE))) AS result
        """
      Then query result collected
        | result |
        | Infinity |

    @sail-bug
    Scenario: to_csv of the largest double
      When query
        """
        SELECT to_csv(named_struct('v', 1.7976931348623157E308D)) AS result
        """
      Then query result collected
        | result |
        | 1.7976931348623157E308 |
