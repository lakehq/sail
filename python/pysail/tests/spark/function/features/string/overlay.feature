Feature: overlay output schema

  Rule: argument count

    # Spark rejects arity during function resolution (`FunctionRegistry.scala:167`). This guards
    # the planner before it indexes the position argument.
    Scenario: overlay rejects too few arguments without panicking
      When query
        """
        SELECT overlay('abc', 'X')
        """
      Then query error (?i)(WRONG_NUM_ARGS|overlay requires 3 or 4 arguments)

  Rule: a negative length uses the replacement's character length

    # `Overlay.calculate` uses `replace.numChars` for every negative length, not just -1
    # (`stringExpressions.scala:984-997`). This is the STRING counterpart of the BINARY rule.
    Scenario Outline: overlay with a negative length: <case>
      When query
        """
        SELECT overlay('Spark SQL' PLACING '_' FROM 6 FOR <length>) AS result
        """
      Then query result
        | result    |
        | Spark_SQL |

      Examples:
        | case      | length |
        | minus one | -1     |
        | minus two | -2     |

    # `UTF8String.substringSQL` clamps a non-positive start before `Overlay.calculate` runs
    # (`UTF8String.scala:1780-1793`). STRING follows the same rule as BINARY.
    Scenario Outline: overlay with a non-positive string position: <case>
      When query
        """
        SELECT overlay('abc' PLACING 'X' FROM <position>) AS result
        """
      Then query result
        | result |
        | Xabc   |

      Examples:
        | case      | position |
        | zero      | 0        |
        | minus one | -1       |

    Scenario: a non-positive literal position keeps Overlay non-nullable
      When query
        """
        SELECT overlay('abc' PLACING 'X' FROM 0) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    # `UTF8String.substringSQL` counts negative positions back from the end of the input
    # (`UTF8String.java:671`); it does not clamp every negative position to zero.
    Scenario Outline: overlay counts a negative string position from the end: <case>
      When query
        """
        SELECT overlay(<input> PLACING <replacement> FROM <position> <length>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                      | input    | replacement | position | length | result |
        | implicit replacement span | 'abcde'  | 'X'         | -2       |        | Xe     |
        | explicit replacement span | 'abcde'  | 'X'         | -3       | FOR 2  | Xe     |
        | one-character span        | 'abcdef' | 'X'         | -4       | FOR 1  | Xdef   |
        | multi-character span      | 'abcdef' | 'XYZ'       | -4       |        | XYZf   |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to overlay yields the schema Spark declares
      When query
        """
        SELECT overlay('Spark SQL' PLACING '_' FROM 6) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a four-argument literal overlay yields the schema Spark declares
      When query
        """
        SELECT overlay('Spark SQL' PLACING '_' FROM 6 FOR 2) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable overlay input yields a nullable schema
      When query
        """
        SELECT overlay(value PLACING 'X' FROM 1) AS result
        FROM VALUES (CAST(NULL AS STRING)) AS t(value)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a string position makes overlay nullable because Spark inserts a nullable cast
      When query
        """
        SELECT overlay(s PLACING 'X' FROM p) AS result
        FROM VALUES ('abc', '2') AS t(s, p)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
