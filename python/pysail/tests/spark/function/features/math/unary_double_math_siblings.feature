Feature: unary DOUBLE math functions outside the acos family (sqrt, expm1, rint)

  # Spark's UnaryMathExpression: ImplicitCastInputTypes(DoubleType), always nullable DOUBLE.
  # Expected values and messages come from the Spark JVM (4.2.0).
  # `signum` / `sign` are not here: they also accept intervals, so they are not the same rule.

  Rule: Argument count error carries Spark's class

    Scenario Outline: <fn> with <count> arguments is WRONG_NUM_ARGS
      When query
        """
        SELECT <fn>(<args>) AS result
        """
      Then query error \[WRONG_NUM_ARGS.*The `<fn>` requires 1 parameters but the actual number is <count>

      Examples:
        | fn    | count | args |
        | sqrt  | 0     |      |
        | sqrt  | 2     | 1, 2 |
        | expm1 | 0     |      |
        | expm1 | 2     | 1, 2 |
        | rint  | 0     |      |
        | rint  | 2     | 1, 2 |

  Rule: A BOOLEAN argument is rejected with Spark's message

    Scenario Outline: <fn> of a BOOLEAN names <shown>
      When query
        """
        SELECT <fn>(true) AS result
        """
      Then query error Cannot resolve "<shown>\(true\)" due to data type mismatch: The first parameter requires the "DOUBLE" type, however "true" has the type "BOOLEAN"

      Examples:
        | fn    | shown |
        | sqrt  | SQRT  |
        | expm1 | EXPM1 |
        | rint  | rint  |

  Rule: Argument types Spark rejects

    Scenario Outline: <fn> of <type> is DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE
      When query
        """
        SELECT <fn>(<input>) AS result
        """
      Then query error \[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE\]

      Examples:
        | fn    | type      | input                          |
        | sqrt  | DATE      | DATE'2024-01-02'               |
        | sqrt  | TIMESTAMP | TIMESTAMP'2024-01-02 03:04:05' |
        | sqrt  | BINARY    | X'01'                          |
        | sqrt  | ARRAY     | array(1.0D)                    |
        | sqrt  | INTERVAL  | INTERVAL '1' YEAR              |
        | expm1 | DATE      | DATE'2024-01-02'               |
        | expm1 | TIMESTAMP | TIMESTAMP'2024-01-02 03:04:05' |
        | expm1 | BINARY    | X'01'                          |
        | expm1 | ARRAY     | array(1.0D)                    |
        | expm1 | INTERVAL  | INTERVAL '1' YEAR              |
        | rint  | DATE      | DATE'2024-01-02'               |
        | rint  | TIMESTAMP | TIMESTAMP'2024-01-02 03:04:05' |
        | rint  | BINARY    | X'01'                          |
        | rint  | ARRAY     | array(1.0D)                    |
        | rint  | INTERVAL  | INTERVAL '1' YEAR              |

    Scenario Outline: <fn> of a BOOLEAN is rejected under ANSI=true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <fn>(true) AS result
        """
      Then query error \[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE\]

      Examples:
        | fn    |
        | sqrt  |
        | expm1 |
        | rint  |

  Rule: A malformed string is NULL under ANSI=false

    Scenario Outline: <fn> of a non-numeric string is NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <fn>('abc') AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | fn    |
        | sqrt  |
        | expm1 |
        | rint  |

  Rule: Values

    Scenario Outline: <fn>(<input>) = <result> (<what>)
      When query
        """
        SELECT <fn>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | fn    | what    | input                            | result |
        | sqrt  | int     | 4                                | 2.0    |
        | sqrt  | string  | '4'                              | 2.0    |
        | sqrt  | neg     | -1                               | NaN    |
        | sqrt  | null    | NULL                             | NULL   |
        | expm1 | zero    | 0                                | 0.0    |
        | expm1 | string  | '0'                              | 0.0    |
        | expm1 | null    | NULL                             | NULL   |
        | rint  | tinyint | CAST(3 AS TINYINT)               | 3.0    |
        | rint  | null    | NULL                             | NULL   |

  Rule: rint rounds half to even

    Scenario Outline: rint(<input>) = <result> (<what>)
      When query
        """
        SELECT rint(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | what                   | input                              | result    |
        | tie down               | 2.5D                               | 2.0       |
        | tie up                 | 3.5D                               | 4.0       |
        | tie up (odd)           | 1.5D                               | 2.0       |
        | tie negative down      | -2.5D                              | -2.0      |
        | tie negative up        | -3.5D                              | -4.0      |
        | tie at zero            | 0.5D                               | 0.0       |
        | tie at negative zero   | -0.5D                              | -0.0      |
        | below a tie            | 2.4D                               | 2.0       |
        | above a tie            | 2.6D                               | 3.0       |
        | above a tie, negative  | -2.6D                              | -3.0      |
        | just below 0.5         | CAST(0.49999999999999994 AS DOUBLE) | 0.0       |
        | negative below 0.5     | CAST(-0.4 AS DOUBLE)               | -0.0      |
        | tie at 2^52            | 4503599627370496.5D                | 4.503599627370496E15 |
        | NaN                    | CAST('NaN' AS DOUBLE)              | NaN       |
        | infinity                | CAST('Infinity' AS DOUBLE)          | Infinity  |
        | negative infinity       | CAST('-Infinity' AS DOUBLE)         | -Infinity |
        | decimal tie            | CAST(2.5 AS DECIMAL(10,2))         | 2.0       |
        | string tie             | '2.5'                              | 2.0       |

  Rule: String argument under ANSI=true

    # Cast-level gap shared with `CAST('abc' AS DOUBLE)`: no [CAST_INVALID_INPUT] class.
    @sail-bug
    Scenario Outline: <fn> of a malformed string under ANSI=true is CAST_INVALID_INPUT
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <fn>('abc') AS result
        """
      Then query error \[CAST_INVALID_INPUT\]

      Examples:
        | fn    |
        | sqrt  |
        | expm1 |
        | rint  |

  @function(nullability)
  Rule: Output schema is always a nullable double

    Scenario Outline: <fn> output schema is a nullable double (<form>)
      When query
        """
        <query>
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

      Examples:
        | fn    | form            | query                                                |
        | sqrt  | literal         | SELECT sqrt(1) AS result                             |
        | sqrt  | non-null column | SELECT sqrt(CAST(id AS INT)) AS result FROM range(2) |
        | expm1 | literal         | SELECT expm1(1) AS result                            |
        | expm1 | non-null column | SELECT expm1(CAST(id AS INT)) AS result FROM range(2) |

    Scenario Outline: rint output schema is a nullable double (<form>)
      When query
        """
        <query>
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

      Examples:
        | form            | query                                                |
        | literal         | SELECT rint(1) AS result                             |
        | non-null column | SELECT rint(CAST(id AS INT)) AS result FROM range(2) |
