Feature: unary minus (negative) honors ANSI overflow semantics

  Rule: Negating the minimum integral value overflows

    Scenario Outline: ANSI on: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT -CAST(<value> AS <type>) AS result
        """
      Then query error (?i)overflow

      Examples:
        | case                                     | value                | type     |
        | negate INT_MIN errors under ANSI on      | -2147483648          | INT      |
        | negate BIGINT_MIN errors under ANSI on   | -9223372036854775808 | BIGINT   |
        | negate SMALLINT_MIN errors under ANSI on | -32768               | SMALLINT |
        | negate TINYINT_MIN errors under ANSI on  | -128                 | TINYINT  |

    Scenario Outline: ANSI off: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT -CAST(<value> AS <type>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                     | value                | type     | result               |
        | negate INT_MIN wraps under ANSI off      | -2147483648          | INT      | -2147483648          |
        | negate BIGINT_MIN wraps under ANSI off   | -9223372036854775808 | BIGINT   | -9223372036854775808 |
        | negate SMALLINT_MIN wraps under ANSI off | -32768               | SMALLINT | -32768               |
        | negate TINYINT_MIN wraps under ANSI off  | -128                 | TINYINT  | -128                 |

  Rule: Ordinary negation is unaffected by ANSI mode

    Scenario Outline: Ordinary negation: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                       | expr                         | result |
        | negate a positive integer                  | -CAST(5 AS INT)              | -5     |
        | double negation returns the original value | -(-CAST(5 AS INT))           | 5      |
        | negate a double                            | -CAST(1.5 AS DOUBLE)         | -1.5   |
        | negate a decimal                           | -CAST(1.50 AS DECIMAL(10,2)) | -1.50  |
        | negate NULL returns NULL                   | -CAST(NULL AS INT)           | NULL   |

  Rule: Floating-point negation never overflows

    Scenario Outline: Floating-point: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                      | expr                        | result    |
        | negate a float                            | -CAST(1.5 AS FLOAT)         | -1.5      |
        | negate positive zero yields negative zero | -CAST(0.0 AS DOUBLE)        | -0.0      |
        | negate NaN returns NaN                    | -CAST('NaN' AS DOUBLE)      | NaN       |
        | negate Infinity returns negative Infinity | -CAST('Infinity' AS DOUBLE) | -Infinity |

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

    Scenario: negating a cast negative-zero double column yields negative zero
      When query
        """
        SELECT -value AS result FROM VALUES (CAST(-0.0 AS DOUBLE)) AS t(value)
        """
      Then query result
        | result |
        | -0.0   |

    Scenario Outline: Floating-point predicate: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result |
        | true   |

      Examples:
        | case                                                  | expr                                                      |
        | negate NaN is NaN by predicate                        | isnan(-CAST('NaN' AS DOUBLE))                             |
        | negate Infinity equals negative infinity by predicate | -CAST('Infinity' AS DOUBLE) = CAST('-Infinity' AS DOUBLE) |

  Rule: Day-time interval negation

    Scenario Outline: Interval: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result |
        | true   |

      Examples:
        | case                                           | expr                                                 |
        | negate a day-time interval                     | -(INTERVAL '5' SECOND) = INTERVAL '-5' SECOND        |
        | negate a negative interval                     | -(INTERVAL '-3' DAY) = INTERVAL '3' DAY              |
        | negative function negates an interval          | negative(INTERVAL '5' SECOND) = INTERVAL '-5' SECOND |
        | double negation of an interval is the original | -(-(INTERVAL '5' SECOND)) = INTERVAL '5' SECOND      |

  Rule: String arguments coerce to double

    Scenario Outline: String coercion: <case>
      When query
        """
        SELECT negative(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                      | input | result |
        | negate a numeric string coerces to double | '1.5' | -1.5   |
        | negate a negative numeric string          | '-3'  | 3.0    |

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

    # UnaryMinus retains DECIMAL(p,s), then Spark validates the negated value against
    # that declared precision. Its physical integer may fit Arrow storage while the
    # value does not fit DECIMAL(p,s), so this raises NUMERIC_VALUE_OUT_OF_RANGE in
    # both ANSI modes. Sail used to validate only physical integer overflow.
    Scenario Outline: Negating the maximum decimal overflows its precision: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expression> AS result
        """
      Then query error (?i)out.of.range

      Examples:
        | case                                                        | expression                                                            | ansi  |
        | unary minus rejects the maximum DECIMAL(38,0) under ANSI on | -CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))    | true  |
        | unary minus rejects the maximum DECIMAL(38,0) under ANSI off| -CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))    | false |
        | negative rejects the maximum DECIMAL(38,0) under ANSI on    | negative(CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))) | true |
        | negative rejects the maximum DECIMAL(38,0) under ANSI off   | negative(CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))) | false |

  Rule: Negating expanded decimals uses Spark's DECIMAL128 context

    Scenario Outline: negation rounds expanded DECIMAL values: <case>
      When query
        """
        SELECT <expression> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                  | expression                                                                  | result                                |
        | unary minus rounds to 34 digits       | -CAST('12345678901234567890123456789012345' AS DECIMAL(38,0))              | -12345678901234567890123456789012340 |
        | negative function rounds to 34 digits | negative(CAST('12345678901234567890123456789012345' AS DECIMAL(38,0)))     | -12345678901234567890123456789012340 |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null integer literal yields a non-nullable integer
      When query
        """
        SELECT negative(5) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a non-null integer column yields a non-nullable integer
      When query
        """
        SELECT negative(id) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    Scenario: a nullable integer column stays nullable
      When query
        """
        SELECT negative(c) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
