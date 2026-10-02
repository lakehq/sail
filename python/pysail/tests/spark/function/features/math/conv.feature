Feature: conv with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: conv — the argument may come from a column

    @function(columnargs)
    Scenario: conv with the argument as a literal
      When query
        """
        SELECT conv('100', 2, 10) AS result
        """
      Then query result ordered
        | result |
        | 4      |

    # Sail rejects the column: Sail errors: Unsupported Data Type: Spark `spark_conv` function expects (Utf8 | Utf8View | LargeUtf8 |...
    @function(columnargs)
    Scenario: conv takes argument 2 from a column holding two different values
      When query
        """
        SELECT conv('100', c, 10) AS result FROM VALUES (1, 2), (2, 16) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 4      |
        | 256    |

    # Sail rejects the column: Sail errors: Unsupported Data Type: Spark `spark_conv` function expects (Utf8 | Utf8View | LargeUtf8 |...
    @function(columnargs)
    Scenario: conv takes argument 2 from a column
      When query
        """
        SELECT conv('100', c, 10) AS result FROM VALUES (1, 2), (2, 2) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 4      |
        | 4      |

    # Sail rejects the column: Sail errors: Unsupported Data Type: Spark `spark_conv` function expects (Utf8 | Utf8View | LargeUtf8 |...
    @function(columnargs)
    Scenario: conv takes argument 3 from a column
      When query
        """
        SELECT conv('100', 2, c) AS result FROM VALUES (1, 10), (2, 10) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 4      |
        | 4      |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null string literal is nullable (conv is inherently nullable in Spark)
      When query
        """
        SELECT conv('11', 2, 10) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable string column stays nullable
      When query
        """
        SELECT conv(c, 2, 10) AS result FROM VALUES ('11'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null string column is nullable (conv is inherently nullable in Spark)
      When query
        """
        SELECT conv(CAST(id AS STRING), 10, 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  Rule: Result values (migrated from test_conv.txt doctests)

    Scenario: conv doctest #1 — to_binary/octal/hex
      When query
        """
        SELECT conv('10', 10, 2) as to_binary, conv('10', 10, 8) as to_octal, conv('10', 10, 16) as to_hex
        """
      Then query result
        | to_binary | to_octal | to_hex |
        | 1010      | 12       | A      |

  Rule: conversion follows Spark's signed and unsigned 64-bit rules

    # `Conv` declares `ImplicitCastInputTypes`: the value is cast to STRING and both bases to INT
    # before `NumberConverter` runs (`mathExpressions.scala:477-505`).
    Scenario Outline: conv implicitly casts <case>
      When query
        """
        SELECT conv(<number>, <from_base>, <to_base>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                   | number | from_base              | to_base | result |
        | a tinyint source base  | 'ff'   | CAST(16 AS TINYINT)    | 10      | 255    |
        | string bases           | 'ff'   | '16'                   | '10'    | 255    |
        | a bigint input value   | 255L   | 10                     | 16      | FF     |

    Scenario Outline: conv <case>
      When query
        """
        SELECT conv(<number>, <from_base>, <to_base>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                 | number             | from_base | to_base | result               |
        | a whitespace-only value returns NULL | ' '                | 2         | 10      | NULL                 |
        | trims input around binary digits     | '  100  '          | 2         | 10      | 4                    |
        | retains an unsigned 64-bit value     | 'FFFFFFFFFFFFFFFF' | 16        | 10      | 18446744073709551615 |
        | accepts a negative destination base  | '-10'              | 16        | -10     | -16                  |

  Rule: conversion follows Spark's NumberConverter branches

    # A negative destination interprets the 64-bit intermediate as signed. Its high bit therefore
    # yields -1 regardless of whether the source text carried a minus sign
    # (`NumberConverter.scala:167-192`).
    Scenario Outline: conv to a negative base: <case>
      When query
        """
        SELECT conv(<input>, 16, -10) AS result
        """
      Then query result
        | result  |
        | <value> |

      Examples:
        | case             | input               | value |
        | the high bit set | 'FFFFFFFFFFFFFFFF'  | -1    |
        | a negative input | '-FFFFFFFFFFFFFFFF' | -1    |

    # `char2byte` stops at the first invalid digit (`NumberConverter.scala:115-131`).
    Scenario: conv truncates at an invalid digit
      When query
        """
        SELECT conv('12x3', 10, 10) AS result
        """
      Then query result
        | result |
        | 12     |

    Scenario: conv of a NULL string in base 36 is NULL
      When query
        """
        SELECT conv(CAST(NULL AS STRING), 36, 10) AS result
        """
      Then query result
        | result |
        | NULL   |

    @spark-4
    Scenario: conv overflow raises with ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT conv('FFFFFFFFFFFFFFFFF', 16, 10) AS result
        """
      Then query error ARITHMETIC_OVERFLOW
