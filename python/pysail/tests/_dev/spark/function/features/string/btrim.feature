Feature: btrim function

  Rule: Numeric arguments are implicitly cast to strings

    Scenario Outline: btrim accepts integer and decimal columns with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT btrim(int_col) AS trimmed_int,
               btrim(decimal_col) AS trimmed_decimal,
               typeof(btrim(int_col)) AS result_type
        FROM VALUES (123, 883.33) AS t(int_col, decimal_col)
        """
      Then query result
        | trimmed_int | trimmed_decimal | result_type |
        | 123         | 883.33          | string      |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: btrim preserves numeric representations with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT btrim(CAST(-128 AS TINYINT)) AS tiny,
               btrim(CAST(32767 AS SMALLINT)) AS small,
               btrim(CAST(-2147483648 AS INT)) AS integer,
               btrim(CAST(9223372036854775807 AS BIGINT)) AS big,
               btrim(CAST(1.5 AS FLOAT)) AS float_value,
               btrim(CAST(-2.25 AS DOUBLE)) AS double_value,
               btrim(CAST(883.33 AS DECIMAL(10, 4))) AS decimal_value,
               btrim(CAST(0 AS DECIMAL(10, 2))) AS zero_value
        """
      Then query result
        | tiny | small | integer     | big                 | float_value | double_value | decimal_value | zero_value |
        | -128 | 32767 | -2147483648 | 9223372036854775807 | 1.5         | -2.25        | 883.3300      | 0.00       |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: btrim evaluates numeric columns and null rows with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT id, btrim(n) AS integer_value,
               btrim(CAST(n AS DECIMAL(8, 2))) AS decimal_value,
               btrim(n, 1) AS trimmed
        FROM (
          SELECT id, CASE WHEN id = 3 THEN NULL ELSE CAST(id - 1 AS INT) END AS n
          FROM range(4)
        )
        ORDER BY id
        """
      Then query result ordered
        | id | integer_value | decimal_value | trimmed |
        | 0  | -1            | -1.00         | -       |
        | 1  | 0             | 0.00          | 0       |
        | 2  | 1             | 1.00          |         |
        | 3  | NULL          | NULL          | NULL    |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: btrim coerces each numeric argument independently with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT id,
               btrim(n, CAST(trim_chars AS STRING)) AS numeric_source,
               btrim(CAST(n AS STRING), trim_chars) AS numeric_trim,
               btrim(n, trim_chars) AS both_numeric
        FROM VALUES (0, 12321, 12), (1, -1221, 12), (2, 222, 2),
                    (3, CAST(NULL AS INT), 1), (4, 123, CAST(NULL AS INT))
          AS t(id, n, trim_chars)
        ORDER BY id
        """
      Then query result ordered
        | id | numeric_source | numeric_trim | both_numeric |
        | 0  | 3              | 3            | 3            |
        | 1  | -              | -            | -            |
        | 2  |                |              |              |
        | 3  | NULL           | NULL         | NULL         |
        | 4  | NULL           | NULL         | NULL         |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: btrim applies trim characters after numeric conversion with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT btrim(12321, '') AS empty_trim,
               btrim(CAST(883.33 AS DECIMAL(10, 4)), '0') AS decimal_source,
               btrim('00.1200', CAST(0 AS DECIMAL(3, 2))) AS decimal_trim,
               btrim(123, CAST(1.3 AS DOUBLE)) AS double_trim,
               btrim('1.5value5.1', CAST(1.5 AS FLOAT)) AS float_trim
        """
      Then query result
        | empty_trim | decimal_source | decimal_trim | double_trim | float_trim |
        | 12321      | 883.33         | 12           | 2           | value      |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: btrim propagates null arguments with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT btrim(NULL) AS untyped_source,
               btrim(CAST(NULL AS DECIMAL(10, 2))) AS numeric_source,
               btrim(NULL, 12) AS null_source,
               btrim(123, NULL) AS null_trim,
               btrim(123, CAST(NULL AS INT)) AS numeric_trim,
               btrim(NULL, NULL) AS both_null
        """
      Then query result
        | untyped_source | numeric_source | null_source | null_trim | numeric_trim | both_null |
        | NULL           | NULL           | NULL        | NULL      | NULL         | NULL      |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: btrim preserves special floating point values with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT btrim(CAST('Infinity' AS DOUBLE)) AS positive_infinity,
               btrim(CAST('-Infinity' AS FLOAT)) AS negative_infinity,
               btrim(CAST('NaN' AS DOUBLE)) AS nan_value,
               btrim(CAST('-0.0' AS DOUBLE)) AS negative_zero,
               btrim(CAST('0.0' AS FLOAT)) AS positive_zero
        """
      Then query result
        | positive_infinity | negative_infinity | nan_value | negative_zero | positive_zero |
        | Infinity          | -Infinity         | NaN       | -0.0          | 0.0           |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: btrim combines a numeric constant with a trim column with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT id, btrim(12321, id) AS result FROM range(4) ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 0  | 12321  |
        | 1  | 232    |
        | 2  | 12321  |
        | 3  | 12321  |

      Examples:
        | ansi  |
        | true  |
        | false |

  Rule: Shared numeric formatting parity

    Scenario Outline: btrim formats floating point exponents like Spark with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT btrim(CAST(1e7 AS DOUBLE)) AS large_value,
               btrim(CAST(1e-4 AS FLOAT)) AS small_value
        """
      Then query result
        | large_value | small_value |
        | 1.0E7       | 1.0E-4      |

      Examples:
        | ansi  |
        | true  |
        | false |

    # TODO: Make the shared decimal formatter honor Spark's non-ANSI BigDecimal.toString rules.
    @sail-bug
    Scenario: btrim uses scientific notation for small decimals in legacy mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT btrim(CAST(0.0000001 AS DECIMAL(10, 7))) AS small_value,
               btrim(CAST(0 AS DECIMAL(10, 8))) AS zero_value
        """
      Then query result
        | small_value | zero_value |
        | 1E-7        | 0E-8       |

  Rule: Existing string trimming is preserved

    Scenario Outline: btrim trims string columns and Unicode characters with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT id, concat('[', btrim(s), ']') AS default_trim,
               concat('[', btrim(s, trim_chars), ']') AS custom_trim
        FROM VALUES (0, ' Spark SQL ', ' '), (1, 'é🙂Spark🙂é', 'é🙂'),
                    (2, '  Spark  ', ''), (3, 'xxx', 'x'),
                    (4, CAST(NULL AS STRING), 'x'), (5, 'Spark', CAST(NULL AS STRING))
          AS t(id, s, trim_chars)
        ORDER BY id
        """
      Then query result ordered
        | id | default_trim | custom_trim |
        | 0  | [Spark SQL]  | [Spark SQL] |
        | 1  | [é🙂Spark🙂é] | [Spark]     |
        | 2  | [Spark]      | [  Spark  ] |
        | 3  | [xxx]        | []          |
        | 4  | NULL         | NULL        |
        | 5  | [Spark]      | NULL        |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: btrim defaults to ASCII spaces with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT hex(btrim(concat(' ', chr(9), 'Spark', chr(10), ' '))) AS control_chars,
               hex(btrim('  Spark  ')) AS nonbreaking_spaces,
               btrim('') AS empty_source,
               btrim('   ') AS spaces_only
        """
      Then query result
        | control_chars  | nonbreaking_spaces | empty_source | spaces_only |
        | 09537061726B0A | C2A0537061726BC2A0 |              |             |

      Examples:
        | ansi  |
        | true  |
        | false |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to btrim yields the schema Spark declares
      When query
        """
        SELECT btrim('    SparkSQL   ') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to btrim yields the schema Spark declares
      When query
        """
        SELECT btrim(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to btrim stays nullable
      When query
        """
        SELECT btrim(c) AS result FROM VALUES ('    SparkSQL   '), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
