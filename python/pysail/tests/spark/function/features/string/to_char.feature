@to_char
Feature: to_char with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: to_char — a numeric format must be foldable

    @column_args
    Scenario: to_char with the argument as a literal
      When query
        """
        SELECT to_char(454, '999') AS result
        """
      Then query result ordered
        | result |
        | 454    |

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['454', '454'].
    @column_args @sail-bug
    Scenario: to_char takes argument 2 from a column holding two different values
      When query
        """
        SELECT to_char(454, c) AS result FROM VALUES (1, '999'), (2, '000D00') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['454', '454'].
    @column_args @sail-bug
    Scenario: to_char takes argument 2 from a column containing NULL
      When query
        """
        SELECT to_char(454, c) AS result FROM VALUES (1, '999'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['454', '454'].
    @column_args @sail-bug
    Scenario: to_char takes argument 2 from a column
      When query
        """
        SELECT to_char(454, c) AS result FROM VALUES (1, '999'), (2, '999') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

  # With a DATE or TIMESTAMP input, Spark resolves `to_char` to `DateFormatClass`, which accepts a
  # non-foldable format and applies it row by row. So the foldable rule above is specific to the
  # numeric format; here the column is legal and each row must use its own format.
  Rule: to_char — a date format is resolved per row

    @column_args
    Scenario: to_char takes the date format from a column holding two different values
      When query
        """
        SELECT to_char(DATE '2026-02-02', c) AS result FROM VALUES (1, 'y'), (2, 'MM') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 2026   |
        | 02     |

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null numeric literal yields a non-nullable string
      When query
        """
        SELECT to_char(78.12, '99D99') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable numeric column stays nullable
      When query
        """
        SELECT to_char(c, '99D99') AS result FROM VALUES (CAST(78.12 AS DECIMAL(4,2))), (CAST(NULL AS DECIMAL(4,2))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null numeric column yields a non-nullable string
      When query
        """
        SELECT to_char(CAST(id AS DECIMAL(3,0)), '999') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  # Values are wrapped in [...] so whitespace-significant output (leading/
  # trailing spaces, blanks) survives the stripped result table.
  Rule: Result values (migrated from test_to_char.txt doctests)

    Scenario: to_char doctest #1 (result)
      When query
        """
        SELECT '[' || to_char(e, '$99.99') || ']' AS r FROM VALUES (CAST(78.12 AS DOUBLE)) AS t(e)
        """
      Then query result
        | r        |
        | [$78.12] |

    Scenario: to_char doctest #2 (result)
      When query
        """
        SELECT '[' || (to_char(454, '999')) || ']' AS r
        """
      Then query result
        | r     |
        | [454] |

    Scenario: to_char doctest #3 (result)
      When query
        """
        SELECT '[' || (to_char(454, '9999')) || ']' AS r
        """
      Then query result
        | r      |
        | [ 454] |

    Scenario: to_char doctest #4 (result)
      When query
        """
        SELECT '[' || (to_char(454, '99999')) || ']' AS r
        """
      Then query result
        | r       |
        | [  454] |

    Scenario: to_char doctest #5 (result)
      When query
        """
        SELECT '[' || (to_char(454, '0999')) || ']' AS r
        """
      Then query result
        | r      |
        | [0454] |

    Scenario: to_char doctest #6 (result)
      When query
        """
        SELECT '[' || (to_char(454, '00999')) || ']' AS r
        """
      Then query result
        | r       |
        | [00454] |

    Scenario: to_char doctest #7 (result)
      When query
        """
        SELECT '[' || (to_char(454, '0000')) || ']' AS r
        """
      Then query result
        | r      |
        | [0454] |

    Scenario: to_char doctest #8 (result)
      When query
        """
        SELECT '[' || (to_char(454, '90999')) || ']' AS r
        """
      Then query result
        | r       |
        | [  454] |

    Scenario: to_char doctest #9 (result)
      When query
        """
        SELECT '[' || (to_char(454, '99099')) || ']' AS r
        """
      Then query result
        | r       |
        | [  454] |

    Scenario: to_char doctest #10 (result)
      When query
        """
        SELECT '[' || (to_char(1, '9')) || ']' AS r
        """
      Then query result
        | r   |
        | [1] |

    Scenario: to_char doctest #11 (result)
      When query
        """
        SELECT '[' || (to_char(1, '0')) || ']' AS r
        """
      Then query result
        | r   |
        | [1] |

    Scenario: to_char doctest #12 (result)
      When query
        """
        SELECT '[' || (to_char(454, '99')) || ']' AS r
        """
      Then query result
        | r    |
        | [##] |

    Scenario: to_char doctest #13 (result)
      When query
        """
        SELECT '[' || (to_char(454, '00')) || ']' AS r
        """
      Then query result
        | r    |
        | [##] |

    Scenario: to_char doctest #14 (result)
      When query
        """
        SELECT '[' || (to_char(123.456, '999D9')) || ']' AS r
        """
      Then query result
        | r       |
        | [###.#] |

    Scenario: to_char doctest #15 (result)
      When query
        """
        SELECT '[' || (to_char(78.123, '99.9')) || ']' AS r
        """
      Then query result
        | r      |
        | [##.#] |

    Scenario: to_char doctest #16 (result)
      When query
        """
        SELECT '[' || (to_char(12345, '9,999')) || ']' AS r
        """
      Then query result
        | r       |
        | [# ###] |

    Scenario: to_char doctest #17 (result)
      When query
        """
        SELECT '[' || (to_char(0.05, '9.9')) || ']' AS r
        """
      Then query result
        | r     |
        | [#.#] |

    Scenario: to_char doctest #18 (result)
      When query
        """
        SELECT '[' || (to_char(0, '9')) || ']' AS r
        """
      Then query result
        | r   |
        | [ ] |

    Scenario: to_char doctest #19 (result)
      When query
        """
        SELECT '[' || (to_char(0, '99')) || ']' AS r
        """
      Then query result
        | r    |
        | [  ] |

    Scenario: to_char doctest #20 (result)
      When query
        """
        SELECT '[' || (to_char(0, '0')) || ']' AS r
        """
      Then query result
        | r   |
        | [0] |

    Scenario: to_char doctest #21 (result)
      When query
        """
        SELECT '[' || (to_char(0, '00')) || ']' AS r
        """
      Then query result
        | r    |
        | [00] |

    Scenario: to_char doctest #22 (result)
      When query
        """
        SELECT '[' || (to_char(0, '99.99')) || ']' AS r
        """
      Then query result
        | r       |
        | [ 0.00] |

    Scenario: to_char doctest #23 (result)
      When query
        """
        SELECT '[' || (to_char(0, '00.00')) || ']' AS r
        """
      Then query result
        | r       |
        | [00.00] |

    Scenario: to_char doctest #24 (result)
      When query
        """
        SELECT '[' || (to_char(0, 'S99')) || ']' AS r
        """
      Then query result
        | r     |
        | [  +] |

    Scenario: to_char doctest #25 (result)
      When query
        """
        SELECT '[' || (to_char(0, '99S')) || ']' AS r
        """
      Then query result
        | r     |
        | [+  ] |

    Scenario: to_char doctest #26 (result)
      When query
        """
        SELECT '[' || (to_char(0, '$99.99')) || ']' AS r
        """
      Then query result
        | r        |
        | [$ 0.00] |

    Scenario: to_char doctest #27 (result)
      When query
        """
        SELECT '[' || (to_char(0.0, '9.9')) || ']' AS r
        """
      Then query result
        | r     |
        | [0.0] |

    Scenario: to_char doctest #28 (result)
      When query
        """
        SELECT '[' || (to_char(78, '99.99')) || ']' AS r
        """
      Then query result
        | r       |
        | [78.00] |

    Scenario: to_char doctest #29 (result)
      When query
        """
        SELECT '[' || (to_char(78, '99.')) || ']' AS r
        """
      Then query result
        | r     |
        | [78 ] |

    Scenario: to_char doctest #30 (result)
      When query
        """
        SELECT '[' || (to_char(78, '99D')) || ']' AS r
        """
      Then query result
        | r     |
        | [78 ] |

    Scenario: to_char doctest #31 (result)
      When query
        """
        SELECT '[' || (to_char(0.45, '.99')) || ']' AS r
        """
      Then query result
        | r     |
        | [.45] |

    Scenario: to_char doctest #32 (result)
      When query
        """
        SELECT '[' || (to_char(0.45, 'D99')) || ']' AS r
        """
      Then query result
        | r     |
        | [.45] |

    Scenario: to_char doctest #33 (result)
      When query
        """
        SELECT '[' || (to_char(0.45, '9.99')) || ']' AS r
        """
      Then query result
        | r      |
        | [0.45] |

    Scenario: to_char doctest #34 (result)
      When query
        """
        SELECT '[' || (to_char(0.45, '99.99')) || ']' AS r
        """
      Then query result
        | r       |
        | [ 0.45] |

    Scenario: to_char doctest #35 (result)
      When query
        """
        SELECT '[' || (to_char(12454, '99G999')) || ']' AS r
        """
      Then query result
        | r        |
        | [12,454] |

    Scenario: to_char doctest #36 (result)
      When query
        """
        SELECT '[' || (to_char(1234, '9,999')) || ']' AS r
        """
      Then query result
        | r       |
        | [1,234] |

    Scenario: to_char doctest #37 (result)
      When query
        """
        SELECT '[' || (to_char(1234, '9G999')) || ']' AS r
        """
      Then query result
        | r       |
        | [1,234] |

    Scenario: to_char doctest #38 (result)
      When query
        """
        SELECT '[' || (to_char(45, '9,999')) || ']' AS r
        """
      Then query result
        | r       |
        | [   45] |

    Scenario: to_char doctest #39 (result)
      When query
        """
        SELECT '[' || (to_char(45, '0,000')) || ']' AS r
        """
      Then query result
        | r       |
        | [0,045] |

    Scenario: to_char doctest #40 (result)
      When query
        """
        SELECT '[' || (to_char(1234567, '9,999,999')) || ']' AS r
        """
      Then query result
        | r           |
        | [1,234,567] |

    Scenario: to_char doctest #41 (result)
      When query
        """
        SELECT '[' || (to_char(1234567, '9999,999')) || ']' AS r
        """
      Then query result
        | r          |
        | [1234,567] |

    Scenario: to_char doctest #42 (result)
      When query
        """
        SELECT '[' || (to_char(12454, '99G99,9')) || ']' AS r
        """
      Then query result
        | r         |
        | [12,45,4] |

    Scenario: to_char doctest #43 (result)
      When query
        """
        SELECT '[' || (to_char(1234.56, '9,999.99')) || ']' AS r
        """
      Then query result
        | r          |
        | [1,234.56] |

    Scenario: to_char doctest #44 (result)
      When query
        """
        SELECT '[' || (to_char(78.12, '$99.99')) || ']' AS r
        """
      Then query result
        | r        |
        | [$78.12] |

    Scenario: to_char doctest #45 (result)
      When query
        """
        SELECT '[' || (to_char(78.12, '$00.00')) || ']' AS r
        """
      Then query result
        | r        |
        | [$78.12] |

    Scenario: to_char doctest #46 (result)
      When query
        """
        SELECT '[' || (to_char(-78.12, '$99.99')) || ']' AS r
        """
      Then query result
        | r        |
        | [$78.12] |

    Scenario: to_char doctest #47 (result)
      When query
        """
        SELECT '[' || (to_char(0.12, '$.99')) || ']' AS r
        """
      Then query result
        | r      |
        | [$.12] |

    Scenario: to_char doctest #48 (result)
      When query
        """
        SELECT '[' || (to_char(454, 'S999')) || ']' AS r
        """
      Then query result
        | r      |
        | [+454] |

    Scenario: to_char doctest #49 (result)
      When query
        """
        SELECT '[' || (to_char(-454, 'S999')) || ']' AS r
        """
      Then query result
        | r      |
        | [-454] |

    Scenario: to_char doctest #50 (result)
      When query
        """
        SELECT '[' || (to_char(454, '999S')) || ']' AS r
        """
      Then query result
        | r      |
        | [454+] |

    Scenario: to_char doctest #51 (result)
      When query
        """
        SELECT '[' || (to_char(-454, '999S')) || ']' AS r
        """
      Then query result
        | r      |
        | [454-] |

    Scenario: to_char doctest #52 (result)
      When query
        """
        SELECT '[' || (to_char(-12454.8, '99G999D9S')) || ']' AS r
        """
      Then query result
        | r           |
        | [12,454.8-] |

    Scenario: to_char doctest #53 (result)
      When query
        """
        SELECT '[' || (to_char(-1, 'S9999')) || ']' AS r
        """
      Then query result
        | r       |
        | [   -1] |

    Scenario: to_char doctest #54 (result)
      When query
        """
        SELECT '[' || (to_char(1, 'S9999')) || ']' AS r
        """
      Then query result
        | r       |
        | [   +1] |

    Scenario: to_char doctest #55 (result)
      When query
        """
        SELECT '[' || (to_char(-1, 'S0000')) || ']' AS r
        """
      Then query result
        | r       |
        | [-0001] |

    Scenario: to_char doctest #56 (result)
      When query
        """
        SELECT '[' || (to_char(-454, 'S$999')) || ']' AS r
        """
      Then query result
        | r       |
        | [-$454] |

    Scenario: to_char doctest #57 (result)
      When query
        """
        SELECT '[' || (to_char(-78.12, 'S$99.99')) || ']' AS r
        """
      Then query result
        | r         |
        | [-$78.12] |

    Scenario: to_char doctest #58 (result)
      When query
        """
        SELECT '[' || (to_char(-78.12, '$99.99S')) || ']' AS r
        """
      Then query result
        | r         |
        | [$78.12-] |

    Scenario: to_char doctest #59 (result)
      When query
        """
        SELECT '[' || (to_char(454, 'MI999')) || ']' AS r
        """
      Then query result
        | r      |
        | [ 454] |

    Scenario: to_char doctest #60 (result)
      When query
        """
        SELECT '[' || (to_char(-454, 'MI999')) || ']' AS r
        """
      Then query result
        | r      |
        | [-454] |

    Scenario: to_char doctest #61 (result)
      When query
        """
        SELECT '[' || (to_char(454, '999MI')) || ']' AS r
        """
      Then query result
        | r      |
        | [454 ] |

    Scenario: to_char doctest #62 (result)
      When query
        """
        SELECT '[' || (to_char(-454, '999MI')) || ']' AS r
        """
      Then query result
        | r      |
        | [454-] |

    Scenario: to_char doctest #63 (result)
      When query
        """
        SELECT '[' || (to_char(-1, 'MI9999')) || ']' AS r
        """
      Then query result
        | r       |
        | [   -1] |

    Scenario: to_char doctest #64 (result)
      When query
        """
        SELECT '[' || (to_char(-1, '9999MI')) || ']' AS r
        """
      Then query result
        | r       |
        | [   1-] |

    Scenario: to_char doctest #65 (result)
      When query
        """
        SELECT '[' || (to_char(454, '999PR')) || ']' AS r
        """
      Then query result
        | r       |
        | [454  ] |

    Scenario: to_char doctest #66 (result)
      When query
        """
        SELECT '[' || (to_char(-454, '999PR')) || ']' AS r
        """
      Then query result
        | r       |
        | [<454>] |

    Scenario: to_char doctest #67 (result)
      When query
        """
        SELECT '[' || (to_char(-454, '9999PR')) || ']' AS r
        """
      Then query result
        | r        |
        | [ <454>] |

    Scenario: to_char doctest #68 (result)
      When query
        """
        SELECT '[' || (to_char(-4, '9999PR')) || ']' AS r
        """
      Then query result
        | r        |
        | [   <4>] |

    Scenario: to_char doctest #69 (result)
      When query
        """
        SELECT '[' || (to_char(-78.12, '$99.99PR')) || ']' AS r
        """
      Then query result
        | r          |
        | [<$78.12>] |

    Scenario: to_char doctest #70 (result)
      When query
        """
        SELECT '[' || (to_char(-454, 'S999PR')) || ']' AS r
        """
      Then query result
        | r        |
        | [<-454>] |

    Scenario: to_char doctest #71 (result)
      When query
        """
        SELECT '[' || (to_char(-0.1, 'S9.9')) || ']' AS r
        """
      Then query result
        | r      |
        | [ -.1] |

    Scenario: to_char doctest #72 (result)
      When query
        """
        SELECT '[' || (to_char(-0.1, '9.9S')) || ']' AS r
        """
      Then query result
        | r      |
        | [0.1-] |

    Scenario: to_char doctest #73 (result)
      When query
        """
        SELECT '[' || (to_char(-0.1, '9.9MI')) || ']' AS r
        """
      Then query result
        | r      |
        | [0.1-] |

    Scenario: to_char doctest #74 (result)
      When query
        """
        SELECT '[' || (to_char(0.1, '9.9S')) || ']' AS r
        """
      Then query result
        | r      |
        | [0.1+] |

    Scenario: to_char doctest #75 (result)
      When query
        """
        SELECT '[' || (to_char(78, '99.S')) || ']' AS r
        """
      Then query result
        | r      |
        | [78+ ] |

    Scenario: to_char doctest #76 (result)
      When query
        """
        SELECT '[' || (to_char(-78, '99.S')) || ']' AS r
        """
      Then query result
        | r      |
        | [78- ] |

    Scenario: to_char doctest #77 (result)
      When query
        """
        SELECT '[' || (to_char(-78, '99.PR')) || ']' AS r
        """
      Then query result
        | r       |
        | [<78> ] |

    Scenario: to_char doctest #78 (result)
      When query
        """
        SELECT '[' || (to_char(-454, '999')) || ']' AS r
        """
      Then query result
        | r     |
        | [454] |

    Scenario: to_char doctest #79 (result)
      When query
        """
        SELECT '[' || (to_char(-12.34, '99.99')) || ']' AS r
        """
      Then query result
        | r       |
        | [12.34] |

    Scenario: to_char doctest #80 (result)
      When query
        """
        SELECT '[' || (to_char(CAST(78.12 AS DECIMAL(10,4)), '99.99')) || ']' AS r
        """
      Then query result
        | r       |
        | [78.12] |

    Scenario: to_char doctest #81 (result)
      When query
        """
        SELECT '[' || (to_char(CAST(78.1 AS DECIMAL(10,4)), '99.99')) || ']' AS r
        """
      Then query result
        | r       |
        | [78.10] |

    Scenario: to_char doctest #82 (result)
      When query
        """
        SELECT '[' || (to_char(CAST(78 AS DECIMAL(10,4)), '99.99')) || ']' AS r
        """
      Then query result
        | r       |
        | [78.00] |

    Scenario: to_char doctest #83 (result)
      When query
        """
        SELECT '[' || (to_char(CAST(0.5 AS DECIMAL(10,4)), '99.99')) || ']' AS r
        """
      Then query result
        | r       |
        | [ 0.50] |

    Scenario: to_char doctest #84 (result)
      When query
        """
        SELECT '[' || (to_char(454.00, '000D00')) || ']' AS r
        """
      Then query result
        | r        |
        | [454.00] |

    Scenario: to_char doctest #85 (result)
      When query
        """
        SELECT '[' || (to_char(CAST(12454.8 AS DECIMAL(20,10)), '99G999D9S')) || ']' AS r
        """
      Then query result
        | r           |
        | [12,454.8+] |

    Scenario: to_char doctest #86 (result)
      When query
        """
        SELECT '[' || (to_char(78.12D, '$99.99')) || ']' AS r
        """
      Then query result
        | r        |
        | [$78.12] |

    Scenario: to_char doctest #87 (result)
      When query
        """
        SELECT '[' || (to_char(CAST(0.001 AS DOUBLE), '9.999')) || ']' AS r
        """
      Then query result
        | r       |
        | [0.001] |

    Scenario: to_char doctest #88 (result)
      When query
        """
        SELECT '[' || (to_char(CAST(0.1 AS DOUBLE), '9.9')) || ']' AS r
        """
      Then query result
        | r     |
        | [0.1] |

    Scenario: to_char doctest #89 (result)
      When query
        """
        SELECT '[' || (to_char(CAST(-12454.8 AS DOUBLE), '99G999D9S')) || ']' AS r
        """
      Then query result
        | r           |
        | [12,454.8-] |

    Scenario: to_char doctest #90 (result)
      When query
        """
        SELECT '[' || (to_char(CAST(0.12345678901234567 AS DOUBLE), '9.99999999999999999')) || ']' AS r
        """
      Then query result
        | r                     |
        | [0.12345678901234600] |

    Scenario: to_char doctest #91 (result)
      When query
        """
        SELECT '[' || (to_char(CAST(1.5 AS FLOAT), '9.9')) || ']' AS r
        """
      Then query result
        | r     |
        | [1.5] |

    Scenario: to_char doctest #92 (result)
      When query
        """
        SELECT '[' || (to_char(CAST(78.12 AS FLOAT), '99.99')) || ']' AS r
        """
      Then query result
        | r       |
        | [##.##] |

    Scenario: to_char doctest #93 (result)
      When query
        """
        SELECT '[' || (to_char(CAST(127 AS TINYINT), '999')) || ']' AS r
        """
      Then query result
        | r     |
        | [127] |

    Scenario: to_char doctest #94 (result)
      When query
        """
        SELECT '[' || (to_char(CAST(-32768 AS SMALLINT), 'S99999')) || ']' AS r
        """
      Then query result
        | r        |
        | [-32768] |

    Scenario: to_char doctest #95 (result)
      When query
        """
        SELECT '[' || (to_char(CAST(2147483647 AS INT), '9999999999')) || ']' AS r
        """
      Then query result
        | r            |
        | [2147483647] |

    Scenario: to_char doctest #96 (result)
      When query
        """
        SELECT '[' || (to_char(CAST(9223372036854775807 AS BIGINT), '9999999999999999999')) || ']' AS r
        """
      Then query result
        | r                     |
        | [9223372036854775807] |

    Scenario: to_char doctest #97 (result)
      When query
        """
        SELECT '[' || (to_char('78.12', '99.99')) || ']' AS r
        """
      Then query result
        | r       |
        | [78.12] |

    Scenario: to_char doctest #98 (result)
      When query
        """
        SELECT '[' || (to_char('454', '999')) || ']' AS r
        """
      Then query result
        | r     |
        | [454] |

    Scenario: to_char doctest #99 (result)
      When query
        """
        SELECT '[' || (to_char('-454', 'S999')) || ']' AS r
        """
      Then query result
        | r      |
        | [-454] |

    Scenario: to_char doctest #100 (result)
      When query
        """
        SELECT '[' || (to_char(CAST(NULL AS DECIMAL(5,2)), '999')) || ']' AS r
        """
      Then query result
        | r    |
        | NULL |

    Scenario: to_char doctest #101 (result)
      When query
        """
        SELECT '[' || (to_char(CAST(NULL AS INT), '999')) || ']' AS r
        """
      Then query result
        | r    |
        | NULL |

    Scenario: to_char doctest #102 (result)
      When query
        """
        SELECT '[' || (to_char(454, CAST(NULL AS STRING))) || ']' AS r
        """
      Then query result
        | r    |
        | NULL |

    Scenario: to_char doctest #103 (result)
      When query
        """
        SELECT '[' || (to_char(CAST('12345678901234567890123456789012345678' AS DECIMAL(38,0)), '99999999999999999999999999999999999999')) || ']' AS r
        """
      Then query result
        | r                                        |
        | [12345678901234567890123456789012345678] |

    Scenario: to_char doctest #104 (result)
      When query
        """
        SELECT '[' || (to_char(CAST('1234567890.0987654321' AS DECIMAL(38,18)), '9999999999.999999999999999999')) || ']' AS r
        """
      Then query result
        | r                               |
        | [1234567890.098765432100000000] |

    Scenario: to_char doctest #105 (result)
      When query
        """
        SELECT '[' || (to_char(-12454.8, '99g999d9s')) || ']' AS r
        """
      Then query result
        | r           |
        | [12,454.8-] |

    Scenario: to_char doctest #106 (result)
      When query
        """
        SELECT '[' || (to_char(-454, '999pr')) || ']' AS r
        """
      Then query result
        | r       |
        | [<454>] |

    Scenario: to_char doctest #107 (result)
      When query
        """
        SELECT '[' || (to_char(-454, 'mi999')) || ']' AS r
        """
      Then query result
        | r      |
        | [-454] |

    Scenario: to_char doctest #108 (result)
      When query
        """
        SELECT '[' || (to_char(78.12, '$99d99')) || ']' AS r
        """
      Then query result
        | r        |
        | [$78.12] |

    Scenario: to_char doctest #109 (error)
      When query
        """
        SELECT to_char(454, '') AS r
        """
      Then query error (?i).*

    Scenario: to_char doctest #110 (error)
      When query
        """
        SELECT to_char(454, 'SS999') AS r
        """
      Then query error (?i).*

    Scenario: to_char doctest #111 (error)
      When query
        """
        SELECT to_char(454, '9.9.9') AS r
        """
      Then query error (?i).*

    Scenario: to_char doctest #112 (error)
      When query
        """
        SELECT to_char(454, '$$999') AS r
        """
      Then query error (?i).*

    Scenario: to_char doctest #113 (error)
      When query
        """
        SELECT to_char(454, '9$9') AS r
        """
      Then query error (?i).*

    Scenario: to_char doctest #114 (error)
      When query
        """
        SELECT to_char(454, '.99$') AS r
        """
      Then query error (?i).*

    Scenario: to_char doctest #115 (error)
      When query
        """
        SELECT to_char(78.12, '99.99$') AS r
        """
      Then query error (?i).*

    Scenario: to_char doctest #116 (error)
      When query
        """
        SELECT to_char(454, ',999') AS r
        """
      Then query error (?i).*

    Scenario: to_char doctest #117 (error)
      When query
        """
        SELECT to_char(454, '999,') AS r
        """
      Then query error (?i).*

    Scenario: to_char doctest #118 (error)
      When query
        """
        SELECT to_char(454, '9,,99') AS r
        """
      Then query error (?i).*

    Scenario: to_char doctest #119 (error)
      When query
        """
        SELECT to_char(454, '99.9,9') AS r
        """
      Then query error (?i).*

    Scenario: to_char doctest #120 (error)
      When query
        """
        SELECT to_char(454, 'PR999') AS r
        """
      Then query error (?i).*

    Scenario: to_char doctest #121 (error)
      When query
        """
        SELECT to_char(454, 'MI999MI') AS r
        """
      Then query error (?i).*

    Scenario: to_char doctest #122 (error)
      When query
        """
        SELECT to_char(454, 'x99') AS r
        """
      Then query error (?i).*

    Scenario: to_char doctest #123 (error)
      When query
        """
        SELECT to_char(454, '99x') AS r
        """
      Then query error (?i).*

    Scenario: to_char doctest #124 (error)
      When query
        """
        SELECT to_char(12.34, '99v99') AS r
        """
      Then query error (?i).*

    Scenario: to_char doctest #125 (error)
      When query
        """
        SELECT to_char(454, 'S') AS r
        """
      Then query error (?i).*

    Scenario: to_char doctest #126 (error)
      When query
        """
        SELECT to_char(454, '$') AS r
        """
      Then query error (?i).*

    Scenario: to_char doctest #127 (error)
      When query
        """
        SELECT to_char(454, 'MI') AS r
        """
      Then query error (?i).*

    Scenario: to_char doctest #128 (error)
      When query
        """
        SELECT to_char(454, 'PR') AS r
        """
      Then query error (?i).*

    Scenario: to_char doctest #129 (error)
      When query
        """
        SELECT to_char(454, '999PRMI') AS r
        """
      Then query error (?i).*

    @sail-only
    Scenario: to_char doctest #130 (result)
      When query
        """
        SELECT '[' || (to_char(date'2016-04-08', 'y')) || ']' AS r
        """
      Then query result
        | r      |
        | [2016] |

    @sail-only
    Scenario: to_char doctest #131 (result)
      When query
        """
        SELECT '[' || (to_char(date'2022-04-08', 'yyyy-MM-dd')) || ']' AS r
        """
      Then query result
        | r            |
        | [2022-04-08] |

    @sail-only
    Scenario: to_char doctest #132 (result)
      When query
        """
        SELECT '[' || (to_char(timestamp'2016-04-08 13:14:15', 'yyyy-MM-dd HH:mm:ss')) || ']' AS r
        """
      Then query result
        | r                     |
        | [2016-04-08 13:14:15] |

    @sail-only
    Scenario: to_char doctest #133 (result)
      When query
        """
        SELECT '[' || (to_char(date'2022-04-08', 'MMM')) || ']' AS r
        """
      Then query result
        | r     |
        | [Apr] |

    @sail-only
    Scenario: to_char doctest #134 (result)
      When query
        """
        SELECT '[' || (to_char(date'2022-04-08', 'E')) || ']' AS r
        """
      Then query result
        | r     |
        | [Fri] |

    @sail-only
    Scenario: to_char doctest #135 (result)
      When query
        """
        SELECT '[' || (to_char(timestamp_ntz'2016-04-08 13:14:15', 'yyyy MM dd')) || ']' AS r
        """
      Then query result
        | r            |
        | [2016 04 08] |

    @sail-only
    Scenario: to_char doctest #136 (result)
      When query
        """
        SELECT '[' || (to_char(CAST(NULL AS DATE), 'yyyy')) || ']' AS r
        """
      Then query result
        | r    |
        | NULL |

    @sail-only
    Scenario: to_char doctest #137 (result)
      When query
        """
        SELECT '[' || (to_char(encode('Spark SQL', 'utf-8'), 'base64')) || ']' AS r
        """
      Then query result
        | r              |
        | [U3BhcmsgU1FM] |

    @sail-only
    Scenario: to_char doctest #138 (result)
      When query
        """
        SELECT '[' || (to_char(encode('Spark SQL', 'utf-8'), 'hex')) || ']' AS r
        """
      Then query result
        | r                    |
        | [537061726B2053514C] |

    @sail-only
    Scenario: to_char doctest #139 (result)
      When query
        """
        SELECT '[' || (to_char(encode('abc', 'utf-8'), 'utf-8')) || ']' AS r
        """
      Then query result
        | r     |
        | [abc] |

    @sail-only
    Scenario: to_char doctest #140 (result)
      When query
        """
        SELECT '[' || (to_char(encode('Spark SQL', 'utf-8'), 'BASE64')) || ']' AS r
        """
      Then query result
        | r              |
        | [U3BhcmsgU1FM] |

    @sail-only
    Scenario: to_char doctest #141 (result)
      When query
        """
        SELECT '[' || (to_char(encode('Spark SQL', 'utf-8'), ' hex ')) || ']' AS r
        """
      Then query result
        | r                    |
        | [537061726B2053514C] |

    @sail-only
    Scenario: to_char doctest #142 (result)
      When query
        """
        SELECT '[' || (to_char(encode('abc', 'utf-8'), 'UTF-8')) || ']' AS r
        """
      Then query result
        | r     |
        | [abc] |

    @sail-only
    Scenario: to_char doctest #143 (error)
      When query
        """
        SELECT to_char(encode('abc', 'utf-8'), 'foo') AS r
        """
      Then query error (?i).*

    @sail-only
    Scenario: to_char doctest #144 (error)
      When query
        """
        SELECT to_char(encode('abc', 'utf-8'), CAST(NULL AS STRING)) AS r
        """
      Then query error (?i).*
