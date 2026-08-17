Feature: to_number comprehensive tests

  Rule: Argument count validation

    Scenario Outline: Argument count: <case>
      When query
        """
        SELECT to_number(<args>) AS result
        """
      Then query error .*

      Examples:
        | case                             | args                  |
        | to_number zero arguments errors  |                       |
        | to_number one argument errors    | '123'                 |
        | to_number three arguments errors | '123', '999', 'extra' |

  Rule: NULL combinatorial

    Scenario Outline: NULL: <case>
      When query
        """
        SELECT to_number(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                               | args                                       |
        | to_number NULL value               | CAST(NULL AS STRING), '999'                |
        | to_number NULL format returns NULL | '123', CAST(NULL AS STRING)                |
        | to_number both NULL returns NULL   | CAST(NULL AS STRING), CAST(NULL AS STRING) |

  Rule: Basic parsing

    Scenario Outline: Basic parsing: <case>
      When query
        """
        SELECT to_number(<value>, <format>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                           | value    | format   | result |
        | to_number basic integer        | '123'    | '999'    | 123    |
        | to_number zero-padded          | '00042'  | '00000'  | 42     |
        | to_number with comma separator | '12,345' | '99,999' | 12345  |
        | to_number with decimal         | '1.23'   | '9.99'   | 1.23   |
        | to_number with dollar sign     | '$1,234' | '$9,999' | 1234   |
        | to_number zero                 | '0'      | '9'      | 0      |
        | to_number leading spaces       | '  42'   | '999'    | 42     |

  Rule: G and D separators

    Scenario Outline: Separator: <case>
      When query
        """
        SELECT to_number(<value>, <format>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | value    | format   | result |
        | to_number G separator | '12,345' | '99G999' | 12345  |
        | to_number D separator | '123.45' | '999D99' | 123.45 |

  Rule: Sign handling

    Scenario Outline: Sign: <case>
      When query
        """
        SELECT to_number(<value>, <format>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                         | value   | format  | result |
        | to_number S prefix negative  | '-123'  | 'S999'  | -123   |
        | to_number S prefix positive  | '+123'  | 'S999'  | 123    |
        | to_number S suffix negative  | '123-'  | '999S'  | -123   |
        | to_number S suffix positive  | '123+'  | '999S'  | 123    |
        | to_number MI prefix negative | '-123'  | 'MI999' | -123   |
        | to_number MI prefix space    | ' 123'  | 'MI999' | 123    |
        | to_number PR negative        | '<123>' | '999PR' | -123   |
        | to_number PR positive        | ' 123 ' | '999PR' | 123    |

  Rule: L format rejected

    Scenario: to_number L format rejected
      When query
        """
        SELECT to_number('$1,234', 'L9,999') AS result
        """
      Then query error .*

  Rule: Multi-row

    Scenario: to_number multi-row
      When query
        """
        SELECT to_number(v, '999') AS result FROM VALUES ('123'), ('  0'), (' 42') AS t(v)
        """
      Then query result
        | result |
        | 123    |
        | 0      |
        | 42     |

  Rule: Error conditions

    Scenario Outline: Error: <case>
      When query
        """
        SELECT to_number(<value>, '999') AS result
        """
      Then query error .*

      Examples:
        | case                              | value |
        | to_number mismatched input errors | 'abc' |
        | to_number all spaces errors       | '   ' |
        | to_number empty value errors      | ''    |

    Scenario: to_number empty format errors
      When query
        """
        SELECT to_number('123', '') AS result
        """
      Then query error .*

  Rule: Basic usage

    Scenario Outline: Basic: <case>
      When query
        """
        SELECT to_number(<value>, <format>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                               | value      | format     | result  |
        | integer with extra format slots    | '12'       | '999'      | 12      |
        | zero-padded input                  | '007'      | '009'      | 7       |
        | trailing zero preserved in decimal | '1.50'     | '9.99'     | 1.50    |
        | full combo thousands plus decimals | '1,234.56' | '9,999.99' | 1234.56 |
        | no integer part                    | '.5'       | '.9'       | 0.5     |

  Rule: Sign handling - S

    Scenario Outline: S sign: <case>
      When query
        """
        SELECT to_number(<value>, <format>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                 | value  | format | result |
        | S prefix with positive leading blank | ' 123' | 'S999' | 123    |
        | trailing S negative                  | ' 5-'  | '9S'   | -5     |
        | trailing S positive                  | ' 5+'  | '9S'   | 5      |

  Rule: Sign handling - MI

    Scenario Outline: MI sign: <case>
      When query
        """
        SELECT to_number(<value>, '999MI') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                    | value  | result |
        | MI suffix with negative | '123-' | -123   |
        | MI suffix with positive | '123'  | 123    |

  Rule: Currency

    Scenario: dollar prefix
      When query
        """
        SELECT to_number('$123', '$999') AS result
        """
      Then query result
        | result |
        | 123    |

  Rule: Grouping and decimal markers (case-insensitive)

    Scenario Outline: Marker: <case>
      When query
        """
        SELECT to_number(<value>, <format>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                 | value   | format  | result |
        | uppercase G grouping | '123'   | '9G999' | 123    |
        | uppercase D decimal  | '1.5'   | '9D9'   | 1.5    |
        | lowercase g grouping | '1,234' | '9g999' | 1234   |
        | lowercase d decimal  | '1.5'   | '9d9'   | 1.5    |

  Rule: Multi-row

    Scenario: multi-row with NULL
      When query
        """
        SELECT to_number(v, '999') AS result FROM VALUES
          ('1'),
          ('50'),
          ('100'),
          (CAST(NULL AS STRING)) AS t(v)
        """
      Then query result ordered
        | result |
        | 1      |
        | 50     |
        | 100    |
        | NULL   |

  Rule: Large magnitudes

    Scenario: 20-digit integer
      When query
        """
        SELECT to_number('99999999999999999999', '99999999999999999999') AS result
        """
      Then query result
        | result               |
        | 99999999999999999999 |

  Rule: Whitespace handling

    Scenario: surrounding spaces in input
      When query
        """
        SELECT to_number(' 123 ', '999') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: leading space only
      When query
        """
        SELECT to_number('  1', '999') AS result
        """
      Then query result
        | result |
        | 1      |

  Rule: Input errors (valid format, wrong input)

    Scenario Outline: Input error: <case>
      When query
        """
        SELECT to_number(<value>, <format>) AS result
        """
      Then query error .*

      Examples:
        | case                             | value  | format |
        | input larger than format         | '1234' | '999'  |
        | negative input without sign spec | '-123' | '999'  |
        | decimal in integer format        | '12.3' | '999'  |
        | dollar suffix not allowed        | '123$' | '999$' |

  Rule: Format errors

    Scenario Outline: Format error: <case>
      When query
        """
        SELECT to_number(<value>, <format>) AS result
        """
      Then query error .*

      Examples:
        | case               | value | format  |
        | double S in format | '123' | 'SS999' |
        | comma at start     | '123' | ',999'  |
        | comma at end       | '123' | '999,'  |
        | dot only           | '1.5' | '.'     |

  Rule: All-null short-circuit must NOT bypass format validation

    Scenario: to_number all-null column with invalid double-S format still errors
      When query
        """
        SELECT to_number(v, 'SS999') AS result FROM VALUES
          (CAST(NULL AS STRING)),
          (CAST(NULL AS STRING))
          AS t(v)
        """
      Then query error .*

    Scenario: to_number all-null column with invalid comma-start format still errors
      When query
        """
        SELECT to_number(v, ',999') AS result FROM VALUES
          (CAST(NULL AS STRING))
          AS t(v)
        """
      Then query error .*

    Scenario: to_number all-null column with invalid dot-only format still errors
      When query
        """
        SELECT to_number(v, '.') AS result FROM VALUES
          (CAST(NULL AS STRING))
          AS t(v)
        """
      Then query error .*

    Scenario: to_number all-null column with VALID format returns all NULL
      When query
        """
        SELECT to_number(v, '999') AS result FROM VALUES
          (CAST(NULL AS STRING)),
          (CAST(NULL AS STRING))
          AS t(v)
        """
      Then query result ordered
        | result |
        | NULL   |
        | NULL   |

  Rule: Format must be a constant literal

    Scenario: non-literal format column reference errors at planning time
      When query
        """
        SELECT to_number('123', fmt) AS result FROM VALUES ('$999') AS t(fmt)
        """
      Then query error .*

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null string literal yields a decimal
      When query
        """
        SELECT to_number('123', '999') AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(3,0) (nullable = false)
        """

    @sail-bug
    Scenario: a non-null string column yields a decimal
      When query
        """
        SELECT to_number(CAST(id AS STRING), '9') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: decimal(1,0) (nullable = false)
        """

    Scenario: a nullable string column stays nullable
      When query
        """
        SELECT to_number(c, '999') AS result FROM VALUES ('123'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: decimal(3,0) (nullable = true)
        """

  Rule: Result values (migrated from test_to_number.txt doctests)

    Scenario Outline: Doctest: <case>
      When query
        """
        SELECT to_number(<args>) AS result, typeof(to_number(<args>)) AS type
        """
      Then query result
        | result   | type   |
        | <result> | <type> |

      Examples:
        | case                           | args                                   | result        | type          |
        | to_number doctest #1 (result)  | '<-$12,345.67>', 'S$999,099.99PR'      | 12345.67      | decimal(8,2)  |
        | to_number doctest #2 (result)  | '<$1,212,345.67>', 'S$0,000,000.99PR'  | -1212345.67   | decimal(9,2)  |
        | to_number doctest #3 (result)  | '$345', 'S$999,099.99'                 | 345.00        | decimal(8,2)  |
        | to_number doctest #4 (result)  | '$045', 'S$999,099.99'                 | 45.00         | decimal(8,2)  |
        | to_number doctest #5 (result)  | '<1234>', '999999PR'                   | -1234         | decimal(6,0)  |
        | to_number doctest #6 (result)  | '12,454.8-', '99,999.9S'               | -12454.8      | decimal(6,1)  |
        | to_number doctest #7 (result)  | '<-$123,456.32>', 'S$999,999.999999PR' | 123456.320000 | decimal(12,6) |
        | to_number doctest #8 (result)  | '$123,456.32', 'MI$999,999.99S'        | 123456.32     | decimal(8,2)  |
        | to_number doctest #9 (result)  | '$123,456.32-', 'MI$999,999.99S'       | -123456.32    | decimal(8,2)  |
        | to_number doctest #10 (result) | '-$123,456.32-', 'MI$999,999.99S'      | 123456.32     | decimal(8,2)  |
        | to_number doctest #11 (result) | '-$123,456.32+', 'MI$999,999.99S'      | -123456.32    | decimal(8,2)  |
        | to_number doctest #12 (result) | '-$123,456.32', 'MI$999,999.99S'       | -123456.32    | decimal(8,2)  |
