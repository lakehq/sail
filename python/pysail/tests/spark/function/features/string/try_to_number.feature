@try_to_number
Feature: try_to_number comprehensive tests (safe version of to_number)

  Rule: Argument count validation

    Scenario Outline: Arity: <case>
      When query
        """
        SELECT try_to_number(<args>) AS result
        """
      Then query error .*

      Examples:
        | case                                 | args                  |
        | try_to_number zero arguments errors  |                       |
        | try_to_number one argument errors    | '123'                 |
        | try_to_number three arguments errors | '123', '999', 'extra' |

  Rule: NULL combinatorial

    Scenario Outline: NULL combinatorial: <case>
      When query
        """
        SELECT try_to_number(<value>, <format>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                      | value                | format               |
        | try_to_number NULL value  | CAST(NULL AS STRING) | '999'                |
        | try_to_number NULL format | '123'                | CAST(NULL AS STRING) |
        | try_to_number both NULL   | CAST(NULL AS STRING) | CAST(NULL AS STRING) |

  Rule: Basic parsing (same as to_number)

    Scenario Outline: Basic parsing: <case>
      When query
        """
        SELECT try_to_number(<value>, <format>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                        | value    | format   | result |
        | try_to_number basic integer | '123'    | '999'    | 123    |
        | try_to_number decimal       | '1.23'   | '9.99'   | 1.23   |
        | try_to_number with dollar   | '$1,234' | '$9,999' | 1234   |
        | try_to_number S negative    | '-123'   | 'S999'   | -123   |
        | try_to_number G separator   | '12,345' | '99G999' | 12345  |
        | try_to_number D separator   | '123.45' | '999D99' | 123.45 |

  Rule: Safe behavior (errors become NULL)

    Scenario Outline: Safe behavior: <case>
      When query
        """
        SELECT try_to_number(<value>, '999') AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                       | value  |
        | try_to_number invalid string returns NULL  | 'abc'  |
        | try_to_number format mismatch returns NULL | '$123' |
        | try_to_number all spaces returns NULL      | '   '  |
        | try_to_number empty value returns NULL     | ''     |

  Rule: Multi-row with mixed valid and invalid

    Scenario: try_to_number multi-row with invalid
      When query
        """
        SELECT try_to_number(v, '999') AS result FROM VALUES ('123'), ('abc'), (' 42'), (NULL), (''), ('  0') AS t(v)
        """
      Then query result
        | result |
        | 123    |
        | NULL   |
        | 42     |
        | NULL   |
        | NULL   |
        | 0      |

  Rule: Error conditions (type errors still error)

    Scenario: try_to_number empty format errors
      When query
        """
        SELECT try_to_number('123', '') AS result
        """
      Then query error .*

  Rule: Result values (migrated from test_try_to_number.txt doctests)

    Scenario Outline: Doctest: <case>
      When query
        """
        SELECT try_to_number(<value>, <format>) AS result, typeof(try_to_number(<value>, <format>)) AS type
        """
      Then query result
        | result   | type   |
        | <result> | <type> |

      Examples:
        | case                               | value             | format               | result         | type          |
        | try_to_number doctest #1 (result)  | '<-$12,345.67>'   | 'S$999,099.99PR'     | 12345.67       | decimal(8,2)  |
        | try_to_number doctest #2 (result)  | '<$1,212,345.67>' | 'S$0,000,000.99PR'   | -1212345.67    | decimal(9,2)  |
        | try_to_number doctest #3 (result)  | '$345'            | 'S$999,099.99'       | 345.00         | decimal(8,2)  |
        | try_to_number doctest #4 (result)  | '$045'            | 'S$999,099.99'       | 45.00          | decimal(8,2)  |
        | try_to_number doctest #5 (result)  | '<1234>'          | '999999PR'           | -1234          | decimal(6,0)  |
        | try_to_number doctest #6 (result)  | '12,454.8-'       | '99,999.9S'          | -12454.8       | decimal(6,1)  |
        | try_to_number doctest #7 (result)  | '<-$123,456.32>'  | 'S$999,999.999999PR' | 123456.320000  | decimal(12,6) |
        | try_to_number doctest #8 (result)  | '$123,456.32'     | 'MI$999,999.99S'     | 123456.32      | decimal(8,2)  |
        | try_to_number doctest #9 (result)  | '$123,456.32-'    | 'MI$999,999.99S'     | -123456.32     | decimal(8,2)  |
        | try_to_number doctest #10 (result) | '-$123,456.32-'   | 'MI$999,999.99S'     | 123456.32      | decimal(8,2)  |
        | try_to_number doctest #11 (result) | '-$123,456.32+'   | 'MI$999,999.99S'     | -123456.32     | decimal(8,2)  |
        | try_to_number doctest #12 (result) | '-$123,456.32'    | 'MI$999,999.99S'     | -123456.32     | decimal(8,2)  |
        | try_to_number doctest #13 (result) | '045'             | 'S$999,099.99'       | NULL           | decimal(8,2)  |
        | try_to_number doctest #14 (result) | '1234'            | '999999PR'           | 1234           | decimal(6,0)  |
        | try_to_number doctest #16 (result) | '1234>'           | '999999PR'           | NULL           | decimal(6,0)  |
        | try_to_number doctest #17 (result) | '12454.8-'        | '99,999.9S'          | NULL           | decimal(6,1)  |
        | try_to_number doctest #19 (result) | '-$123,456.32'    | 'S$999,999.999999PR' | -123456.320000 | decimal(12,6) |
        | try_to_number doctest #20 (result) | '-123,456.32-'    | 'MI$999,999.99S'     | NULL           | decimal(8,2)  |
        | try_to_number doctest #21 (result) | '+<$123,456.32+'  | 'MI$999,999.99S'     | NULL           | decimal(8,2)  |
        | try_to_number doctest #22 (result) | '-123,456.32'     | 'MI$999,999.99S'     | NULL           | decimal(8,2)  |

    Scenario Outline: Doctest (typeof on a different value): <case>
      When query
        """
        SELECT try_to_number(<value>, <format>) AS result, typeof(try_to_number(<type_value>, <format>)) AS type
        """
      Then query result
        | result   | type   |
        | <result> | <type> |

      Examples:
        | case                               | value           | type_value        | format             | result | type         |
        | try_to_number doctest #15 (result) | '<1234>'        | '>1234>'          | '999999PR'         | -1234  | decimal(6,0) |
        | try_to_number doctest #18 (result) | '<$1212345.67>' | '<$1,212,345.67>' | 'S$0,000,000.99PR' | NULL   | decimal(9,2) |
