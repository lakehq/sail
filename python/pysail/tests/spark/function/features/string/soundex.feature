Feature: soundex() returns the Soundex code of a string

  Rule: Basic usage

    Scenario Outline: Basic: <case>
      When query
        """
        SELECT soundex(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | input    | result |
        | standard word         | 'Robert' | R163   |
        | single character      | 'A'      | A000   |
        | all same code letters | 'BFPV'   | B000   |

    Scenario Outline: Comparison: <case>
      When query
        """
        SELECT soundex('Robert') = soundex(<other>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                             | other    | result |
        | similar sounding names produce same code         | 'Rupert' | true   |
        | different sounding names produce different codes | 'Smith'  | false  |

  Rule: Edge cases

    Scenario Outline: Edge case: <case>
      When query
        """
        SELECT soundex(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                              | input                | result |
        | empty string                                      | ''                   |        |
        | numeric string returns input unchanged            | '123'                | 123    |
        | non-alpha first character returns input unchanged | '123abc'             | 123abc |
        | space first character returns input unchanged     | ' abc'               | abc    |
        | null input returns null                           | CAST(NULL AS STRING) | NULL   |

  Rule: Non-alpha characters after first letter act as separators

    Scenario Outline: Separator: <case>
      When query
        """
        SELECT soundex(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                              | input  | result |
        | digit separates same-code letters | 'B1F'  | B100   |
        | space separates same-code letters | 'B F'  | B100   |
        | letters with embedded digits      | 'a1bc' | A120   |

  Rule: H and W handling (ignored separators)

    Scenario Outline: H and W: <case>
      When query
        """
        SELECT soundex(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                    | input      | result |
        | H and W do not separate identical codes | 'Ashcraft' | A261   |
        | vowel separates identical codes         | 'Tymczak'  | T522   |

  Rule: Column expressions

    Scenario Outline: Column expressions: <case>
      When query
        """
        SELECT soundex(name) AS result
        FROM VALUES <values> AS t(name)
        """
      Then query result
        | result |
        | <r1>   |
        | <r2>   |
        | <r3>   |

      Examples:
        | case                        | values                                       | r1   | r2   | r3   |
        | soundex on column values    | ('Robert'), ('Rupert'), ('Smith')            | R163 | R163 | S530 |
        | soundex with null in column | ('Hello'), (CAST(NULL AS STRING)), ('World') | H400 | NULL | W643 |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null string literal yields a non-nullable string
      When query
        """
        SELECT soundex('Miller') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null string column yields a non-nullable string
      When query
        """
        SELECT soundex(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable string column stays nullable
      When query
        """
        SELECT soundex(c) AS result FROM VALUES ('Miller'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
