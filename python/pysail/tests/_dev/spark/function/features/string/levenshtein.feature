Feature: levenshtein() returns edit distance between two strings

  Rule: Basic usage

    Scenario Outline: Basic: <case>
      When query
        """
        SELECT levenshtein(<s1>, <s2>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                        | s1       | s2        | result |
        | basic distance              | 'kitten' | 'sitting' | 3      |
        | identical strings           | 'hello'  | 'hello'   | 0      |
        | empty string vs non-empty   | ''       | 'abc'     | 3      |
        | both empty strings          | ''       | ''        | 0      |
        | single character difference | 'abc'    | 'adc'     | 1      |

  Rule: Threshold (3-argument form)

    Scenario Outline: Threshold: <case>
      When query
        """
        SELECT levenshtein(<s1>, <s2>, <threshold>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                  | s1       | s2        | threshold | result |
        | distance within threshold             | 'kitten' | 'sitting' | 4         | 3      |
        | distance exceeds threshold            | 'kitten' | 'sitting' | 2         | -1     |
        | distance equals threshold (boundary)  | 'kitten' | 'sitting' | 3         | 3      |
        | threshold zero with different strings | 'abc'    | 'def'     | 0         | -1     |
        | threshold zero with identical strings | 'abc'    | 'abc'     | 0         | 0      |

  Rule: Null handling

    Scenario Outline: Null handling: <case>
      When query
        """
        SELECT levenshtein(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                 | args                                       |
        | first argument null  | CAST(NULL AS STRING), 'hello'              |
        | second argument null | 'hello', CAST(NULL AS STRING)              |
        | both arguments null  | CAST(NULL AS STRING), CAST(NULL AS STRING) |
        | null threshold       | 'kitten', 'sitting', CAST(NULL AS INT)     |

  Rule: Unicode and special characters

    Scenario Outline: Unicode and special characters: <case>
      When query
        """
        SELECT levenshtein(<s1>, <s2>) AS result
        """
      Then query result
        | result |
        | 1      |

      Examples:
        | case                | s1            | s2             |
        | unicode strings     | 'café'        | 'cafe'         |
        | strings with spaces | 'hello world' | 'hello world!' |

  Rule: Column expressions

    Scenario Outline: Column expressions: <case>
      When query
        """
        SELECT levenshtein(s1, s2<threshold>) AS result
        FROM VALUES ('abc', 'abc'), ('abc', 'def'), ('kitten', 'sitting') AS t(s1, s2)
        """
      Then query result
        | result |
        | <r1>   |
        | <r2>   |
        | <r3>   |

      Examples:
        | case                                     | threshold | r1 | r2 | r3 |
        | levenshtein on columns from inline table |           | 0  | 3  | 3  |
        | threshold on columns from inline table   | , 2       | 0  | -1 | -1 |

  Rule: Per-row threshold (threshold varies per row)

    Scenario: different threshold per row
      When query
        """
        SELECT levenshtein(s1, s2, t) AS result
        FROM VALUES ('abc', 'def', 2), ('abc', 'def', 5), ('abc', 'def', 3) AS t(s1, s2, t)
        ORDER BY t
        """
      Then query result ordered
        | result |
        | -1     |
        | 3      |
        | 3      |

    Scenario: null threshold per row treated as zero
      When query
        """
        SELECT levenshtein(s1, s2, t) AS result
        FROM VALUES ('kitten', 'sitting', CAST(NULL AS INT)), ('abc', 'def', 1) AS t(s1, s2, t)
        ORDER BY t NULLS FIRST
        """
      Then query result ordered
        | result |
        | -1     |
        | -1     |

    Scenario: null threshold per row with identical strings returns zero
      When query
        """
        SELECT levenshtein(s1, s2, t) AS result
        FROM VALUES ('abc', 'abc', CAST(NULL AS INT)) AS t(s1, s2, t)
        """
      Then query result
        | result |
        | 0      |

    Scenario: null strings with threshold in columns
      When query
        """
        SELECT levenshtein(s1, s2, t) AS result
        FROM VALUES
          (CAST(NULL AS STRING), 'hello', 5),
          ('hello', CAST(NULL AS STRING), 5),
          (CAST(NULL AS STRING), CAST(NULL AS STRING), 5),
          ('abc', 'abc', CAST(NULL AS INT)),
          ('abc', 'def', CAST(NULL AS INT))
        AS t(s1, s2, t)
        """
      Then query result
        | result |
        | NULL   |
        | NULL   |
        | NULL   |
        | 0      |
        | -1     |

  Rule: Threshold edge cases

    Scenario Outline: Threshold edge case: <case>
      When query
        """
        SELECT levenshtein(<s1>, <s2>, <threshold>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                        | s1       | s2        | threshold | result |
        | negative threshold returns minus one                        | 'kitten' | 'sitting' | -1        | -1     |
        | negative threshold with identical strings returns minus one | 'abc'    | 'abc'     | -1        | -1     |
        | very large threshold returns actual distance                | 'abc'    | 'def'     | 1000      | 3      |
        | threshold equals one at boundary                            | 'abc'    | 'adc'     | 1         | 1      |

  Rule: Case sensitivity and long strings

    Scenario Outline: Case sensitivity and long strings: <case>
      When query
        """
        SELECT levenshtein(<s1>, <s2>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                      | s1               | s2               | result |
        | case sensitive comparison | 'ABC'            | 'abc'            | 3      |
        | long strings              | REPEAT('a', 100) | REPEAT('b', 100) | 100    |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: non-null string literals yield a non-nullable integer
      When query
        """
        SELECT levenshtein('kitten', 'sitting') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    @sail-bug
    Scenario: non-null string columns yield a non-nullable integer
      When query
        """
        SELECT levenshtein(CAST(id AS STRING), 'x') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable string column stays nullable
      When query
        """
        SELECT levenshtein(c, 'x') AS result FROM VALUES ('a'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
