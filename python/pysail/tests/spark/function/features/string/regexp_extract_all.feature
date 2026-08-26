Feature: regexp_extract_all() extracts all regex capture group matches from strings

  Rule: Basic extraction with group index

    Scenario Outline: Group index: <case>
      When query
        """
        SELECT regexp_extract_all('100-200,300-400,500-600', r'(\d+)-(\d+)', <idx>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                               | idx | result                      |
        | regexp_extract_all with group index 0 returns entire matches       | 0   | [100-200, 300-400, 500-600] |
        | regexp_extract_all with group index 1 returns first capture group  | 1   | [100, 300, 500]             |
        | regexp_extract_all with group index 2 returns second capture group | 2   | [200, 400, 600]             |

  Rule: Default group index

    Scenario: regexp_extract_all defaults to group index 1
      When query
        """
        SELECT regexp_extract_all('1a 2b 14m', r'(\d+)([a-z]+)') AS result
        """
      Then query result
        | result     |
        | [1, 2, 14] |

    Scenario: regexp_extract_all without idx renders the synthesized idx=1 in the column name
      When query
        """
        SELECT regexp_extract_all('1a 2b 14m', r'([0-9]+)([a-z]+)')
        """
      Then query result
        | regexp_extract_all(1a 2b 14m, ([0-9]+)([a-z]+), 1) |
        | [1, 2, 14]                                         |

  Rule: No match and edge cases

    Scenario: regexp_extract_all returns empty array when no match
      When query
        """
        SELECT regexp_extract_all('foo', r'(\d+)', 1) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: regexp_extract_all returns empty strings for unmatched optional groups
      When query
        """
        SELECT to_json(regexp_extract_all('aaaac aaabc', r'(a+)(b)?(c)', 2)) AS result
        """
      Then query result
        | result   |
        | ["","b"] |

  Rule: NULL handling

    Scenario Outline: NULL handling: <case>
      When query
        """
        SELECT regexp_extract_all(<args>, 1) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                                 | args           |
        | regexp_extract_all returns NULL when input is NULL   | NULL, r'(\d+)' |
        | regexp_extract_all returns NULL when pattern is NULL | 'abc', NULL    |

  Rule: Pattern from a column

    Scenario: regexp_extract_all with the pattern supplied by a column
      When query
        """
        SELECT regexp_extract_all(s, p, 1) AS result FROM VALUES ('1a2b', '([0-9])'), ('3c4d', '([0-9])'), ('3c4d', '([a-z])'), ('5e6f', CAST(NULL AS STRING)) AS t(s, p)
        """
      Then query result
        | result |
        | [1, 2] |
        | [3, 4] |
        | [c, d] |
        | NULL   |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null string literal yields a non-nullable array
      When query
        """
        SELECT regexp_extract_all('a1b2', '[0-9]', 0) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = true)
        """

    Scenario: a nullable string column stays nullable
      When query
        """
        SELECT regexp_extract_all(c, '[0-9]', 0) AS result FROM VALUES ('a1'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: string (containsNull = true)
        """

    @sail-bug
    Scenario: a non-null string column yields a non-nullable array
      When query
        """
        SELECT regexp_extract_all(CAST(id AS STRING), '[0-9]', 0) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = true)
        """
