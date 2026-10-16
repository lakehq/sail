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

  Rule: group index validated only on a match (Spark parity)

    # Spark validates the group index PER MATCH: a zero-match row returns [] even
    # for an invalid idx; an invalid idx only errors when there is at least one match.
    @sail-bug
    Scenario: regexp_extract_all negative idx with no match returns empty
      When query
      """
      SELECT regexp_extract_all('abc', r'(\d+)', -1) AS result
      """
      Then query result
      | result |
      | []     |

    Scenario: regexp_extract_all out-of-range idx with no match returns empty
      When query
      """
      SELECT regexp_extract_all('abc', r'(\d+)', 5) AS result
      """
      Then query result
      | result |
      | []     |

    Scenario: regexp_extract_all negative idx with a match errors
      When query
      """
      SELECT regexp_extract_all('1a2b', r'(\d+)', -1) AS result
      """
      Then query error (?i).*group index.*

    Scenario: regexp_extract_all out-of-range idx with a match errors
      When query
      """
      SELECT regexp_extract_all('1a2b', r'(\d+)', 5) AS result
      """
      Then query error (?i).*group index.*

  Rule: java.util.regex vs Rust `regex` divergences (@sail-bug)

    # Sail uses Rust's `regex` crate; Spark uses `java.util.regex`. These are the
    # known differences (verified vs Spark JVM). They affect ALL Sail regex funcs.

    # Rust `regex` does not support backreferences (Sail errors, Spark matches).
    @sail-bug
    Scenario: regexp_extract_all backreference
      When query
      """
      SELECT regexp_extract_all('abcabc', r'(abc)\1', 0) AS result
      """
      Then query result
      | result   |
      | [abcabc] |

    # Rust `regex` does not support lookaround (Sail errors, Spark matches).
    @sail-bug
    Scenario: regexp_extract_all lookahead
      When query
      """
      SELECT regexp_extract_all('foobar', r'foo(?=bar)', 0) AS result
      """
      Then query result
      | result |
      | [foo]  |

    # Rust `\w` is Unicode-aware; Java `\w` is ASCII-only — 'é' matches in Sail, not Spark.
    @sail-bug
    Scenario: regexp_extract_all word class is ASCII in Spark, Unicode in Sail
      When query
      """
      SELECT regexp_extract_all('café', r'(\w+)', 1) AS result
      """
      Then query result
      | result |
      | [caf]  |

    # Rust `regex` supports POSIX classes; Java treats `[[:digit:]]` literally.
    @sail-bug
    Scenario: regexp_extract_all POSIX character class
      When query
      """
      SELECT regexp_extract_all('a1b2', r'[[:digit:]]', 0) AS result
      """
      Then query result
      | result |
      | []     |

    # Zero-width matches: Java emits a trailing empty at end-of-input, Rust does not.
    @sail-bug
    Scenario: regexp_extract_all zero-width match count
      When query
      """
      SELECT to_json(regexp_extract_all('abc', r'a*', 0)) AS result
      """
      Then query result
      | result            |
      | ["a","","",""]    |
