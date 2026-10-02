Feature: regexp_extract() extracts regex capture groups from strings

  Rule: Basic extraction

    Scenario Outline: Basic extraction: <case>
      When query
        """
        SELECT regexp_extract(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                   | args                           | result |
        | regexp_extract with group index 1 (default)            | '100-200', '(\\d+)-(\\d+)', 1  | 100    |
        | regexp_extract with group index 2                      | '100-200', '(\\d+)-(\\d+)', 2  | 200    |
        | regexp_extract with group index 0 returns entire match | 'hello 123 world', '(\\d+)', 0 | 123    |
        | regexp_extract defaults to group index 1               | 'abc-def', '([a-z]+)-([a-z]+)' | abc    |

  Rule: Multiple groups

    Scenario Outline: Multiple groups: <case>
      When query
        """
        SELECT regexp_extract(<value>, <pattern>, <group>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                                 | value        | pattern                | group | result |
        | regexp_extract on date-like string group 1                           | '2024-01-15' | '(\\d+)-(\\d+)-(\\d+)' | 1     | 2024   |
        | regexp_extract on date-like string group 2                           | '2024-01-15' | '(\\d+)-(\\d+)-(\\d+)' | 2     | 01     |
        | regexp_extract on date-like string group 3                           | '2024-01-15' | '(\\d+)-(\\d+)-(\\d+)' | 3     | 15     |
        | regexp_extract returns empty string for unmatched optional group     | 'aaaac'      | '(a+)(b)?(c)'          | 2     |        |
        | regexp_extract preserves later groups after unmatched optional group | 'aaaac'      | '(a+)(b)?(c)'          | 3     | c      |

  Rule: No match and edge cases

    Scenario Outline: No match and edge cases: <case>
      When query
        """
        SELECT regexp_extract(<value>, <pattern>, 1) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                              | value    | pattern   | result |
        | regexp_extract returns empty string when no match | 'hello'  | '(\\d+)'  |        |
        | regexp_extract with anchored pattern at beginning | '123abc' | '^(\\d+)' | 123    |
        | regexp_extract with anchored pattern at end       | 'abc123' | '(\\d+)$' | 123    |

  Rule: Pattern from a column

    Scenario: regexp_extract with the pattern supplied by a column
      When query
        """
        SELECT regexp_extract(s, p, 1) AS result FROM VALUES ('a11b', '([0-9]+)'), ('a22b', '([0-9]+)'), ('a33b', '([a-z]+)'), ('a44b', CAST(NULL AS STRING)) AS t(s, p)
        """
      Then query result
        | result |
        | 11     |
        | 22     |
        | a      |
        | NULL   |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null string literal yields a non-nullable string
      When query
        """
        SELECT regexp_extract('abc123', '[0-9]+', 0) AS result
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
        SELECT regexp_extract(CAST(id AS STRING), '[0-9]', 0) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable string column stays nullable
      When query
        """
        SELECT regexp_extract(c, '[0-9]', 0) AS result FROM VALUES ('a1'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  Rule: group index validated only on a match (Spark parity)

    # Spark validates the group index PER MATCH: a no-match input returns '' even
    # for an invalid idx; an invalid idx only errors when the input matches.
    @sail-bug
    Scenario: regexp_extract negative idx with no match returns empty string
      When query
      """
      SELECT regexp_extract('abc', r'(\d+)', -1) AS result
      """
      Then query result
      | result |
      |        |

    Scenario: regexp_extract out-of-range idx with no match returns empty string
      When query
      """
      SELECT regexp_extract('abc', r'(\d+)', 5) AS result
      """
      Then query result
      | result |
      |        |

    Scenario: regexp_extract negative idx with a match errors
      When query
      """
      SELECT regexp_extract('1a2b', r'(\d+)', -1) AS result
      """
      Then query error (?i).*group index.*

    Scenario: regexp_extract out-of-range idx with a match errors
      When query
      """
      SELECT regexp_extract('1a2b', r'(\d+)', 5) AS result
      """
      Then query error (?i).*group index.*

  Rule: java.util.regex vs Rust `regex` divergences (@sail-bug)

    # Rust `regex` does not support backreferences (Sail errors, Spark matches).
    @sail-bug
    Scenario: regexp_extract backreference
      When query
      """
      SELECT regexp_extract('abcabc', r'(abc)\1', 0) AS result
      """
      Then query result
      | result |
      | abcabc |

    # Rust `\w` is Unicode-aware; Java `\w` is ASCII-only — 'é' matches in Sail, not Spark.
    @sail-bug
    Scenario: regexp_extract word class is ASCII in Spark, Unicode in Sail
      When query
      """
      SELECT regexp_extract('café', r'(\w+)', 1) AS result
      """
      Then query result
      | result |
      | caf    |
