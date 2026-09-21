Feature: substring() and substr() extract substrings

  Rule: Basic usage with positive positions (1-based)

    Scenario Outline: Positive position: <case>
      When query
        """
        SELECT <fn>(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                | fn        | args              | result |
        | substring with pos=1 returns full length from start | substring | 'Spark SQL', 1, 4 | Spar   |
        | substring with pos=5 returns from that position     | substring | 'Spark SQL', 5, 1 | k      |
        | substring without length returns tail               | substring | 'Spark SQL', 7    | SQL    |
        | substr is an alias for substring                    | substr    | 'Spark SQL', 1, 5 | Spark  |

  Rule: Position zero is treated as position one (Spark semantics)

    Scenario Outline: Position zero: <case>
      When query
        """
        SELECT <fn>(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                               | fn        | args                     | result          |
        | substring with pos=0 returns same as pos=1         | substring | 'Spark SQL', 0, 5        | Spark           |
        | substring with pos=0 returns full requested length | substring | 'abcdefghijklmno', 0, 15 | abcdefghijklmno |
        | substr with pos=0 returns tail same as pos=1       | substr    | 'Spark SQL', 0           | Spark SQL       |

  Rule: Negative positions count from the end of the string

    Scenario Outline: Negative position: <case>
      When query
        """
        SELECT <fn>(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                     | fn        | args               | result |
        | substring with pos=-3 starts 3 chars from the end        | substring | 'Spark SQL', -3, 3 | SQL    |
        | substring with pos=-1 starts at last character           | substring | 'Spark SQL', -1, 1 | L      |
        | substr with negative pos returns tail from that position | substr    | 'Spark SQL', -3    | SQL    |

  Rule: Edge cases

    Scenario Outline: Edge case: <case>
      When query
        """
        SELECT substring(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                            | args                       | result |
        | substring length exceeds remaining string       | 'Spark', 3, 100            | ark    |
        | substring with zero length returns empty string | 'Spark SQL', 1, 0          |        |
        | substring on null returns null                  | CAST(NULL AS STRING), 1, 3 | NULL   |

    Scenario: substring on column values with pos=0
      When query
        """
        SELECT substring(id, 0, 15) AS result
        FROM VALUES ('abcdefghijklmno') AS t(id)
        """
      Then query result
        | result          |
        | abcdefghijklmno |

  Rule: Literal prefixes on string columns

    Scenario Outline: Literal prefixes preserve mixed string columns with length <length>
      When query
        """
        SELECT substring(v, 1, <length>) AS prefix,
               substr(v, 0, <length>) AS zero_prefix,
               substr(substring(v, 1, 20), 1, <length>) AS nested_prefix
        FROM VALUES
          (1, 'abcdefghijklmnopqrstu'),
          (2, 'é🙂漢abcdef'),
          (3, 'éclair'),
          (4, 'x'),
          (5, '🙂'),
          (6, ''),
          (7, CAST(NULL AS STRING))
        AS t(id, v)
        ORDER BY id
        """
      Then query result collected ordered
        | prefix      | zero_prefix | nested_prefix |
        | <ascii>     | <ascii>     | <ascii>       |
        | <unicode>   | <unicode>   | <unicode>     |
        | <combining> | <combining> | <combining>   |
        | <short>     | <short>     | <short>       |
        | <emoji>     | <emoji>     | <emoji>       |
        |             |             |               |
        | NULL        | NULL        | NULL          |
      Then query schema
        """
        root
         |-- prefix: string (nullable = true)
         |-- zero_prefix: string (nullable = true)
         |-- nested_prefix: string (nullable = true)
        """

      Examples:
        | length | ascii                | unicode     | combining | short | emoji |
        | 0      |                      |             |           |       |       |
        | 2      | ab                   | é🙂         | é        | x     | 🙂    |
        | 5      | abcde                | é🙂漢ab     | écla     | x     | 🙂    |
        | 20     | abcdefghijklmnopqrst | é🙂漢abcdef | éclair   | x     | 🙂    |

    Scenario: Positive non-prefix positions preserve character boundaries
      When query
        """
        SELECT substring(v, 2, 2) AS result, substr(v, 2, 2) AS alias_result
        FROM VALUES
          (1, 'abcdef'),
          (2, 'é🙂漢abc'),
          (3, 'x'),
          (4, ''),
          (5, CAST(NULL AS STRING))
        AS t(id, v)
        ORDER BY id
        """
      Then query result collected ordered
        | result | alias_result |
        | bc     | bc           |
        | 🙂漢   | 🙂漢         |
        |        |              |
        |        |              |
        | NULL   | NULL         |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null string literal yields a non-nullable string
      When query
        """
        SELECT substring('Spark', 1, 3) AS result
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
        SELECT substring(CAST(id AS STRING), 1, 1) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable string column stays nullable
      When query
        """
        SELECT substring(c, 1, 1) AS result FROM VALUES ('abc'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
