Feature: regexp_instr returns the first match position

  Rule: Argument semantics

    Scenario Outline: First match: <args>
      When query
        """
        SELECT regexp_instr(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | args                       | result |
        | 'ab12cd34', '[0-9]+'        | 3      |
        | 'ab12cd34', '[0-9]+', 0     | 3      |
        | 'ab12cd34', '[0-9]+', 1     | 3      |
        | 'ab12cd34', '[0-9]+', 2     | 3      |
        | 'ab12cd34', '[0-9]+', -1    | 3      |
        | 'ab12cd34', '[0-9]+', 9.5   | 3      |
        | 'ab12cd34', '[0-9]+', '2'   | 3      |
        | 'ab12cd34', '[0-9]+', NULL  | NULL   |
        | NULL, '[0-9]+', 1          | NULL   |
        | 'ab12cd34', NULL, 1        | NULL   |
        | 'abc', '[0-9]+', 2         | 0      |
        | '', '[0-9]+', 0            | 0      |

    Scenario: Invalid string index becomes NULL outside ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT regexp_instr('abc', 'a', 'invalid') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Invalid string index raises an error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT regexp_instr('abc', 'a', 'invalid') AS result
        """
      Then query error .*

    Scenario: NULL input short-circuits index conversion in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT regexp_instr(NULL, 'a', 'invalid') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Non-ANSI numeric index narrowing does not affect the match
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT regexp_instr('abc', 'a', 2147483648L) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: ANSI numeric index narrowing checks overflow
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT regexp_instr('abc', 'a', 2147483648L) AS result
        """
      Then query error .*

    Scenario: ANSI numeric index narrowing checks overflow for a column
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT regexp_instr('abc', 'a', id) AS result FROM range(2147483648, 2147483649)
        """
      Then query error .*

    Scenario: ANSI string index conversion checks column values
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT regexp_instr('abc', 'a', concat('invalid', CAST(id AS STRING))) AS result FROM range(1)
        """
      Then query error .*

    Scenario: NULL input short-circuits index conversion for each row
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT regexp_instr(s, 'a', i) AS result
        FROM VALUES (CAST(NULL AS STRING), 'invalid'), ('abc', '0') AS t(s, i)
        """
      Then query result
        | result |
        | NULL   |
        | 1      |

    Scenario: A NULL index short-circuits an invalid pattern
      When query
        """
        SELECT regexp_instr('abc', '[', CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL search arguments short-circuit index expressions for each row
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT regexp_instr(s, p, CAST(10 / i AS INT)) AS result
        FROM VALUES (CAST(NULL AS STRING), 'a', 0),
                    ('abc', 'a', 1),
                    ('abc', CAST(NULL AS STRING), 0),
                    ('abc', 'a', 2) AS t(s, p, i)
        """
      Then query result
        | result |
        | NULL   |
        | 1      |
        | NULL   |
        | 1      |

    Scenario Outline: Invalid arguments: <args>
      When query
        """
        SELECT regexp_instr(<args>) AS result
        """
      Then query error .*

      Examples:
        | args                   |
        | 'abc'                  |
        | 'abc', 'a', 1, 2        |
        | 'abc', 'a', true        |
        | 'abc', 'a', array(1)    |
        | array('abc'), 'a'       |

    Scenario: The third argument propagates NULL for each row
      When query
        """
        SELECT regexp_instr('ab12cd34', '[0-9]+', i) AS result
        FROM VALUES (2), (-1), (CAST(NULL AS INT)) AS t(i)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
      Then query result
        | result |
        | 3      |
        | 3      |
        | NULL   |

  @function(nullability)
  Rule: Output schema

    Scenario: ANSI string index conversion retains nullable metadata
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT regexp_instr('abc', 'a', CAST(id AS STRING)) AS result FROM range(1)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
      Then query result
        | result |
        | 1      |

    Scenario: a non-null literal input to regexp_instr yields the schema Spark declares
      When query
        """
        SELECT regexp_instr(r"\abc", r"^\\abc$") AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a non-null column input to regexp_instr yields the schema Spark declares
      When query
        """
        SELECT regexp_instr(CAST(id AS STRING), r"^\\abc$") AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to regexp_instr stays nullable
      When query
        """
        SELECT regexp_instr(c, r"^\\abc$") AS result FROM VALUES (r"\abc"), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Basic position

    Scenario: regexp_instr returns the start of the first match
      When query
        """
        SELECT regexp_instr('1a 2b 14m', '\\d+(a|b|m)') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: regexp_instr reports the position of the first of several matches
      When query
        """
        SELECT regexp_instr('zzz 9q', '\\d+(q)') AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: regexp_instr returns 0 when there is no match
      When query
        """
        SELECT regexp_instr('abc', '\\d+') AS result
        """
      Then query result
        | result |
        | 0      |

  Rule: The idx argument does not affect the returned position

    # idx is the "matched group id" but Spark always returns the start of the
    # whole match, even when the requested group starts elsewhere or does not
    # exist. These cases are exactly where the naive mapping to DataFusion's
    # regexp_instr (3rd arg = search start) diverges.

    Scenario: idx 1 yields the whole-match start
      When query
        """
        SELECT regexp_instr('1a 2b 14m', '\\d+(a|b|m)', 1) AS result
        """
      Then query result
        | result |
        | 1      |

    @sail-bug
    Scenario: idx 2 yields the whole-match start, not the group-2 position
      When query
        """
        SELECT regexp_instr('1a 2b 14m', '\\d+(a|b|m)', 2) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: idx is ignored even when the group starts at a different position
      When query
        """
        SELECT regexp_instr('xx1a', '(\\d+)(a)', 2) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: idx is ignored when the match is not at the start of the string
      When query
        """
        SELECT regexp_instr('zzz 9q', '\\d+(q)', 2) AS result
        """
      Then query result
        | result |
        | 5      |

    @sail-bug
    Scenario: out-of-range idx does not error and returns the whole-match start
      When query
        """
        SELECT regexp_instr('1a', '(\\d)(a)', 5) AS result
        """
      Then query result
        | result |
        | 1      |

  Rule: Pattern from a column

    Scenario: regexp_instr with the pattern taken from a column
      When query
        """
        SELECT regexp_instr(str, regexp) AS result
        FROM VALUES ('1a 2b 14m', '\\d+(a|b|m)') AS t(str, regexp)
        """
      Then query result
        | result |
        | 1      |

  Rule: idx is cast to INT, so its NULL-ness and type still affect the result

    # Although idx's value never changes the reported position, Spark implicitly
    # casts it to INT and evaluates it: a NULL idx makes the whole result NULL
    # (it is not silently dropped), and a non-integer numeric idx is coerced and
    # ignored. All expected values verified against the Spark JVM.

    Scenario: a NULL idx makes the result NULL
      When query
        """
        SELECT regexp_instr('1a 2b 14m', '\\d+(a|b|m)', NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: a typed NULL idx makes the result NULL
      When query
        """
        SELECT regexp_instr('1a 2b 14m', '\\d+(a|b|m)', CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: a NULL idx makes the result NULL even when there is no match
      When query
        """
        SELECT regexp_instr('abc', '\\d+', NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: a non-integer numeric idx is coerced and ignored
      When query
        """
        SELECT regexp_instr('1a 2b 14m', '\\d+(a|b|m)', 1.5) AS result
        """
      Then query result
        | result |
        | 1      |

  Rule: A non-numeric string idx follows ANSI cast semantics

    # Casting a non-numeric string to INT errors under ANSI and yields NULL
    # otherwise, so idx 'abc' raises under ANSI=true and makes the result NULL
    # under ANSI=false (verified against the Spark JVM).

    Scenario: a non-numeric string idx errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT regexp_instr('1a 2b 14m', '\\d+(a|b|m)', 'abc') AS result
        """
      Then query error .*

    @sail-bug
    Scenario: a non-numeric string idx is NULL under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT regexp_instr('1a 2b 14m', '\\d+(a|b|m)', 'abc') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Return type

    Scenario: the position is returned as INT, matching Spark
      When query
        """
        SELECT regexp_instr('abcabc', 'b') AS result
        """
      Then query result
        | result |
        | 2      |
