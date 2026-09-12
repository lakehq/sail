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
