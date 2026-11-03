Feature: regular expression predicates

  # Spark 4.2 RegexpExpressionsSuite: RLIKE Regular Expression.
  # Keep patterns within the shared Java/Rust regex subset.
  Rule: Regex predicates search without SQL wildcard translation

    Scenario Outline: regex aliases and operators match <case>
      When query
        """
        SELECT regexp(<value>, <pattern>) AS regexp_result,
               regexp_like(<value>, <pattern>) AS regexp_like_result,
               rlike(<value>, <pattern>) AS rlike_result,
               <value> RLIKE <pattern> AS rlike_operator,
               <value> REGEXP <pattern> AS regexp_operator
        """
      Then query result
        | regexp_result | regexp_like_result | rlike_result | rlike_operator | regexp_operator |
        | <result>      | <result>           | <result>     | <result>       | <result>        |

      Examples:
        | case                 | value       | pattern       | result |
        | substring            | 'abc'       | 'b'           | true   |
        | dot                  | 'abc'       | 'a.c'         | true   |
        | no match             | 'abc'       | 'z'           | false  |
        | case sensitive       | 'abc'       | 'B'           | false  |
        | start anchor         | 'abc'       | '^ab'         | true   |
        | wrong start          | 'abc'       | '^bc'         | false  |
        | end anchor           | 'abc'       | 'bc$'         | true   |
        | wrong end            | 'abc'       | 'ab$'         | false  |
        | full match           | 'abc'       | '^a.c$'       | true   |
        | percent literal      | 'a%b'       | '%'           | true   |
        | percent not wildcard | 'abc'       | '%'           | false  |
        | underscore literal   | 'a_b'       | '_'           | true   |
        | underscore not dot   | 'abc'       | 'a_c'         | false  |
        | escaped dot          | 'a.c'       | r'a\.c'       | true   |
        | escaped dot mismatch | 'abc'       | r'a\.c'       | false  |
        | backslash            | r'a\b'      | r'a\\\\b'     | true   |
        | digit class          | 'a1b'       | r'\d'         | true   |
        | empty pattern        | 'abc'       | ''            | true   |
        | empty input          | ''          | 'b'           | false  |
        | both empty           | ''          | ''            | true   |
        | null input           | NULL        | 'b'           | NULL   |
        | null pattern         | 'abc'       | NULL          | NULL   |
        | both null            | NULL        | NULL          | NULL   |

    Scenario: regex aliases match column values with a literal pattern
      When query
        """
        SELECT id, regexp(s, 'b') AS regexp_result,
               regexp_like(s, 'b') AS regexp_like_result,
               rlike(s, 'b') AS rlike_result
        FROM (
          SELECT id, CASE WHEN id = 2 THEN CAST(NULL AS STRING)
                          ELSE concat('abc', CAST(id AS STRING)) END AS s
          FROM range(3)
        )
        ORDER BY id
        """
      Then query result ordered
        | id | regexp_result | regexp_like_result | rlike_result |
        | 0  | true          | true               | true         |
        | 1  | true          | true               | true         |
        | 2  | NULL          | NULL               | NULL         |

    Scenario: regex aliases match dynamic patterns and propagate nulls
      When query
        """
        SELECT id, regexp(s, p) AS regexp_result,
               regexp_like(s, p) AS regexp_like_result,
               rlike(s, p) AS rlike_result,
               s RLIKE p AS rlike_operator, s REGEXP p AS regexp_operator
        FROM (
          SELECT id, CASE WHEN id >= 4 THEN CAST(NULL AS STRING)
                          ELSE concat('abc', CAST(id AS STRING)) END AS s,
                 CASE id WHEN 0 THEN 'b' WHEN 1 THEN 'a.c'
                         WHEN 2 THEN '^bc' WHEN 4 THEN 'b'
                         ELSE CAST(NULL AS STRING) END AS p
          FROM range(6)
        )
        ORDER BY id
        """
      Then query result ordered
        | id | regexp_result | regexp_like_result | rlike_result | rlike_operator | regexp_operator |
        | 0  | true          | true               | true         | true           | true            |
        | 1  | true          | true               | true         | true           | true            |
        | 2  | false         | false              | false        | false          | false           |
        | 3  | NULL          | NULL               | NULL         | NULL           | NULL            |
        | 4  | NULL          | NULL               | NULL         | NULL           | NULL            |
        | 5  | NULL          | NULL               | NULL         | NULL           | NULL            |

    Scenario: regex aliases match a literal value with column patterns
      When query
        """
        SELECT id, regexp('abc', p) AS regexp_result,
               regexp_like('abc', p) AS regexp_like_result,
               rlike('abc', p) AS rlike_result
        FROM (
          SELECT id, CASE id WHEN 0 THEN 'b' WHEN 1 THEN '^bc'
                             ELSE CAST(NULL AS STRING) END AS p
          FROM range(3)
        )
        ORDER BY id
        """
      Then query result ordered
        | id | regexp_result | regexp_like_result | rlike_result |
        | 0  | true          | true               | true         |
        | 1  | false         | false              | false        |
        | 2  | NULL          | NULL               | NULL         |

    Scenario: LIKE and ILIKE retain SQL wildcard matching
      When query
        """
        SELECT 'abc' LIKE '%b%' AS like_percent,
               'abc' LIKE 'a_c' AS like_underscore,
               'abc' LIKE 'b' AS like_anchored,
               'abc' LIKE 'a.c' AS like_dot,
               'AbC' ILIKE '%b%' AS ilike_percent,
               'AbC' ILIKE 'a_c' AS ilike_underscore,
               'AbC' ILIKE 'b' AS ilike_anchored,
               'AbC' ILIKE 'a.c' AS ilike_dot
        """
      Then query result
        | like_percent | like_underscore | like_anchored | like_dot | ilike_percent | ilike_underscore | ilike_anchored | ilike_dot |
        | true         | true            | false         | false    | true          | true             | false          | false     |

  Rule: Invalid regex patterns are rejected

    Scenario Outline: regex predicates reject an invalid literal pattern in <expression>
      When query
        """
        SELECT <expression> AS result
        FROM (SELECT concat('abc', CAST(id AS STRING)) AS s FROM range(2))
        """
      Then query error (?i)(INVALID_PARAMETER_VALUE.PATTERN|PatternSyntaxException|regular expression did not compile)

      Examples:
        | expression         |
        | regexp(s, '[')     |
        | regexp_like(s, '[') |
        | rlike(s, '[')       |
        | s RLIKE '['        |
        | s REGEXP '['       |

    Scenario Outline: regex predicates reject an invalid column pattern in <expression>
      When query
        """
        SELECT <expression> AS result
        FROM (
          SELECT concat('abc', CAST(id AS STRING)) AS s,
                 CASE WHEN id = 0 THEN 'b' ELSE '[' END AS p
          FROM range(2)
        )
        """
      Then query error (?i)(INVALID_PARAMETER_VALUE.PATTERN|PatternSyntaxException|regular expression did not compile)

      Examples:
        | expression        |
        | regexp(s, p)      |
        | regexp_like(s, p) |
        | rlike(s, p)       |
        | s RLIKE p        |
        | s REGEXP p       |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to rlike yields the schema Spark declares
      When query
        """
        SELECT rlike('%SystemDrive%\Users\John', '%SystemDrive%\\Users.*') AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a non-null column input to rlike yields the schema Spark declares
      When query
        """
        SELECT rlike(CAST(id AS STRING), '%SystemDrive%\\Users.*') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a nullable column input to rlike stays nullable
      When query
        """
        SELECT rlike(c, '%SystemDrive%\\Users.*') AS result FROM VALUES ('%SystemDrive%\Users\John'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """
