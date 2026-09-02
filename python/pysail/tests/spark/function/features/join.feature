Feature: NATURAL and USING joins

  Rule: The join key appears once and keeps the values of both sides

    Scenario Outline: <clause> <join type>
      When query
        """
        SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS l(k, lv)
        <clause> <join type> JOIN (VALUES (1, 'x'), (3, 'y')) AS r(k, rv) <using>
        """
      Then query result ordered
        | k    | lv   | rv   |
        | <k1> | <l1> | <v1> |
        | <k2> | <l2> | <v2> |

      Examples:
        | clause  | join type   | using     | k1 | l1 | v1 | k2 | l2   | v2   |
        | NATURAL | LEFT OUTER  |           | 1  | a  | x  | 2  | b    | NULL |
        | NATURAL | RIGHT OUTER |           | 1  | a  | x  | 3  | NULL | y    |
        |         | LEFT OUTER  | USING (k) | 1  | a  | x  | 2  | b    | NULL |
        |         | RIGHT OUTER | USING (k) | 1  | a  | x  | 3  | NULL | y    |

    Scenario Outline: every common name becomes a key: <case>
      When query
        """
        SELECT * FROM (SELECT 1 AS a, 2 AS b, 3 AS c) AS l
        <clause> JOIN (SELECT 1 AS a, 2 AS b, 4 AS d) AS r <using>
        """
      Then query result
        | a | b | c | d |
        | 1 | 2 | 3 | 4 |

      Examples:
        | case    | clause  | using        |
        | natural | NATURAL |              |
        | using   |         | USING (a, b) |

    Scenario: a natural join without common names is a cross join
      When query
        """
        SELECT * FROM (SELECT 1 AS a) AS l NATURAL JOIN (SELECT 2 AS b) AS r
        """
      Then query result
        | a | b |
        | 1 | 2 |

    Scenario: a NULL key does not match another NULL key
      When query
        """
        SELECT * FROM (VALUES (CAST(NULL AS INT), 'a'), (1, 'b')) AS l(k, lv)
        FULL OUTER JOIN (VALUES (CAST(NULL AS INT), 'x'), (1, 'y')) AS r(k, rv) USING (k)
        """
      Then query result
        | k    | lv   | rv   |
        | 1    | b    | y    |
        | NULL | a    | NULL |
        | NULL | NULL | x    |

  Rule: A join on a condition keeps the column of both sides

    Scenario: the shared name appears once per side
      When query
        """
        SELECT * FROM (VALUES ('Alice', 2), ('Bob', 5)) AS l(name, age)
        JOIN (VALUES ('Tom', 80), ('Bob', 85)) AS r(name, height) ON l.name = r.name
        """
      Then query result
        | name | age | name | height |
        | Bob  | 5   | Bob  | 85     |

  Rule: The common names of a natural join are matched by the resolver

    # Spark folds the common names only since 4.2; before that the join degenerates into a cross
    # join that keeps both columns, which is a different result rather than an error.
    @spark-4.2
    Scenario Outline: natural join: <case>
      When query
        """
        SELECT * FROM (SELECT 1 AS <left>) AS l NATURAL JOIN (SELECT 1 AS <right>, 3 AS c) AS r
        """
      Then query result
        | <name> | c |
        | 1      | 3 |

      Examples:
        | case   | left | right | name |
        | ASCII  | a    | `A`   | a    |
        | umlaut | `ä`  | `Ä`   | ä    |

  Rule: A USING key is matched by the resolver and named after the left side

    Scenario Outline: USING key: <case>
      When query
        """
        SELECT * FROM (SELECT 1 AS <left>, 'p' AS b) AS l
        JOIN (SELECT 1 AS <right>, 'q' AS c) AS r USING (<right>)
        """
      Then query result
        | <name> | b | c |
        | 1      | p | q |

      Examples:
        | case                   | left | right | name |
        | ASCII                  | a    | `A`   | a    |
        | umlaut                 | `ä`  | `Ä`   | ä    |
        | lowercase forms differ | `ıd` | `Id`  | ıd   |
        | Greek final sigma      | `ς`  | `Σ`   | ς    |

  Rule: The join key stays reachable through the qualifier of each side

    @sail-bug
    Scenario Outline: qualified key: <case>
      When query
        """
        SELECT l.k, r.k FROM (VALUES (1, 'a')) AS l(k, lv)
        <clause> JOIN (VALUES (1, 'x')) AS r(k, rv) <using>
        """
      Then query result
        | k | k |
        | 1 | 1 |

      Examples:
        | case    | clause  | using     |
        | using   |         | USING (k) |
        | natural | NATURAL |           |

  Rule: A natural join is not defined for a semi or anti join

    @sail-bug
    Scenario Outline: natural <join type> is rejected
      When query
        """
        SELECT * FROM (SELECT 1 AS k) AS l NATURAL <join type> JOIN (SELECT 1 AS k) AS r
        """
      Then query error Unsupported natural join type

      Examples:
        | join type |
        | LEFT SEMI |
        | LEFT ANTI |

  Rule: OUTER alone is not a natural join type

    # Spark parses the join type of a NATURAL join from a closed list that has no bare OUTER,
    # so this is rejected before analysis rather than treated as a full outer join.
    @sail-bug
    Scenario: a natural outer join is rejected by the parser
      When query
        """
        SELECT * FROM (SELECT 1 AS k) AS l NATURAL OUTER JOIN (SELECT 1 AS k) AS r
        """
      Then query error PARSE_SYNTAX_ERROR

  Rule: A USING key that matches no column is rejected

    Scenario: the error names the clause that could not be resolved
      When query
        """
        SELECT * FROM (SELECT 1 AS k) AS l JOIN (SELECT 1 AS k) AS r USING (nope)
        """
      Then query error UNRESOLVED_USING_COLUMN_FOR_JOIN
