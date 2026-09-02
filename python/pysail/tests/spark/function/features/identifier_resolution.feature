Feature: identifier resolution beyond ASCII

  Rule: A qualified attribute reference is matched by the resolver and by the lowercased name

    Scenario Outline: qualified reference: <case>
      When query
        """
        SELECT <qualifier>.a FROM (SELECT 1 AS a) AS <alias>
        """
      Then query result
        | a |
        | 1 |

      Examples:
        | case     | qualifier | alias |
        | ASCII    | t         | T     |
        | umlaut   | `ä`       | `Ä`   |
        | Cherokee | `Ꭰ`       | `ꭰ`   |

    Scenario: a qualifier whose lowercase form differs does not resolve an attribute
      # `ı` uppercases to `I`, so the resolver accepts it, but the attribute is looked up in a
      # map keyed by the lowercased name, and `ID` lowercases to `id` instead of `ıd`.
      When query
        """
        SELECT `ID`.a FROM (SELECT 1 AS a) AS `ıd`
        """
      Then query error UNRESOLVED_COLUMN|is missing from the schema

  Rule: A wildcard target is matched by the resolver alone

    Scenario Outline: wildcard target: <case>
      When query
        """
        SELECT <qualifier>.* FROM (SELECT 1 AS a) AS <alias>
        """
      Then query result
        | a |
        | 1 |

      Examples:
        | case                          | qualifier | alias |
        | ASCII                         | t         | T     |
        | umlaut                        | `ä`       | `Ä`   |
        | Cherokee                      | `Ꭰ`       | `ꭰ`   |
        | lowercase forms differ        | `ID`      | `ıd`  |

  Rule: A lambda parameter is matched by the lowercased name

    @function(lambda)
    Scenario Outline: lambda parameter: <case>
      When query
        """
        SELECT transform(array(1, 2), <param> -> <reference> + 1) AS result
        """
      Then query result
        | result |
        | [2, 3] |

      Examples:
        | case     | param | reference |
        | ASCII    | x     | X         |
        | umlaut   | `Ä`   | `ä`       |
        | Cherokee | `Ꭰ`   | `ꭰ`       |

    @function(lambda)
    Scenario: a lambda parameter whose lowercase form differs is not referenceable
      # Spark canonicalizes lambda variable names by lowercasing them rather than using the
      # resolver, so `ID` does not reach the `ıd` parameter even though the resolver matches them.
      When query
        """
        SELECT transform(array(1, 2), `ıd` -> `ID` + 1) AS result
        """
      Then query error UNRESOLVED_COLUMN|is missing from the schema

  Rule: A struct field is matched by the resolver

    Scenario: a struct field whose name differs beyond ASCII is matched
      When query
        """
        SELECT s.`Ä` FROM (SELECT named_struct('ä', 1) AS s)
        """
      Then query result
        | Ä |
        | 1 |

    Scenario: a struct field is not matched when the analysis is case sensitive
      # A field that matches no name is not an error on its own, since the name may still resolve
      # through another candidate, so the error comes from the attribute that failed to resolve
      # rather than from the field lookup, and it does not name the field.
      Given config spark.sql.caseSensitive = true
      When query
        """
        SELECT s.X FROM (SELECT named_struct('x', 1) AS s)
        """
      Then query error FIELD_NOT_FOUND|is missing from the schema

    Scenario: a struct is expanded through a qualifier written in a different case
      # The wildcard target is matched by the resolver, but the expansion compares the qualifier
      # literally, so the qualifier that the user wrote has to be replaced with the one in the
      # schema, as it already is for a wildcard whose target is only a qualifier.
      When query
        """
        SELECT T.s.* FROM (SELECT named_struct('x', 1) AS s) AS t
        """
      Then query result
        | x |
        | 1 |

    Scenario: a struct field is matched through an array of structs
      When query
        """
        SELECT s.`Ä` FROM (SELECT array(named_struct('ä', 1)) AS s)
        """
      Then query result
        | Ä   |
        | [1] |

    Scenario: a struct field reference is ambiguous when two fields match
      When query
        """
        SELECT s.x FROM (SELECT named_struct('x', 1, 'X', 2) AS s)
        """
      Then query error AMBIGUOUS_REFERENCE_TO_FIELDS

    Scenario: expanding a struct does not reject the fields that differ only in case
      When query
        """
        SELECT s.* FROM (SELECT named_struct('x', 1, 'X', 2) AS s)
        """
      Then query result
        | x | X |
        | 1 | 2 |

  Rule: A name is folded with the case mappings that the JVM knows

    @function(lambda)
    Scenario: a lambda parameter is not reachable through a newer Unicode case pair
      When query
        """
        SELECT transform(array(1), `𐕰` -> `𐖗` + 1) AS result
        """
      Then query error UNRESOLVED_COLUMN|is missing from the schema

  Rule: A join key is matched by the resolver alone

    @sail-bug
    Scenario Outline: USING: <case>
      # The name is matched by the resolver, which folds `ı` to `I` and `ς` to `Σ`, and the output
      # column takes the name of the left side rather than the one written in the clause.
      When query
        """
        SELECT * FROM (SELECT 1 AS <left>) AS l JOIN (SELECT 1 AS <right>) AS r USING (<right>)
        """
      Then query schema
        """
        root
         |-- <name>: integer (nullable = false)
        """

      Examples:
        | case                   | left | right | name |
        | ASCII                  | id   | `ID`  | id   |
        | lowercase forms differ | `ıd` | `Id`  | ıd   |
        | Greek final sigma      | `ς`  | `Σ`   | ς    |

    # Spark folds the common names case insensitively only since 4.2; before that the join
    # produces a cross join, which is what Sail still does.
    @sail-bug
    @spark-4.2
    Scenario: a natural join matches the common names case insensitively
      When query
        """
        SELECT * FROM (SELECT 1 AS a, 2 AS b) AS l NATURAL JOIN (SELECT 1 AS A, 3 AS c) AS r
        """
      Then query result
        | a | b | c |
        | 1 | 2 | 3 |

    Scenario: a natural join without common names is a cross join
      When query
        """
        SELECT * FROM (SELECT 1 AS a) AS l NATURAL JOIN (SELECT 2 AS b) AS r
        """
      Then query result
        | a | b |
        | 1 | 2 |

    @sail-bug
    Scenario: a USING clause keeps one copy of the join key
      When query
        """
        SELECT * FROM (SELECT 1 AS a, 'x' AS b) AS l JOIN (SELECT 1 AS A, 'y' AS c) AS r USING (A)
        """
      Then query result
        | a | b | c |
        | 1 | x | y |

  Rule: A name is folded with the full case mappings, including the final sigma

    Scenario: a word-final sigma folds like the lowercased name
      # `String.toLowerCase` maps a word-final `Σ` to `ς` rather than `σ`, so the two names are
      # the same once folded. Folding character by character would keep them apart.
      When query
        """
        SELECT `ΑΣ` FROM (SELECT 1 AS `ας`)
        """
      Then query result
        | ΑΣ |
        | 1  |

    @function(lambda)
    Scenario: a lambda parameter is reached through a word-final sigma
      When query
        """
        SELECT transform(array(1), `ΑΣ` -> `ας` + 1) AS result
        """
      Then query result
        | result |
        | [2]    |

  Rule: A nested field is extracted from every complex type Spark supports

    @sail-bug
    Scenario: a map value is extracted by key
      When query
        """
        SELECT m.k FROM (SELECT map('k', 1) AS m)
        """
      Then query result
        | k |
        | 1 |

    @sail-bug
    Scenario: extracting from a NULL base yields NULL
      When query
        """
        SELECT s.x FROM (SELECT CAST(NULL AS VOID) AS s)
        """
      Then query result
        | x    |
        | NULL |

  Rule: An ambiguous nested field only fails the interpretation that reaches it

    Scenario: a qualified reference resolves even when another interpretation is ambiguous
      # Spark picks the qualifier split first and only then extracts the field, so the ambiguous
      # struct on the other relation never takes part.
      When query
        """
        SELECT a.x FROM (SELECT named_struct('x', 1, 'X', 2) AS a) t, (SELECT 10 AS x) a
        """
      Then query result
        | x  |
        | 10 |
