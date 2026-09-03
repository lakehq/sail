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
        | case                   | qualifier | alias |
        | ASCII                  | t         | T     |
        | umlaut                 | `ä`       | `Ä`   |
        | Cherokee               | `Ꭰ`       | `ꭰ`   |
        | lowercase forms differ | `ID`      | `ıd`  |

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

    # A field that matches no name is not an error on its own, since the name may still resolve
    # through another candidate, so Sail reports the attribute that failed to resolve rather than
    # the field lookup: it names the column instead of the field, and reports the other condition.
    @sail-bug
    Scenario: a struct field is not matched when the analysis is case sensitive
      Given config spark.sql.caseSensitive = true
      When query
        """
        SELECT s.X FROM (SELECT named_struct('x', 1) AS s)
        """
      Then query error FIELD_NOT_FOUND

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

  Rule: The alias of an aggregate is matched the way an attribute reference is

    # `HAVING` and `ORDER BY` look the alias up with the rule for an attribute reference, not with
    # the resolver alone: `ıd` and `Id` are equal under `equalsIgnoreCase` and Spark still rejects
    # them, so the name must also survive lowercasing. The pair is what tells the two rules apart.
    Scenario Outline: aggregate alias: <case>
      When query
        """
        SELECT a, count(*) AS <alias> FROM (SELECT 1 AS a) GROUP BY a HAVING <probe> > 0
        """
      Then query result
        | a | <name> |
        | 1 | 1      |

      Examples:
        | case           | alias | probe | name |
        | same case      | c     | c     | c    |
        | differing case | c     | C     | c    |

    Scenario: the alias is matched beyond ASCII
      When query
        """
        SELECT a, count(*) AS `ä` FROM (SELECT 1 AS a) GROUP BY a HAVING `Ä` > 0
        """
      Then query result
        | a | ä |
        | 1 | 1 |

    Scenario: the alias of a sort is matched the same way
      When query
        """
        SELECT a, count(*) AS c FROM (SELECT 1 AS a) GROUP BY a ORDER BY C
        """
      Then query result
        | a | c |
        | 1 | 1 |

    # The controls for the setting: matching the name exactly still works when the analysis is
    # case sensitive, and a name that differs beyond ASCII is rejected there rather than folded.
    Scenario: a case sensitive analysis still matches the alias written exactly
      Given config spark.sql.caseSensitive = true
      When query
        """
        SELECT a, count(*) AS c FROM (SELECT 1 AS a) GROUP BY a HAVING c > 0
        """
      Then query result
        | a | c |
        | 1 | 1 |

    Scenario: a case sensitive analysis does not match the alias beyond ASCII
      Given config spark.sql.caseSensitive = true
      When query
        """
        SELECT a, count(*) AS `ä` FROM (SELECT 1 AS a) GROUP BY a HAVING `Ä` > 0
        """
      Then query error UNRESOLVED_COLUMN\.WITH_SUGGESTION

    Scenario Outline: an alias that only the resolver would match is rejected: <case>
      When query
        """
        SELECT a, count(*) AS <alias> FROM (SELECT 1 AS a) GROUP BY a HAVING <probe> > 0
        """
      Then query error UNRESOLVED_COLUMN\.WITH_SUGGESTION

      Examples:
        | case              | alias | probe |
        | dotless i         | `ıd`  | `Id`  |
        | Greek final sigma | `ς`   | `Σ`   |

    Scenario: a case sensitive analysis does not match the alias
      Given config spark.sql.caseSensitive = true
      When query
        """
        SELECT a, count(*) AS c FROM (SELECT 1 AS a) GROUP BY a HAVING C
        """
      Then query error UNRESOLVED_COLUMN\.WITH_SUGGESTION

    Scenario: a case sensitive analysis does not match the alias of a sort
      Given config spark.sql.caseSensitive = true
      When query
        """
        SELECT a, count(*) AS c FROM (SELECT 1 AS a) GROUP BY a ORDER BY C
        """
      Then query error UNRESOLVED_COLUMN\.WITH_SUGGESTION
