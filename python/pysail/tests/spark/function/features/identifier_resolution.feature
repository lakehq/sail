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
