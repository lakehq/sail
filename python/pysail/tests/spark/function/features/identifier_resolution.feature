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
      Then query error \[UNRESOLVED_COLUMN\.WITH_SUGGESTION\] A column, variable, or function parameter with name `ID`\.`a` cannot be resolved\. Did you mean one of the following\? \[`ıd`\.`a`\]\.

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
      Then query error \[UNRESOLVED_COLUMN\.WITHOUT_SUGGESTION\] A column, variable, or function parameter with name `ID` cannot be resolved\.

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
      Then query error \[UNRESOLVED_COLUMN\.WITHOUT_SUGGESTION\] A column, variable, or function parameter with name `𐖗` cannot be resolved\.

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

  Rule: An unresolved name is reported the way the analyzer reports it

    Scenario: the suggestion is ordered by similarity and truncated to five names
      # `zzzzzz` is the first column of the schema but the least similar name, so it is the one
      # that the truncation drops.
      When query
        """
        SELECT nope FROM (SELECT 1 AS zzzzzz, 2 AS nope1, 3 AS c, 4 AS d, 5 AS e, 6 AS f)
        """
      Then query error \[UNRESOLVED_COLUMN\.WITH_SUGGESTION\] A column, variable, or function parameter with name `nope` cannot be resolved\. Did you mean one of the following\? \[`nope1`, `c`, `d`, `e`, `f`\]\.

    Scenario: the suggestion for an unqualified name carries no qualifier
      When query
        """
        SELECT nope FROM (SELECT 1 AS a) AS t
        """
      Then query error Did you mean one of the following\? \[`a`\]\.

    Scenario: a backtick in a suggested name is doubled
      When query
        """
        SELECT nope FROM (SELECT 1 AS `a``b`)
        """
      Then query error Did you mean one of the following\? \[`a``b`\]\.

    Scenario: an ambiguous reference lists the requested name once per candidate
      # The candidates are `a` and `A`, but the analyzer renames each match to the name that was
      # requested before it builds the message, so both entries read `a`.
      When query
        """
        SELECT a FROM (SELECT 1 AS a, 2 AS A)
        """
      Then query error \[AMBIGUOUS_REFERENCE\] Reference `a` is ambiguous, could be: \[`a`, `a`\]\.

    Scenario: an ambiguous reference quotes each part of the qualifier
      When query
        """
        SELECT id FROM (SELECT 1 AS ID) l JOIN (SELECT 2 AS ID) r
        """
      Then query error \[AMBIGUOUS_REFERENCE\] Reference `id` is ambiguous, could be: \[`l`\.`id`, `r`\.`id`\]\.

    Scenario: a wildcard whose target does not resolve reports the star expansion condition
      When query
        """
        SELECT nope.* FROM (SELECT 1 AS a)
        """
      Then query error \[CANNOT_RESOLVE_STAR_EXPAND\] Cannot resolve `nope`\.\* given input columns `a`\. Please check that the specified table or struct exists and is accessible in the input columns\.

  Rule: An unresolved join key is reported the way the analyzer reports it

    Scenario: the left-side columns are sorted before they are quoted
      # Sorting the quoted names would put `a b` first, since a space sorts below a backtick.
      When query
        """
        SELECT * FROM (SELECT 1 AS a, 2 AS `a b`) t1 JOIN (SELECT 1 AS z) t2 USING (nope)
        """
      Then query error \[UNRESOLVED_USING_COLUMN_FOR_JOIN\] USING column `nope` cannot be resolved on the left side of the join\. The left-side columns: \[`a`, `a b`\]\.

    Scenario: a dotted join key is reported as several quoted parts
      When query
        """
        SELECT * FROM (SELECT 1 AS a) t1 JOIN (SELECT 1 AS a) t2 USING (`x.y`)
        """
      Then query error USING column `x`\.`y` cannot be resolved on the left side of the join\.

  Rule: The names suggested for an unresolved column are ordered the way the analyzer orders them

    Scenario: two names at the same distance are ordered by name, not by position in the schema
      # The candidates reach the ordering through `AttributeSet.toSeq`, which sorts them by name,
      # and the sort by distance is stable, so an order that the schema imposes never survives.
      When query
        """
        SELECT xx FROM (SELECT 1 AS mm, 2 AS aa, 3 AS zz)
        """
      Then query error Did you mean one of the following\? \[`aa`, `mm`, `zz`\]\.

    Scenario: the order of the schema does not reach the suggestion
      When query
        """
        SELECT xx FROM (SELECT 1 AS zz, 2 AS yy, 3 AS ww)
        """
      Then query error Did you mean one of the following\? \[`ww`, `yy`, `zz`\]\.

    Scenario: the nearest name comes first even when it is last in the schema
      When query
        """
        SELECT nope FROM (SELECT 1 AS aaaaaa, 2 AS bbbbbb, 3 AS nope1)
        """
      Then query error Did you mean one of the following\? \[`nope1`, `aaaaaa`, `bbbbbb`\]\.

    Scenario: the distance is measured over characters rather than bytes
      When query
        """
        SELECT nope FROM (SELECT 1 AS `ñññññññ`, 2 AS `nopé`)
        """
      Then query error Did you mean one of the following\? \[`nopé`, `ñññññññ`\]\.

    Scenario: a qualifier shared by every candidate is stripped
      When query
        """
        SELECT nope FROM (SELECT 1 AS a, 2 AS b) AS t
        """
      Then query error Did you mean one of the following\? \[`a`, `b`\]\.

    Scenario: a qualifier is kept when the candidates do not share one
      When query
        """
        SELECT nope FROM (SELECT 1 AS a) AS t1 JOIN (SELECT 2 AS b) AS t2
        """
      Then query error Did you mean one of the following\? \[`t1`\.`a`, `t2`\.`b`\]\.

    Scenario: a qualifier is kept when the name that failed carries one
      When query
        """
        SELECT t.nope FROM (SELECT 1 AS a, 2 AS b) AS t
        """
      Then query error with name `t`\.`nope` cannot be resolved\. Did you mean one of the following\? \[`t`\.`a`, `t`\.`b`\]\.

    Scenario: the suggestion is truncated to five names after it is ordered
      When query
        """
        SELECT nope FROM (SELECT 1 AS q, 2 AS r, 3 AS nope1, 4 AS s, 5 AS t, 6 AS u)
        """
      Then query error Did you mean one of the following\? \[`nope1`, `q`, `r`, `s`, `t`\]\.

  Rule: A qualified interpretation of a name wins over a nested one

    @sail-bug
    Scenario: a qualifier is preferred over a struct of the same name
      # The analyzer tries the interpretations from the longest qualifier down and stops at the
      # first one that matches anything, so the struct field is never considered.
      When query
        """
        SELECT a.b FROM (SELECT named_struct('b', 1) AS a, 2 AS b) a
        """
      Then query result
        | b |
        | 2 |

  Rule: A nested field that matches nothing is reported as a missing field

    @sail-bug
    Scenario: a struct field that matches nothing is not an unresolved column
      # Once one attribute has matched, the remaining parts walk into it, and a part that names
      # no field is a missing field rather than a name that did not resolve.
      When query
        """
        SELECT s.missing FROM (SELECT named_struct('x', 1) AS s)
        """
      Then query error \[FIELD_NOT_FOUND\] No such struct field `missing` in `x`\.

  Rule: The columns listed by a failed wildcard are ordered the way the analyzer orders them

    Scenario: the input columns of a star expansion are sorted by name
      When query
        """
        SELECT nope.* FROM (SELECT 1 AS zz, 2 AS aa, 3 AS mm)
        """
      Then query error given input columns `aa`, `mm`, `zz`\.
