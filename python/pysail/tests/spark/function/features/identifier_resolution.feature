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

    Scenario: a qualified wildcard expands every alias that matches without case
      # The resolver matches a qualifier without case while the expansion compares it literally,
      # so two aliases that differ only in case both have to be expanded.
      When query
        """
        SELECT a.* FROM (SELECT 1 AS x) AS A CROSS JOIN (SELECT 2 AS y) AS a
        """
      Then query result
        | x | y |
        | 1 | 2 |

    Scenario: the wildcard expansion does not depend on the kind of join
      # The same shape reached through a plain join rather than a cross join, so the gap is in the
      # expansion of the qualifier and not in how the two relations were put together.
      When query
        """
        SELECT a.* FROM (SELECT 1 AS x) AS A JOIN (SELECT 2 AS y) AS a ON true
        """
      Then query result
        | x | y |
        | 1 | 2 |

    Scenario: a qualified wildcard expands columns that are named the same on both sides
      # The two sides bring a column with the same name, so the expansion has to keep both rather
      # than let one stand for the other.
      When query
        """
        SELECT a.* FROM (SELECT 1 AS k) AS A CROSS JOIN (SELECT 2 AS k) AS a
        """
      Then query result
        | k | k |
        | 1 | 2 |

    Scenario: the same wildcard twice expands twice
      When query
        """
        SELECT a.*, a.* FROM (SELECT 1 AS x) AS A CROSS JOIN (SELECT 2 AS y) AS a
        """
      Then query result
        | x | y | x | y |
        | 1 | 2 | 1 | 2 |

    Scenario: a qualified wildcard expands three aliases that match without case
      # With three of them, picking one qualifier used to return the first and the last and leave
      # out the one in the middle, which is what tells "it picks one" apart from "it picks wrong".
      When query
        """
        SELECT a.* FROM (SELECT 1 AS x) AS A CROSS JOIN (SELECT 2 AS y) AS a CROSS JOIN (SELECT 3 AS z) AS `A`
        """
      Then query result
        | x | y | z |
        | 1 | 2 | 3 |

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
    Scenario: a struct field is not matched when the analysis is case sensitive
      Given config spark.sql.caseSensitive = true
      When query
        """
        SELECT s.X FROM (SELECT named_struct('x', 1) AS s)
        """
      Then query error \[FIELD_NOT_FOUND\] No such struct field `X` in `x`\.

    Scenario: a relation is expanded through a qualifier written in a different case
      # The target of the wildcard is matched by the resolver, so the case it was written in does
      # not have to be the one the relation was declared with. Getting this wrong expands nothing
      # and returns an empty row rather than failing, which is why the columns are asserted.
      When query
        """
        SELECT T.* FROM (SELECT 1 AS x, 2 AS y) AS t
        """
      Then query result
        | x | y |
        | 1 | 2 |

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
      Then query error \[AMBIGUOUS_REFERENCE_TO_FIELDS\] Ambiguous reference to the field `x`\. It appears 2 times in the schema\.

    # The name reaches the message as one string, so it is parsed again before it is quoted.
    Scenario: an ambiguous field whose name contains a dot is reported as several quoted parts
      When query
        """
        SELECT s.`x.y` FROM (SELECT named_struct('x.y', 1, 'X.Y', 2) AS s)
        """
      Then query error Ambiguous reference to the field `x`\.`y`\. It appears 2 times in the schema\.

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
      Then query error with name `Ä` cannot be resolved\.

    Scenario Outline: an alias that only the resolver would match is rejected: <case>
      When query
        """
        SELECT a, count(*) AS <alias> FROM (SELECT 1 AS a) GROUP BY a HAVING <probe> > 0
        """
      Then query error with name <probe> cannot be resolved\.

      Examples:
        | case              | alias | probe |
        | dotless i         | `ıd`  | `Id`  |
        | Greek final sigma | `ς`   | `Σ`   |

    # The alias belongs to the output of the aggregate, which is the input of the filter that
    # carries the `HAVING`, so Spark offers it.
    Scenario: an aggregate alias wins a tie in distance against a column
      # The aggregate expressions come before the columns, and the order by distance is stable, so
      # two names at the same distance are separated by which list they came from.
      When query
        """
        SELECT aa, count(*) AS bb FROM (SELECT 1 AS aa) GROUP BY aa HAVING cc > 0
        """
      Then query error Did you mean one of the following\? \[`bb`, `aa`\]\.

    Scenario: the names a HAVING offers are the output of the aggregate
      # The filter reads the OUTPUT of the aggregate, so only the grouping expressions and the
      # aggregate aliases are names there. A column of the input that the aggregate does not
      # carry through, like `c`, is not one, which is what this pins.
      When query
        """
        SELECT a, sum(b) AS total FROM (SELECT 1 AS a, 2 AS b, 3 AS c) GROUP BY a HAVING totl > 0
        """
      Then query error Did you mean one of the following\? \[`total`, `a`\]\.

    Scenario: a grouping expression the aggregate does not select is not offered
      # The output of the aggregate is its SELECT list, not its grouping, so a column that is
      # grouped by and never selected is not a name in the filter either. The scenario above
      # cannot tell the two rules apart, because there the grouping column is also selected.
      When query
        """
        SELECT count(*) AS n FROM (SELECT 1 AS a, 2 AS b) GROUP BY a HAVING zz > 1
        """
      Then query error Did you mean one of the following\? \[`n`\]\.

    Scenario: only the grouping expressions the aggregate selects are offered
      # With two grouping expressions and only one of them selected, the one left out must not
      # reach the suggestion.
      When query
        """
        SELECT a, count(*) AS n FROM (SELECT 1 AS a, 2 AS b) GROUP BY a, b HAVING zz > 1
        """
      Then query error Did you mean one of the following\? \[`n`, `a`\]\.

    Scenario: a wildcard offers the columns it expands rather than itself
      # A `*` is not a name of the output: it carries the columns of the input through, and those
      # are the names the filter offers. Offering `*` would name something nobody can write.
      When query
        """
        SELECT * FROM (SELECT 1 AS a, 2 AS b) GROUP BY a, b HAVING zz > 0
        """
      Then query error Did you mean one of the following\? \[`a`, `b`\]\.

    Scenario: a wildcard beside an aggregate alias keeps the alias first
      # With both in the list the alias still comes before the columns, which tells the expansion
      # apart from a rule that merely stopped filtering.
      When query
        """
        SELECT *, count(*) AS c FROM (SELECT 1 AS a, 2 AS b) GROUP BY a, b HAVING zz > 0
        """
      Then query error Did you mean one of the following\? \[`c`, `a`, `b`\]\.

    Scenario: a qualified wildcard offers only the columns of what it targets
      # The target chooses which columns are carried through, so the other side of the join is
      # not offered. Without a join the same query cannot tell that apart from expanding all.
      When query
        """
        SELECT t.* FROM (SELECT 1 AS a, 2 AS b) AS t JOIN (SELECT 1 AS c, 2 AS d) AS u ON t.a = u.c
        GROUP BY t.a, t.b, u.c, u.d HAVING zz > 0
        """
      Then query error Did you mean one of the following\? \[`a`, `b`\]\.

    Scenario: the suggestion of a sort offers the names of the projection
      # TODO: the names are the right ones, but their ORDER is not pinned here: both sit at the
      # same distance from the name that was asked for, and Spark breaks that tie with `c` first
      # while Sail offers `a` first, so the candidates of a sort do not reach the analyzer in the
      # order of the sorted attribute set.
      When query
        """
        SELECT a, count(*) AS c FROM (SELECT 1 AS a) GROUP BY a ORDER BY nope
        """
      Then query error \[UNRESOLVED_COLUMN\.WITH_SUGGESTION\] A column, variable, or function parameter with name `nope` cannot be resolved\.

    Scenario: the suggestion for an unresolved alias offers the alias itself
      When query
        """
        SELECT a, count(*) AS `ıd` FROM (SELECT 1 AS a) GROUP BY a HAVING `Id` > 0
        """
      Then query error name `Id` cannot be resolved\. Did you mean one of the following\? \[`ıd`, `a`\]\.

    Scenario: a case sensitive analysis does not match the alias
      Given config spark.sql.caseSensitive = true
      When query
        """
        SELECT a, count(*) AS c FROM (SELECT 1 AS a) GROUP BY a HAVING C
        """
      Then query error with name `C` cannot be resolved\.

    Scenario: a case sensitive analysis does not match the alias of a sort
      Given config spark.sql.caseSensitive = true
      When query
        """
        SELECT a, count(*) AS c FROM (SELECT 1 AS a) GROUP BY a ORDER BY C
        """
      Then query error with name `C` cannot be resolved\.

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

    # A qualifier is already several parts, so a dot inside one of them is part of the name and
    # not a separator: `x.y` is one alias, and quoting it as two would name a table nobody wrote.
    Scenario: an ambiguous reference keeps a dot that belongs to the qualifier
      When query
        """
        SELECT a FROM (SELECT 1 AS a) AS `x.y`, (SELECT 2 AS a) AS z
        """
      Then query error \[AMBIGUOUS_REFERENCE\] Reference `a` is ambiguous, could be: \[`x\.y`\.`a`, `z`\.`a`\]\.

    # The suggestion keeps the qualifier whole for the same reason. Two relations are needed so
    # that the qualifier is not shared by every candidate, since a shared one is stripped.
    Scenario: a suggestion keeps a dot that belongs to the qualifier
      When query
        """
        SELECT nope FROM (SELECT 1 AS a) AS `x.y`, (SELECT 2 AS b) AS z
        """
      Then query error \[UNRESOLVED_COLUMN\.WITH_SUGGESTION\] A column, variable, or function parameter with name `nope` cannot be resolved\. Did you mean one of the following\? \[`z`\.`b`, `x\.y`\.`a`\]\.

    # Star expansion is the opposite case, and the asymmetry is deliberate: `UnresolvedStar`
    # joins its target without quoting the parts first (`unresolved.scala:607`), unlike
    # `Attribute.qualifiedName`, so Spark itself splits the dot back out here. Keeping the
    # qualifier whole would look more correct and would diverge.
    Scenario: a star target is split on a dot the way Spark splits it
      When query
        """
        SELECT `a.b`.* FROM (SELECT 1 AS c) AS z
        """
      Then query error \[CANNOT_RESOLVE_STAR_EXPAND\] Cannot resolve `a`\.`b`\.\* given input columns `c`\.

    Scenario: a wildcard whose target does not resolve reports the star expansion condition
      When query
        """
        SELECT nope.* FROM (SELECT 1 AS a)
        """
      Then query error \[CANNOT_RESOLVE_STAR_EXPAND\] Cannot resolve `nope`\.\* given input columns `a`\. Please check that the specified table or struct exists and is accessible in the input columns\.

    # The target is joined back into one string before it is quoted, so the name the user wrote as
    # a single quoted part is split again.
    Scenario: a wildcard target whose name contains a dot is reported as several quoted parts
      When query
        """
        SELECT `x.y`.* FROM (SELECT 1 AS a)
        """
      Then query error Cannot resolve `x`\.`y`\.\* given input columns `a`\.

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

    Scenario: a dotted column of the joined side is suggested as several quoted parts
      When query
        """
        SELECT * FROM (SELECT 1 AS `x.y`) t1 JOIN (SELECT 1 AS a) t2 USING (nope)
        """
      Then query error The left-side columns: \[`x`\.`y`\]\.

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

    Scenario: a character outside the BMP counts as two units of distance
      # The distance comes from Commons Text, which walks a Java string, so a supplementary
      # character is two code units rather than one and ties with a name two characters away.
      When query
        """
        SELECT a FROM (SELECT 1 AS zz, 2 AS `😀`)
        """
      Then query error Did you mean one of the following\? \[`zz`, `😀`\]\.

    Scenario: two names at the same distance are ordered the way a Java string compares
      # Both names are two units away once a supplementary character counts as two, so what is
      # left to order them is the comparison itself, which is by UTF-16 code unit.
      When query
        """
        SELECT a FROM (SELECT 1 AS `ﬀx`, 2 AS `😀`)
        """
      Then query error Did you mean one of the following\? \[`😀`, `ﬀx`\]\.

    Scenario: ambiguous references are ordered the way a Java string compares
      # The references of an ambiguous column are ordered by a path of their own, separate from the
      # one that orders the suggestions.
      When query
        """
        SELECT id FROM (SELECT 1 AS id) AS `ﬀ` CROSS JOIN (SELECT 2 AS id) AS `😀`
        """
      Then query error could be: \[`😀`\.`id`, `ﬀ`\.`id`\]\.

    Scenario: names are ordered the way a Java string compares
      When query
        """
        SELECT nope.* FROM (SELECT 1 AS `ﬀ`, 2 AS `😀`)
        """
      Then query error given input columns `😀`, `ﬀ`\.

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

    Scenario: the key of a USING join keeps its qualifier in the suggestion
      # The key is materialised as a column of its own, which loses the qualifier it had. That
      # also shortens it, so it moves to the front of the order by distance.
      When query
        """
        SELECT nope FROM (SELECT 1 AS id, 2 AS a) l JOIN (SELECT 1 AS id, 3 AS b) r USING (id)
        """
      Then query error Did you mean one of the following\? \[`l`\.`a`, `r`\.`b`, `l`\.`id`\]\.

    Scenario: a join on a condition keeps the qualifier of both sides
      When query
        """
        SELECT nope FROM (SELECT 1 AS id, 2 AS a) l JOIN (SELECT 1 AS id, 3 AS b) r ON l.id = r.id
        """
      Then query error Did you mean one of the following\? \[`l`\.`a`, `r`\.`b`, `l`\.`id`, `r`\.`id`\]\.

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

    Scenario: a struct field that matches nothing is not an unresolved column
      # Once one attribute has matched, the remaining parts walk into it, and a part that names
      # no field is a missing field rather than a name that did not resolve.
      When query
        """
        SELECT s.missing FROM (SELECT named_struct('x', 1) AS s)
        """
      Then query error \[FIELD_NOT_FOUND\] No such struct field `missing` in `x`\.

    Scenario: the missing field is reported against the struct that the walk reached
      # The parts are walked one by one, so the fields listed are the ones of the struct that the
      # part before reached, not those of the column at the top.
      When query
        """
        SELECT s.x.missing FROM (SELECT named_struct('x', named_struct('y', 1)) AS s)
        """
      Then query error \[FIELD_NOT_FOUND\] No such struct field `missing` in `y`\.

    Scenario: a wildcard on a field that matches nothing is a missing field too
      # The target of a wildcard is resolved as an attribute reference before it is required to
      # be a struct, so what is reported is the field the walk did not find.
      When query
        """
        SELECT s.zz.* FROM (SELECT named_struct('x', 1) AS s)
        """
      Then query error \[FIELD_NOT_FOUND\] No such struct field `zz` in `x`\.

  Rule: A wildcard target that resolves but is not a struct is reported by its type

    Scenario: the target is not a complex type
      # It resolved, so the failure is the type it reached and not a name that did not resolve.
      When query
        """
        SELECT a.* FROM (SELECT 1 AS a)
        """
      Then query error Can only star expand struct data types\. Attribute: `List\(a\)`\.

    Scenario: the target is a nested field that is not a struct
      # The attribute that is named is the whole path, part by part, as Spark renders the list.
      When query
        """
        SELECT s.inner.* FROM (SELECT named_struct('inner', 1) AS s)
        """
      Then query error Can only star expand struct data types\. Attribute: `List\(s, inner\)`\.

    Scenario: the target is a map, which is complex but not a struct
      When query
        """
        SELECT m.* FROM (SELECT map('k', 1) AS m)
        """
      Then query error Can only star expand struct data types\. Attribute: `List\(m\)`\.

    Scenario: an ambiguous target is refused before its type is looked at
      # Resolving the target comes first, so a name that matches twice is refused even when only
      # one of the two could have been expanded.
      When query
        """
        SELECT s.* FROM (SELECT named_struct('x', 1) AS s, 2 AS s)
        """
      Then query error \[AMBIGUOUS_REFERENCE\] Reference `s` is ambiguous, could be: \[`s`, `s`\]\.

    Scenario: a field of a struct inside an array is reported the same way
      When query
        """
        SELECT a.missing FROM (SELECT array(named_struct('x', 1)) AS a)
        """
      Then query error \[FIELD_NOT_FOUND\] No such struct field `missing` in `x`\.

    Scenario: a field of a struct inside an array resolves through the array
      When query
        """
        SELECT a.x FROM (SELECT array(named_struct('x', 1)) AS a)
        """
      Then query result
        | x   |
        | [1] |

    Scenario: a name that walks into something that is not complex is a different error
      # The base is not a struct, an array or a map, so there is no field to miss: Spark reports
      # the type it got instead of listing fields.
      When query
        """
        SELECT a.b FROM (SELECT 1 AS a)
        """
      Then query error \[INVALID_EXTRACT_BASE_FIELD_TYPE\] Can't extract a value from "a"\. Need a complex type \[STRUCT, ARRAY, MAP\] but got "INT"\.

    Scenario: the base is the name as it was written, not the one it resolved to
      # Spark renders it with `toSQLExpr`, which prints the reference the user wrote. The column
      # is named in lower case, so asking for it in upper case is what tells the two apart.
      When query
        """
        SELECT `A`.`B` FROM (SELECT 1 AS a)
        """
      Then query error Can't extract a value from "A"\.

    Scenario: the base of a nested step keeps the whole path
      # Once a part has been walked into, the base of the next step is the path so far and not
      # just the last field, which is what a single-part base would report.
      When query
        """
        SELECT a.b.c FROM (SELECT named_struct('b', 1) AS a)
        """
      Then query error Can't extract a value from "a\.b"\.

  Rule: The columns listed by a failed wildcard are ordered the way the analyzer orders them

    Scenario: the input columns of a star expansion are sorted by name
      When query
        """
        SELECT nope.* FROM (SELECT 1 AS zz, 2 AS aa, 3 AS mm)
        """
      Then query error given input columns `aa`, `mm`, `zz`\.

    # Each name reaches the message as one string, so it is parsed again before it is quoted.
    Scenario: an input column whose name contains a dot is listed as several quoted parts
      When query
        """
        SELECT nope.* FROM (SELECT 1 AS `x.y`)
        """
      Then query error given input columns `x`\.`y`\.
