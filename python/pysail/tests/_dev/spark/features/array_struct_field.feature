Feature: String literals on arrays of structs select a field of every element

  Rule: The field is selected by name

    Scenario Outline: A field is extracted with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT a['q'],
               a['q'] AS alias,
               size(a['q']),
               slice(a, 2, 1)['q'][0],
               slice(a, 2, 1).q[0],
               a["q"]
        FROM (SELECT array(named_struct('p', 'x', 'q', 1), named_struct('p', 'y', 'q', 2)) AS a)
        """
      Then query result collected
        | a.q    | alias  | size(a.q) | slice(a, 2, 1).q[0] | slice(a, 2, 1).q[0] | a.q    |
        | [1, 2] | [1, 2] | 2         | 2                   | 2                   | [1, 2] |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: A string that looks like an index is a field name with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT a['1'] FROM (SELECT array(named_struct('1', 10), named_struct('1', 20)) AS a)
        """
      Then query result collected
        | a.1      |
        | [10, 20] |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario: The result keeps null elements, null fields and null arrays
      When query
        """
        SELECT id, a['p']
        FROM VALUES
          (1, array(named_struct('p', 1), NULL, named_struct('p', CAST(NULL AS INT)))),
          (2, CAST(NULL AS ARRAY<STRUCT<p: INT>>)),
          (3, CAST(array() AS ARRAY<STRUCT<p: INT>>)),
          (4, array(CAST(NULL AS STRUCT<p: INT>))) AS t(id, a)
        ORDER BY id
        """
      Then query result collected ordered
        | id | a.p             |
        | 1  | [1, None, None] |
        | 2  | NULL            |
        | 3  | []              |
        | 4  | [None]          |

    Scenario: The result is an array of the field type
      When query
        """
        SELECT a['p'], a['q'] FROM (SELECT array(named_struct('p', 'x', 'q', 1)) AS a)
        """
      Then query column types
        | column | type          |
        | a.p    | array<string> |
        | a.q    | array<int>    |

  Rule: A missing field is an analysis error

    Scenario Outline: The field <field> does not exist with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT a['<field>'] FROM (SELECT array(named_struct('p', 1)) AS a)
        """
      Then query error (?i)(FIELD_NOT_FOUND|missing or ambiguous field)

      Examples:
        | field | ansi  |
        | zz    | true  |
        | zz    | false |
        | 0     | true  |
        | 0     | false |

    Scenario: The dot form on a computed array reports a missing field
      When query
        """
        SELECT slice(a, 1, 1).zz FROM (SELECT array(named_struct('p', 1)) AS a)
        """
      Then query error (?i)(FIELD_NOT_FOUND|missing or ambiguous field)

  Rule: Field names follow the case sensitivity setting

    Scenario: A field matches in another case when the setting is off
      Given config spark.sql.caseSensitive = false
      When query
        """
        SELECT a['P'], a['Pp'], a['pP']
        FROM (SELECT array(named_struct('p', 1, 'Pp', 2)) AS a)
        """
      Then query result collected
        | a.P | a.Pp | a.pP |
        | [1] | [2]  | [2]  |

    Scenario: A field does not match in another case when the setting is on
      Given config spark.sql.caseSensitive = true
      When query
        """
        SELECT a['P'] FROM (SELECT array(named_struct('p', 1)) AS a)
        """
      Then query error (?i)(FIELD_NOT_FOUND|missing or ambiguous field)

    Scenario: Fields differing only by case are ambiguous when the setting is off
      Given config spark.sql.caseSensitive = false
      When query
        """
        SELECT a['p'] FROM (SELECT array(named_struct('p', 1, 'P', 2)) AS a)
        """
      Then query error (?i)(AMBIGUOUS_REFERENCE_TO_FIELDS|missing or ambiguous field)

    Scenario: Fields differing only by case are distinct when the setting is on
      Given config spark.sql.caseSensitive = true
      When query
        """
        SELECT a['p'], a['P'] FROM (SELECT array(named_struct('p', 1, 'P', 2)) AS a)
        """
      Then query result collected
        | a.p | a.P |
        | [1] | [2] |

    Scenario Outline: Repeated field names are ambiguous with case sensitivity <case_sensitive>
      Given config spark.sql.caseSensitive = <case_sensitive>
      When query
        """
        SELECT a['p'] FROM (SELECT array(named_struct('p', 1, 'p', 2)) AS a)
        """
      Then query error (?i)(AMBIGUOUS_REFERENCE_TO_FIELDS|missing or ambiguous field)

      Examples:
        | case_sensitive |
        | true           |
        | false          |

  Rule: Nested arrays need an index at each array of arrays

    Scenario: Fields are reached through an index on the inner array
      When query
        """
        SELECT a['b'][0]['c'],
               a['b'][0].c,
               a[0]['b']['c'],
               a[0].b['c'],
               a['b'][0],
               s['w']['c'],
               aa[0]['c']
        FROM (
          SELECT CAST(array(named_struct('b', array(named_struct('c', 1), named_struct('c', 2))))
                      AS ARRAY<STRUCT<b: ARRAY<STRUCT<c: INT>>>>) AS a,
                 CAST(named_struct('w', array(named_struct('c', 3)))
                      AS STRUCT<w: ARRAY<STRUCT<c: INT>>>) AS s,
                 CAST(array(array(named_struct('c', 4))) AS ARRAY<ARRAY<STRUCT<c: INT>>>) AS aa
        )
        """
      Then query result collected
        | a.b[0].c | a.b[0].c | a[0].b.c | a[0].b.c | a.b[0]               | s.w.c | aa[0].c |
        | [1, 2]   | [1, 2]   | [1, 2]   | [1, 2]   | [Row(c=1), Row(c=2)] | [3]   | [4]     |

    Scenario: A field of an inner array is an array of arrays of structs
      When query
        """
        SELECT a['b']
        FROM (SELECT CAST(array(named_struct('b', array(named_struct('c', 1))))
                          AS ARRAY<STRUCT<b: ARRAY<STRUCT<c: INT>>>>) AS a)
        """
      Then query column types
        | column | type                        |
        | a.b    | array<array<struct<c:int>>> |

    Scenario Outline: A string literal on an array of arrays is rejected: <expression>
      When query
        """
        SELECT <expression>
        FROM (
          SELECT array(named_struct('b', array(named_struct('c', 1)))) AS a,
                 array(array(named_struct('c', 1))) AS aa
        )
        """
      Then query error (?i)(DATATYPE_MISMATCH|cannot cast string)

      Examples:
        | expression  |
        | a['b']['c'] |
        | a['b'].c    |
        | aa['c']     |

  Rule: Other extractions on an array of structs stay array indexes

    Scenario: Integer literals select an element
      When query
        """
        SELECT a[0], a[1]['q'], a[1].q, a[1L]['q']
        FROM (SELECT array(named_struct('q', 1), named_struct('q', 2)) AS a)
        """
      Then query result collected
        | a[0]     | a[1].q | a[1].q | a[1].q |
        | Row(q=1) | 2      | 2      | 2      |

    Scenario: A NULL literal selects nothing
      When query
        """
        SELECT a[NULL] FROM (SELECT array(named_struct('q', 1)) AS a)
        """
      Then query result collected
        | a[NULL] |
        | NULL    |

    Scenario: An index out of bounds raises an error with ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT a[5] FROM (SELECT array(named_struct('q', 1)) AS a)
        """
      Then query error (?i)INVALID_ARRAY_INDEX

    Scenario: An index out of bounds is NULL with ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT a[5] FROM (SELECT array(named_struct('q', 1)) AS a)
        """
      Then query result collected
        | a[5] |
        | NULL |

    Scenario: A string column named like a field does not select the field
      When query
        """
        SELECT a[s] FROM (SELECT array(named_struct('s', 1)) AS a, 's' AS s)
        """
      Then query error (?i)(DATATYPE_MISMATCH|cannot cast string)

  Rule: Sort and group expressions resolve fields like the projection

    Scenario: An input array resolves a field the output array does not have
      When query
        """
        SELECT array(named_struct('x', 1)) AS a
        FROM (SELECT array(named_struct('p', 2)) AS a)
        ORDER BY a['p']
        """
      Then query result collected
        | a          |
        | [Row(x=1)] |

    Scenario: An output array resolves its own field
      When query
        """
        SELECT array(named_struct('x', 1)) AS a
        FROM (SELECT array(named_struct('p', 2)) AS a)
        ORDER BY a['x']
        """
      Then query result collected
        | a          |
        | [Row(x=1)] |

    Scenario: HAVING resolves a field of a grouped input array
      When query
        """
        SELECT array(named_struct('x', 1)) AS a, count(1)
        FROM (SELECT array(named_struct('p', 2)) AS a)
        GROUP BY a
        HAVING size(a['p']) > 0
        """
      Then query result collected
        | a          | count(1) |
        | [Row(x=1)] | 1        |

    Scenario: An input array resolves a field that is ambiguous in the output array
      Given config spark.sql.caseSensitive = false
      When query
        """
        SELECT array(named_struct('p', 1, 'P', 2)) AS a
        FROM (SELECT array(named_struct('p', 3)) AS a)
        ORDER BY a['p']
        """
      Then query result collected
        | a               |
        | [Row(p=1, P=2)] |

    Scenario: A field missing from both the output and the input is an error
      When query
        """
        SELECT array(named_struct('x', 1)) AS a
        FROM (SELECT array(named_struct('p', 2)) AS a)
        ORDER BY a['zz']
        """
      Then query error (?i)(UNRESOLVED_COLUMN|FIELD_NOT_FOUND|missing or ambiguous field)

    Scenario: Grouping by a field uses the Spark column name
      When query
        """
        SELECT a['q'], count(1)
        FROM (SELECT array(named_struct('q', 1)) AS a)
        GROUP BY a['q']
        """
      Then query result collected
        | a.q | count(1) |
        | [1] | 1        |
