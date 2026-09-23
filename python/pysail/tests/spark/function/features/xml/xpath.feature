Feature: xpath() extracts XML nodes with Spark-compatible semantics

  Rule: Node selection returns arrays of string-like values

    Scenario: xpath returns text nodes in document order
      When query
        """
        SELECT xpath('<a><b>b1</b><b>b2</b><b>b3</b></a>', 'a/b/text()') AS result
        """
      Then query result
        | result       |
        | [b1, b2, b3] |

    Scenario: xpath returns NULL entries for element nodes
      When query
        """
        SELECT xpath('<a><b>b1</b><b>b2</b></a>', 'a/b') AS result
        """
      Then query result
        | result       |
        | [NULL, NULL] |

    Scenario: xpath returns an empty list when no nodes match
      When query
        """
        SELECT xpath('<a><b>1</b></a>', 'a/c') AS result
        """
      Then query result
        | result |
        | []     |

  Rule: Empty or null inputs return NULL

    Scenario: xpath returns NULL for empty xml
      When query
        """
        SELECT xpath('', 'a/b') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: xpath returns NULL for empty path
      When query
        """
        SELECT xpath('<a><b>1</b></a>', '') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: xpath returns NULL for null xml or path
      When query
        """
        SELECT
          xpath(CAST(NULL AS STRING), 'a/b') AS null_xml,
          xpath('<a><b>1</b></a>', CAST(NULL AS STRING)) AS null_path
        """
      Then query result
        | null_xml | null_path |
        | NULL     | NULL      |

  Rule: Non-node XPath results fail

    Scenario: xpath rejects expressions that do not return a node list
      When query
        """
        SELECT xpath('<a><b>1</b></a>', 'sum(a/b)') AS result
        """
      Then query error (?s).*NodeList.*

  Rule: xpath — the argument must be foldable

    @function(columnargs)
    Scenario: xpath with the argument as a literal
      When query
        """
        SELECT xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>','a/b') AS result
        """
      Then query result ordered
        | result             |
        | [NULL, NULL, NULL] |

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ["['b1', 'b2', 'b3']", '[None, None, None]'].
    @function(columnargs) @sail-bug
    Scenario: xpath takes argument 2 from a column holding two different values
      When query
        """
        SELECT xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', c) AS result FROM VALUES (1, 'a/b/text()'), (2, 'a/b') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['[None, None, None]', 'NULL'].
    @function(columnargs) @sail-bug
    Scenario: xpath takes argument 2 from a column containing NULL
      When query
        """
        SELECT xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', c) AS result FROM VALUES (1, 'a/b'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['[None, None, None]', '[None, None, None]'].
    @function(columnargs) @sail-bug
    Scenario: xpath takes argument 2 from a column
      When query
        """
        SELECT xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', c) AS result FROM VALUES (1, 'a/b'), (2, 'a/b') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null xml literal yields an array
      When query
        """
        SELECT xpath('<a><b>1</b><b>2</b></a>', 'a/b/text()') AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: string (containsNull = true)
        """

    Scenario: a non-null xml column yields an array
      When query
        """
        SELECT xpath(CONCAT('<a><b>', CAST(id AS STRING), '</b></a>'), 'a/b/text()') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: string (containsNull = true)
        """

    Scenario: a nullable xml column stays nullable
      When query
        """
        SELECT xpath(c, 'a/b/text()') AS result FROM VALUES ('<a><b>1</b></a>'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: string (containsNull = true)
        """

  Rule: xpath — a foldable path is accepted, a non-foldable one is not

    # Spark requires the path to be FOLDABLE, which is not the same as literal: any expression
    # without column references and without non-determinism qualifies.

    Scenario: xpath accepts a path built by a constant expression
      When query
        """
        SELECT xpath_string('<a><b>x</b></a>', substring('zza/b', 3)) AS result
        """
      Then query result
        | result |
        | x      |

    @sail-bug
    Scenario: xpath rejects a non-deterministic path
      When query
        """
        SELECT xpath_string('<a><b>x</b></a>', CASE WHEN rand() > 2 THEN 'a/b' ELSE 'a/b' END) AS result
        """
      Then query error (?s).*(NON_FOLDABLE|non-foldable).*

    # A correlated outer reference is not a column of this relation, so it is not caught by the
    # column-reference check, but Spark still rejects it as non-foldable. Confirmed on Spark JVM.
    @sail-bug
    Scenario: xpath rejects a correlated outer reference as the path
      When query
        """
        SELECT (SELECT xpath_string('<a><b>x</b></a>', t.c)) AS result FROM VALUES ('a/b') AS t(c)
        """
      Then query error (?s).*(NON_FOLDABLE|non-foldable).*

    # A scalar subquery is deterministic and has no column reference, yet Spark rejects it as
    # non-foldable. Confirmed on Spark JVM.
    @sail-bug
    Scenario: xpath rejects a scalar subquery as the path
      When query
        """
        SELECT xpath_string('<a><b>x</b></a>', (SELECT 'a/b')) AS result
        """
      Then query error (?s).*(NON_FOLDABLE|non-foldable).*

    # Sail returns an empty array.
    @sail-bug
    Scenario: xpath matches through a default namespace
      When query
        """
        SELECT xpath('<a xmlns="http://x"><b>v</b></a>', 'a/b/text()') AS result
        """
      Then query result
        | result |
        | [v]    |

  Rule: a comma sequence in the path is rejected

    # Spark evaluates the path with XPath 1.0, which has no sequence constructor, so a comma is a
    # syntax error. Sail evaluates it as an XPath 2.0 sequence and returns one entry per item.
    # Deferred with the typed variants: rejecting a top-level comma means parsing the XPath, since a
    # comma inside a function call is legitimate. Confirmed on Spark JVM.

    # Sail returns [NULL, NULL].
    @sail-bug
    Scenario: xpath of a comma sequence is an error
      When query
        """
        SELECT xpath('<a><b>10</b><c>20</c></a>', 'a/b, a/c') AS result
        """
      Then query error (?s).*Invalid XPath.*
