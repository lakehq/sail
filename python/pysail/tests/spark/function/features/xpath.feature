Feature: xpath() extracts XML nodes with Spark-compatible semantics

  Rule: Node selection returns arrays of string-like values

    Scenario: xpath returns text nodes in document order
      When query
        """
        SELECT xpath('<a><b>b1</b><b>b2</b><b>b3</b></a>', 'a/b/text()') AS result
        """
      Then query result
        | result        |
        | [b1, b2, b3]  |

    Scenario: xpath returns NULL entries for element nodes
      When query
        """
        SELECT xpath('<a><b>b1</b><b>b2</b></a>', 'a/b') AS result
        """
      Then query result
        | result        |
        | [NULL, NULL]  |

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

    @column_args
    Scenario: xpath with the argument as a literal
      When query
        """
        SELECT xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>','a/b') AS result
        """
      Then query result ordered
        | result             |
        | [NULL, NULL, NULL] |

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ["['b1', 'b2', 'b3']", '[None, None, None]'].
    @column_args @sail-bug
    Scenario: xpath takes argument 2 from a column holding two different values
      When query
        """
        SELECT xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', c) AS result FROM VALUES (1, 'a/b/text()'), (2, 'a/b') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['[None, None, None]', 'NULL'].
    @column_args @sail-bug
    Scenario: xpath takes argument 2 from a column containing NULL
      When query
        """
        SELECT xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', c) AS result FROM VALUES (1, 'a/b'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['[None, None, None]', '[None, None, None]'].
    @column_args @sail-bug
    Scenario: xpath takes argument 2 from a column
      When query
        """
        SELECT xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', c) AS result FROM VALUES (1, 'a/b'), (2, 'a/b') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT
