Feature: Struct NULL field extraction

  Rule: Extracting a field from a NULL struct should return NULL

    Scenario: Extract string field from NULL struct
      When query
        """
        SELECT s.name AS result
        FROM VALUES (named_struct('id', 1, 'name', 'alice')), (CAST(NULL AS STRUCT<id: INT, name: STRING>)) AS t(s)
        """
      Then query result
        | result |
        | alice  |
        | NULL   |

    Scenario: Extract int field from NULL struct
      When query
        """
        SELECT s.id AS result
        FROM VALUES (named_struct('id', 1, 'name', 'alice')), (CAST(NULL AS STRUCT<id: INT, name: STRING>)) AS t(s)
        """
      Then query result
        | result |
        | 1      |
        | NULL   |

    @sail-bug
    # Sail coerces VALUES with mixed STRUCT<val:DECIMAL> + STRUCT<val:DOUBLE> rows
    # to STRUCT<val:DECIMAL(30,15)> and renders `3.140000000000000`.
    # Spark picks STRUCT<val:DOUBLE> and renders `3.14`. Regression surfaced under
    # DataFusion 54's coercion changes. Tracked for follow-up; keep the expected
    # value as Spark's so this xfails today and xpasses once the coercion is fixed.
    Scenario: Extract double field from NULL struct
      When query
        """
        SELECT s.val AS result
        FROM VALUES (named_struct('val', 3.14)), (CAST(NULL AS STRUCT<val: DOUBLE>)) AS t(s)
        """
      Then query result
        | result |
        | 3.14   |
        | NULL   |

  Rule: Bracket notation extraction from NULL struct

    Scenario: Extract field via bracket notation from NULL struct
      When query
        """
        SELECT s['name'] AS result
        FROM VALUES (named_struct('id', 1, 'name', 'bob')), (CAST(NULL AS STRUCT<id: INT, name: STRING>)) AS t(s)
        """
      Then query result
        | result |
        | bob    |
        | NULL   |

    Scenario: Extract int via bracket notation from NULL struct
      When query
        """
        SELECT s['id'] AS result
        FROM VALUES (named_struct('id', 42, 'name', 'carol')), (CAST(NULL AS STRUCT<id: INT, name: STRING>)) AS t(s)
        """
      Then query result
        | result |
        | 42     |
        | NULL   |

  Rule: Nested struct NULL propagation

    Scenario: Extract nested field from NULL parent struct
      When query
        """
        SELECT s.inner.val AS result
        FROM VALUES (named_struct('inner', named_struct('val', 10))), (CAST(NULL AS STRUCT<inner: STRUCT<val: INT>>)) AS t(s)
        """
      Then query result
        | result |
        | 10     |
        | NULL   |

    Scenario: Extract field from struct with NULL inner struct
      When query
        """
        SELECT s.inner.val AS result
        FROM VALUES (named_struct('inner', CAST(NULL AS STRUCT<val: INT>))) AS t(s)
        """
      Then query result
        | result |
        | NULL   |

  Rule: Mixed NULL and non-NULL structs

    Scenario: Multiple rows with mixed NULL structs
      When query
        """
        SELECT s.name AS result
        FROM VALUES
          (named_struct('id', 1, 'name', 'alice')),
          (CAST(NULL AS STRUCT<id: INT, name: STRING>)),
          (named_struct('id', 3, 'name', 'carol')),
          (CAST(NULL AS STRUCT<id: INT, name: STRING>))
        AS t(s)
        """
      Then query result
        | result |
        | alice  |
        | NULL   |
        | carol  |
        | NULL   |

    Scenario: Struct field in WHERE clause with NULL struct
      When query
        """
        SELECT s.id AS result
        FROM VALUES
          (named_struct('id', 1, 'name', 'alice')),
          (CAST(NULL AS STRUCT<id: INT, name: STRING>)),
          (named_struct('id', 3, 'name', 'carol'))
        AS t(s)
        WHERE s.id IS NOT NULL
        ORDER BY result
        """
      Then query result ordered
        | result |
        | 1      |
        | 3      |

  Rule: Wildcard expansion from NULL struct

    Scenario: Expand all fields from NULL struct via wildcard
      When query
        """
        SELECT s.*
        FROM VALUES (named_struct('id', 1, 'name', 'alice')), (CAST(NULL AS STRUCT<id: INT, name: STRING>)) AS t(s)
        """
      Then query result
        | id   | name  |
        | 1    | alice |
        | NULL | NULL  |

    Scenario: Expand nested fields from NULL parent struct via wildcard
      When query
        """
        SELECT s.inner.*
        FROM VALUES (named_struct('inner', named_struct('val', 10, 'tag', 'x'))), (CAST(NULL AS STRUCT<inner: STRUCT<val: INT, tag: STRING>>)) AS t(s)
        """
      Then query result
        | val  | tag  |
        | 10   | x    |
        | NULL | NULL |

    Scenario: Expand fields from NULL inner struct via wildcard
      When query
        """
        SELECT s.inner.*
        FROM VALUES (named_struct('inner', CAST(NULL AS STRUCT<val: INT, tag: STRING>))) AS t(s)
        """
      Then query result
        | val  | tag  |
        | NULL | NULL |
