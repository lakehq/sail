Feature: first aggregate function returns the first value in a group

  Rule: first returns a single value from a single-row group

    Scenario: first returns an integer value
      When query
        """
        SELECT first(x) AS result FROM (VALUES (42)) AS t(x)
        """
      Then query result
        | result |
        | 42     |

    Scenario: first returns a string value
      When query
        """
        SELECT first(x) AS result FROM (VALUES ('hello')) AS t(x)
        """
      Then query result
        | result |
        | hello  |

    Scenario: first returns NULL when the only value is NULL
      When query
        """
        SELECT first(x) AS result FROM (VALUES (CAST(NULL AS INT))) AS t(x)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: first returns NULL for an empty group
      When query
        """
        SELECT first(x) AS result FROM (SELECT 1 AS x WHERE false) AS t
        """
      Then query result
        | result |
        | NULL   |

  Rule: first with group by returns the value from each single-row group

    Scenario: first over groups with single rows
      When query
        """
        SELECT grp, first(x) AS result
        FROM (VALUES ('a', 5), ('b', NULL), ('c', 10)) AS t(grp, x)
        GROUP BY grp
        ORDER BY grp
        """
      Then query result ordered
        | grp | result |
        | a   | 5      |
        | b   | NULL   |
        | c   | 10     |

  Rule: first with ignorenulls skips null values

    Scenario: first with ignorenulls true returns the only non-null value
      When query
        """
        SELECT first(x, true) AS result
        FROM (VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT)), (5)) AS t(x)
        """
      Then query result
        | result |
        | 5      |

    Scenario: first with ignorenulls true returns NULL when all values are NULL
      When query
        """
        SELECT first(x, true) AS result
        FROM (VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT))) AS t(x)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: first with ignorenulls false returns NULL when all values are NULL
      When query
        """
        SELECT first(x, false) AS result
        FROM (VALUES (CAST(NULL AS INT))) AS t(x)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: first with ignorenulls across groups
      When query
        """
        SELECT grp, first(x, true) AS result
        FROM (VALUES ('a', NULL), ('a', 42), ('b', NULL)) AS t(grp, x)
        GROUP BY grp
        ORDER BY grp
        """
      Then query result ordered
        | grp | result |
        | a   | 42     |
        | b   | NULL   |

  Rule: first supports IGNORE NULLS SQL syntax

    Scenario: first with IGNORE NULLS returns first non-null value
      When query
        """
        SELECT first(x IGNORE NULLS) AS result
        FROM (VALUES (CAST(NULL AS INT)), (5)) AS t(x)
        """
      Then query result
        | result |
        | 5      |

    Scenario: first with IGNORE NULLS returns NULL when all values are NULL
      When query
        """
        SELECT first(x IGNORE NULLS) AS result
        FROM (VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT))) AS t(x)
        """
      Then query result
        | result |
        | NULL   |
