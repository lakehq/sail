Feature: CTE (Common Table Expressions) support

  Rule: Basic CTE

    Scenario: simple CTE with SELECT
      When query
      """
      WITH cte AS (SELECT 1 AS value)
      SELECT * FROM cte
      """
      Then query result
      | value |
      | 1     |

    Scenario: CTE with multiple columns
      When query
      """
      WITH cte AS (SELECT 'hello' AS greeting, 42 AS number)
      SELECT greeting, number FROM cte
      """
      Then query result
      | greeting | number |
      | hello    | 42     |

  Rule: Multiple CTEs

    Scenario: two CTEs in same WITH clause
      When query
      """
      WITH
        cte1 AS (SELECT 1 AS a),
        cte2 AS (SELECT 2 AS b)
      SELECT cte1.a, cte2.b FROM cte1, cte2
      """
      Then query result
      | a | b |
      | 1 | 2 |

    Scenario: CTE referencing another CTE
      When query
      """
      WITH
        cte1 AS (SELECT 1 AS value),
        cte2 AS (SELECT value + 1 AS value FROM cte1)
      SELECT * FROM cte2
      """
      Then query result
      | value |
      | 2     |

  Rule: Nested subqueries with table aliases

    Scenario: subquery with table alias
      When query
      """
      SELECT * FROM (SELECT 'abc' AS col) x
      """
      Then query result
      | col |
      | abc |

    Scenario: CTE with subquery using same alias name
      When query
      """
      WITH x AS (SELECT * FROM (SELECT 'inner' AS val) x)
      SELECT * FROM x
      """
      Then query result
      | val   |
      | inner |

  Rule: CTE shadowing

    Scenario: duplicate CTE name keeps the last definition
      When query
      """
      WITH
        x AS (SELECT 1 AS value),
        x AS (SELECT 2 AS value)
      SELECT * FROM x
      """
      Then query result
      | value |
      | 2     |

    Scenario: duplicate CTE name with intermediate CTEs
      When query
      """
      WITH
        x AS (SELECT 'first' AS label),
        y AS (SELECT 'middle' AS label),
        x AS (SELECT 'last' AS label)
      SELECT * FROM x
      """
      Then query result
      | label |
      | last  |

    Scenario: multiple shadowed CTEs preserve non-duplicate order
      When query
      """
      WITH
        a AS (SELECT 1 AS val),
        b AS (SELECT 2 AS val),
        a AS (SELECT 3 AS val)
      SELECT a.val AS a_val, b.val AS b_val FROM a, b
      """
      Then query result
      | a_val | b_val |
      | 3     | 2     |
