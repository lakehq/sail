Feature: count_if function

  Rule: count_if counts rows where condition is true

    Scenario: count_if with simple boolean condition
      When query
      """
      SELECT count_if(value > 2) AS result
      FROM VALUES (1), (2), (3), (4), (5) AS t(value)
      """
      Then query result
      | result |
      | 3      |

    Scenario: count_if with all true
      When query
      """
      SELECT count_if(value > 0) AS result
      FROM VALUES (1), (2), (3) AS t(value)
      """
      Then query result
      | result |
      | 3      |

    Scenario: count_if with all false
      When query
      """
      SELECT count_if(value > 10) AS result
      FROM VALUES (1), (2), (3) AS t(value)
      """
      Then query result
      | result |
      | 0      |

  Rule: count_if with NULL values

    Scenario: count_if ignores NULLs
      When query
      """
      SELECT count_if(value > 2) AS result
      FROM VALUES (1), (NULL), (3), (NULL), (5) AS t(value)
      """
      Then query result
      | result |
      | 2      |

    Scenario: count_if with all NULLs
      When query
      """
      SELECT count_if(value > 0) AS result
      FROM VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT)) AS t(value)
      """
      Then query result
      | result |
      | 0      |

  Rule: count_if with GROUP BY

    Scenario: count_if grouped by category
      When query
      """
      SELECT category, count_if(value > 2) AS result
      FROM VALUES ('a', 1), ('a', 3), ('a', 5), ('b', 1), ('b', 2) AS t(category, value)
      GROUP BY category
      ORDER BY category
      """
      Then query result ordered
      | category | result |
      | a        | 2      |
      | b        | 0      |

  Rule: count_if with string equality condition

    Scenario: count_if with string condition
      When query
      """
      SELECT count_if(color = 'red') AS result
      FROM VALUES ('red'), ('blue'), ('red'), ('green') AS t(color)
      """
      Then query result
      | result |
      | 2      |

  Rule: count_if as window function

    Scenario: count_if over window
      When query
      """
      SELECT value,
             count_if(value > 2) OVER (ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
      FROM VALUES (1), (2), (3), (4), (5) AS t(value)
      ORDER BY value
      """
      Then query result ordered
      | value | result |
      | 1     | 0      |
      | 2     | 0      |
      | 3     | 1      |
      | 4     | 2      |
      | 5     | 3      |
