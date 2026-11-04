Feature: TABLESAMPLE clause

  Rule: TABLESAMPLE with PERCENT

    Scenario: TABLESAMPLE 100 PERCENT returns all rows
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW ts_data AS
        SELECT * FROM VALUES (1), (2), (3), (4), (5) AS t(id)
        """
      When query
        """
        SELECT id FROM ts_data TABLESAMPLE (100 PERCENT) ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
        | 3  |
        | 4  |
        | 5  |

    Scenario: TABLESAMPLE 0 PERCENT returns no rows
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW ts_data AS
        SELECT * FROM VALUES (1), (2), (3), (4), (5) AS t(id)
        """
      When query
        """
        SELECT COUNT(*) AS cnt FROM ts_data TABLESAMPLE (0 PERCENT)
        """
      Then query result
        | cnt |
        | 0   |

    Scenario: TABLESAMPLE with float percent
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW ts_data AS
        SELECT * FROM VALUES (1), (2), (3), (4), (5) AS t(id)
        """
      When query
        """
        SELECT COUNT(*) AS cnt FROM ts_data TABLESAMPLE (100.0 PERCENT)
        """
      Then query result
        | cnt |
        | 5   |

    Scenario Outline: TABLESAMPLE PERCENT accepts Spark's rounding tolerance
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW ts_data AS
        SELECT * FROM range(0, 10, 1, 1)
        """
      When query
        """
        SELECT COUNT(*) AS cnt
        FROM ts_data TABLESAMPLE (<percent> PERCENT) REPEATABLE (1)
        """
      Then query result
        | cnt   |
        | <cnt> |

      Examples:
        | percent             | cnt |
        | -0.00005            | 0   |
        | -0.0001             | 0   |
        | 100.00005           | 10  |

  Rule: TABLESAMPLE with BUCKET

    Scenario: TABLESAMPLE BUCKET 1 OUT OF 1 returns all rows
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW ts_data AS
        SELECT * FROM VALUES (1), (2), (3), (4), (5) AS t(id)
        """
      When query
        """
        SELECT id FROM ts_data TABLESAMPLE (BUCKET 1 OUT OF 1) ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
        | 3  |
        | 4  |
        | 5  |

    Scenario Outline: TABLESAMPLE BUCKET uses numerator as the sample fraction
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW ts_data AS
        SELECT * FROM range(0, 10, 1, 1)
        """
      When query
        """
        SELECT COUNT(*) AS cnt
        FROM ts_data TABLESAMPLE (BUCKET <numerator> OUT OF 4) REPEATABLE (1)
        """
      Then query result
        | cnt   |
        | <cnt> |

      Examples:
        | numerator | cnt |
        | 0         | 0   |
        | 4         | 10  |

  Rule: TABLESAMPLE error cases

    Scenario: TABLESAMPLE with percent greater than 100
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW ts_data AS
        SELECT * FROM VALUES (1), (2), (3) AS t(id)
        """
      When query
        """
        SELECT * FROM ts_data TABLESAMPLE (200 PERCENT)
        """
      Then query error Sampling fraction

    Scenario: TABLESAMPLE with negative percent
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW ts_data AS
        SELECT * FROM VALUES (1), (2), (3) AS t(id)
        """
      When query
        """
        SELECT * FROM ts_data TABLESAMPLE (-10 PERCENT)
        """
      Then query error Sampling fraction

    Scenario Outline: TABLESAMPLE PERCENT rejects values beyond Spark's rounding tolerance
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW ts_data AS
        SELECT * FROM range(0, 10, 1, 1)
        """
      When query
        """
        SELECT * FROM ts_data TABLESAMPLE (<percent> PERCENT)
        """
      Then query error Sampling fraction

      Examples:
        | percent              |
        | -0.00010000000000001 |
        | 100.0001             |

  Rule: TABLESAMPLE on subqueries

    Scenario: TABLESAMPLE 100 PERCENT on subquery returns all rows
      When query
        """
        SELECT id FROM (SELECT * FROM VALUES (1), (2), (3), (4), (5) AS t(id)) TABLESAMPLE (100 PERCENT) ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
        | 3  |
        | 4  |
        | 5  |

    Scenario: TABLESAMPLE 0 PERCENT on subquery returns no rows
      When query
        """
        SELECT COUNT(*) AS cnt FROM (SELECT * FROM VALUES (1), (2), (3), (4), (5) AS t(id)) TABLESAMPLE (0 PERCENT)
        """
      Then query result
        | cnt |
        | 0   |

    Scenario: TABLESAMPLE on filtered subquery returns all matching rows
      When query
        """
        SELECT id FROM (SELECT * FROM VALUES (1), (2), (3), (4), (5) AS t(id) WHERE id > 2) TABLESAMPLE (100 PERCENT) ORDER BY id
        """
      Then query result ordered
        | id |
        | 3  |
        | 4  |
        | 5  |

    Scenario: TABLESAMPLE BUCKET on subquery returns all rows
      When query
        """
        SELECT id FROM (SELECT * FROM VALUES (1), (2), (3), (4), (5) AS t(id)) TABLESAMPLE (BUCKET 1 OUT OF 1) ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
        | 3  |
        | 4  |
        | 5  |
