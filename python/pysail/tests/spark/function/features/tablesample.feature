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
