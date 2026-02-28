Feature: IDENTIFIER clause

  Rule: IDENTIFIER in expression context

    Scenario: IDENTIFIER with column name in SELECT
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW t AS
        SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)
        """
      When query
        """
        SELECT IDENTIFIER('id') FROM t ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |

    Scenario: IDENTIFIER with column name in WHERE
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW t AS
        SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)
        """
      When query
        """
        SELECT id FROM t WHERE IDENTIFIER('id') > 1
        """
      Then query result
        | id |
        | 2  |

    Scenario: IDENTIFIER with qualified column name in SELECT
      When query
        """
        SELECT IDENTIFIER('t.id') FROM VALUES (1, 'a'), (2, 'b') AS t(id, name) ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |

    Scenario: IDENTIFIER with qualified column name in WHERE
      When query
        """
        SELECT id FROM VALUES (1, 'a'), (2, 'b') AS t(id, name) WHERE IDENTIFIER('t.id') > 1
        """
      Then query result
        | id |
        | 2  |

  Rule: IDENTIFIER in table context

    Scenario: IDENTIFIER as table name in FROM clause
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW my_view AS
        SELECT * FROM VALUES (10), (20) AS t(val)
        """
      When query
        """
        SELECT * FROM IDENTIFIER('my_view') ORDER BY val
        """
      Then query result ordered
        | val |
        | 10  |
        | 20  |

    Scenario: IDENTIFIER as table name with alias
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW my_view AS
        SELECT * FROM VALUES (1), (2) AS t(x)
        """
      When query
        """
        SELECT v.x FROM IDENTIFIER('my_view') AS v ORDER BY x
        """
      Then query result ordered
        | x |
        | 1 |
        | 2 |
