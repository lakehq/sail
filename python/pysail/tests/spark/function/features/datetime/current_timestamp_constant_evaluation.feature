Feature: Current time functions in constant-evaluated arguments

  Rule: range table function arguments

    Scenario Outline: range bound evaluated with <function>
      When query
        """
        SELECT count(*) AS n FROM range(CASE WHEN <predicate> THEN 3 ELSE 0 END)
        """
      Then query result
        | n |
        | 3 |

      Examples:
        | function          | predicate                                             |
        | current_timestamp | current_timestamp() > TIMESTAMP'2000-01-01 00:00:00'  |
        | now               | now() > TIMESTAMP'2000-01-01 00:00:00'                |
        | localtimestamp    | localtimestamp() > TIMESTAMP_NTZ'2000-01-01 00:00:00' |
        | unix_timestamp    | unix_timestamp() > 946684800                          |
        | year              | year(current_timestamp()) > 2000                      |
        | current_date      | current_date() > DATE'2000-01-01'                     |

  Rule: IDENTIFIER clause

    Scenario: IDENTIFIER table name chosen by current_timestamp
      Given final statement
        """
        DROP VIEW IF EXISTS current_time_identifier_new
        """
      And final statement
        """
        DROP VIEW IF EXISTS current_time_identifier_old
        """
      And statement
        """
        CREATE OR REPLACE TEMPORARY VIEW current_time_identifier_new AS SELECT 'new' AS which
        """
      And statement
        """
        CREATE OR REPLACE TEMPORARY VIEW current_time_identifier_old AS SELECT 'old' AS which
        """
      When query
        """
        SELECT which
        FROM IDENTIFIER(
          CASE WHEN current_timestamp() > TIMESTAMP'2000-01-01 00:00:00'
            THEN 'current_time_identifier_new'
            ELSE 'current_time_identifier_old'
          END
        )
        """
      Then query result
        | which |
        | new   |

    Scenario: IDENTIFIER column name chosen by current_timestamp
      When query
        """
        SELECT IDENTIFIER(CASE WHEN current_timestamp() > TIMESTAMP'2000-01-01 00:00:00' THEN 'a' ELSE 'b' END) AS v
        FROM VALUES (1, 2) AS t(a, b)
        """
      Then query result
        | v |
        | 1 |

  Rule: Schema arguments

    Scenario: from_json schema chosen by current_timestamp
      When query
        """
        SELECT v.a AS a
        FROM (
          SELECT from_json('{"a":1}', CASE WHEN current_timestamp() > TIMESTAMP'2000-01-01 00:00:00' THEN 'a INT' ELSE 'b INT' END) AS v
        )
        """
      Then query result
        | a |
        | 1 |

    Scenario: from_csv schema chosen by current_timestamp
      When query
        """
        SELECT v.a AS a
        FROM (
          SELECT from_csv('1', CASE WHEN current_timestamp() > TIMESTAMP'2000-01-01 00:00:00' THEN 'a INT' ELSE 'b INT' END) AS v
        )
        """
      Then query result
        | a |
        | 1 |

  Rule: PIVOT values

    Scenario: PIVOT value chosen by current_timestamp
      When query
        """
        SELECT *
        FROM (SELECT 1 AS k, 10 AS v UNION ALL SELECT 2, 20)
        PIVOT (sum(v) FOR (k) IN (CASE WHEN current_timestamp() > TIMESTAMP'2000-01-01 00:00:00' THEN 1 ELSE 2 END AS one))
        """
      Then query result
        | one |
        | 10  |
