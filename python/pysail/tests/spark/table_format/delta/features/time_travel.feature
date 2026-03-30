Feature: Delta Lake SQL time travel

  @sail-only
  Rule: SQL time travel works for named and direct Delta reads

    Background:
      Given variable location for temporary directory delta_sql_time_travel
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_sql_time_travel_test
        """
      Given statement template
        """
        CREATE TABLE delta_sql_time_travel_test (
          id INT,
          value STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_sql_time_travel_test VALUES (1, 'v0')
        """
      Given statement
        """
        INSERT INTO delta_sql_time_travel_test VALUES (2, 'v1')
        """
      Given statement
        """
        INSERT OVERWRITE TABLE delta_sql_time_travel_test VALUES (3, 'v2')
        """
      Given delta log JSON files for versions 0 in delta_log are backdated by 5 seconds
      Given delta log JSON files for versions 1 in delta_log are backdated by 3 seconds

    Scenario: SQL supports version, timestamp, and scalar-subquery time travel on Delta
      When query
        """
        SELECT * FROM delta_sql_time_travel_test VERSION AS OF 1 ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | v0    |
        | 2  | v1    |

      When query
        """
        SELECT * FROM delta_sql_time_travel_test TIMESTAMP AS OF current_timestamp() - INTERVAL 2 seconds ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | v0    |
        | 2  | v1    |

      When query template
        """
        SELECT * FROM delta.`{{ location.string }}` VERSION AS OF 0
        """
      Then query result ordered
        | id | value |
        | 1  | v0    |

      When query template
        """
        SELECT * FROM delta.`{{ location.string }}` TIMESTAMP AS OF (SELECT current_timestamp())
        """
      Then query result ordered
        | id | value |
        | 3  | v2    |

      When query
        """
        SELECT * FROM delta_sql_time_travel_test TIMESTAMP AS OF id
        """
      Then query error Invalid time travel spec

      When query
        """
        SELECT * FROM delta_sql_time_travel_test TIMESTAMP AS OF rand()
        """
      Then query error Invalid time travel spec

  @sail-only
  Rule: Delta time travel honors in-commit timestamps

    Background:
      Given variable location for temporary directory delta_sql_time_travel_ict
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_sql_time_travel_ict_test
        """
      Given statement template
        """
        CREATE TABLE delta_sql_time_travel_ict_test
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.enableInCommitTimestamps' = 'true'
        )
        AS SELECT 1 AS id, 'v0' AS value
        """
      Given statement
        """
        INSERT INTO delta_sql_time_travel_ict_test VALUES (2, 'v1')
        """
      Given delta log commit and checksum timestamps for versions 0, 1 in delta_log are 100, 200 milliseconds since epoch
      Given delta log JSON file timestamps for versions 0, 1 in delta_log are 86400, 1 seconds since epoch

    Scenario: SQL timestamp time travel uses in-commit timestamps instead of JSON mtimes
      When query
        """
        SELECT * FROM delta_sql_time_travel_ict_test
        TIMESTAMP AS OF '1970-01-01T00:00:00.150Z'
        ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | v0    |
