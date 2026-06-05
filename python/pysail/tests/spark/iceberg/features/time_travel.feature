Feature: Iceberg SQL time travel

  @sail-only
  Rule: SQL time travel works for named and direct Iceberg reads

    Background:
      Given variable location for temporary directory iceberg_sql_time_travel
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_sql_time_travel_test
        """
      Given statement template
        """
        CREATE TABLE iceberg_sql_time_travel_test (
          id INT,
          value STRING
        )
        USING iceberg
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO iceberg_sql_time_travel_test VALUES (1, 'v0')
        """
      Given statement
        """
        INSERT INTO iceberg_sql_time_travel_test VALUES (2, 'v1')
        """
      Given statement
        """
        INSERT INTO iceberg_sql_time_travel_test VALUES (3, 'v2')
        """
      Given iceberg snapshot timestamps in location are rewritten to consecutive seconds starting 10 seconds ago
      Given variable snapshot_ids for iceberg snapshot ids in location
      Given variable snapshot_times for iceberg snapshot timestamp strings in location

    Scenario: SQL supports version, timestamp, direct-path, and scalar-subquery time travel on Iceberg
      When query template
        """
        SELECT * FROM iceberg_sql_time_travel_test VERSION AS OF {{ snapshot_ids[0] }}
        """
      Then query result ordered
        | id | value |
        | 1  | v0    |

      When query template
        """
        SELECT * FROM iceberg_sql_time_travel_test TIMESTAMP AS OF '{{ snapshot_times[1] }}' ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | v0    |
        | 2  | v1    |

      When query template
        """
        SELECT * FROM iceberg.`{{ location.file_uri }}` VERSION AS OF {{ snapshot_ids[0] }}
        """
      Then query result ordered
        | id | value |
        | 1  | v0    |

      When query template
        """
        SELECT * FROM iceberg.`{{ location.file_uri }}` TIMESTAMP AS OF '{{ snapshot_times[2] }}' ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | v0    |
        | 2  | v1    |
        | 3  | v2    |

      When query
        """
        SELECT * FROM iceberg_sql_time_travel_test TIMESTAMP AS OF (SELECT current_timestamp()) ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | v0    |
        | 2  | v1    |
        | 3  | v2    |

      When query
        """
        SELECT * FROM iceberg_sql_time_travel_test TIMESTAMP AS OF id
        """
      Then query error Invalid time travel spec

      When query
        """
        SELECT * FROM iceberg_sql_time_travel_test TIMESTAMP AS OF rand()
        """
      Then query error Invalid time travel spec

  @sail-only
  Rule: VERSION AS OF selects schema by Iceberg ref type

    Background:
      Given variable location for temporary directory iceberg_sql_time_travel_schema
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_sql_time_travel_schema_test
        """
      Given statement template
        """
        CREATE TABLE iceberg_sql_time_travel_schema_test (
          id INT,
          value STRING
        )
        USING iceberg
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO iceberg_sql_time_travel_schema_test VALUES (1, 'base')
        """
      Given append JSON row {"id": 2, "value": "new", "extra": 10} to iceberg table in location with mergeSchema
      Given variable snapshot_ids for iceberg snapshot ids in location
      Given iceberg tag v1tag in location points to snapshot index 0

    Scenario: Main branch uses current schema while tag and snapshot id use snapshot schema
      When query
        """
        SELECT * FROM iceberg_sql_time_travel_schema_test VERSION AS OF 'main' ORDER BY id
        """
      Then query result ordered
        | id | value | extra |
        | 1  | base  | NULL  |
        | 2  | new   | 10    |

      When query template
        """
        SELECT * FROM iceberg_sql_time_travel_schema_test VERSION AS OF {{ snapshot_ids[0] }}
        """
      Then query result ordered
        | id | value |
        | 1  | base  |

      When query
        """
        SELECT * FROM iceberg_sql_time_travel_schema_test VERSION AS OF 'v1tag'
        """
      Then query result ordered
        | id | value |
        | 1  | base  |
