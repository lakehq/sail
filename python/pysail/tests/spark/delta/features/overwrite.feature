Feature: Delta Lake Overwrite

  Rule: Conditional overwrite resolves mapped data columns

    Scenario Outline: REPLACE WHERE preserves rows across mapped scan modes
      Given variable location for temporary directory delta_mapped_replace_where
      Given final statement
        """
        DROP TABLE IF EXISTS delta_mapped_replace_where
        """
      Given statement template
        """
        CREATE TABLE delta_mapped_replace_where (day INT, id BIGINT, value BIGINT)
        USING DELTA PARTITIONED BY (day) LOCATION {{ location.sql }}
        OPTIONS (metadataAsDataRead '<metadata_scan>')
        TBLPROPERTIES (
          'delta.columnMapping.mode' = '<mapping_mode>',
          'delta.dataSkippingNumIndexedCols' = '<indexed_columns>'
        )
        """
      Given statement
        """
        INSERT INTO delta_mapped_replace_where VALUES (0, 1, 10), (0, 100, 1000)
        """
      Given statement
        """
        INSERT INTO delta_mapped_replace_where VALUES (NULL, 2, 20), (NULL, 200, 2000)
        """
      Given statement
        """
        INSERT INTO delta_mapped_replace_where VALUES (2, 300, 3000)
        """
      Given statement
        """
        INSERT INTO delta_mapped_replace_where REPLACE WHERE id < 50
        SELECT * FROM VALUES (2, 10, -10), (2, 20, -20) AS s(day, id, value)
        """
      Then delta log latest commit info contains
        | path                     | value       |
        | operation                | "WRITE"     |
        | operationParameters.mode | "Overwrite" |
      When query template
        """
        SELECT id, day, value FROM delta.`{{ location.string }}` ORDER BY id
        """
      Then query result collected ordered
        | id  | day  | value |
        | 10  | 2    | -10   |
        | 20  | 2    | -20   |
        | 100 | 0    | 1000  |
        | 200 | NULL | 2000  |
        | 300 | 2    | 3000  |
      When query
        """
        SELECT id, day, value FROM delta_mapped_replace_where ORDER BY id
        """
      Then query result collected ordered
        | id  | day  | value |
        | 10  | 2    | -10   |
        | 20  | 2    | -20   |
        | 100 | 0    | 1000  |
        | 200 | NULL | 2000  |
        | 300 | 2    | 3000  |

      Examples:
        | mapping_mode | indexed_columns | metadata_scan |
        | name         | 32              | false         |
        | name         | 0               | false         |
        | name         | 32              | true          |
        | id           | 32              | false         |
        | id           | 32              | true          |

  Rule: Overwrite and conditional overwrite (REPLACE WHERE)
    Background:
      Given variable location for temporary directory delta_overwrite
      Given final statement
        """
        DROP TABLE IF EXISTS delta_overwrite_basic
        """
      Given statement template
        """
        CREATE TABLE delta_overwrite_basic (
          id BIGINT,
          category STRING,
          value BIGINT
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_overwrite_basic
        SELECT * FROM VALUES
          (1, 'A', 10),
          (2, 'B', 20),
          (3, 'A', 30),
          (4, 'B', 40)
        AS tab(id, category, value)
        """

    @sail-bug
    Scenario: Conditional overwrite rejects a non-deterministic predicate before writing
      When query
        """
        INSERT INTO delta_overwrite_basic
        REPLACE WHERE rand(0) > 0.5 AND id > 0
        SELECT * FROM VALUES (5, 'A', 100) AS tab(id, category, value)
        """
      Then query error Non-deterministic expressions are not allowed in OVERWRITE conditions
      When query
        """
        SELECT id, category, value FROM delta_overwrite_basic ORDER BY id
        """
      Then query result ordered
        | id | category | value |
        | 1  | A        | 10    |
        | 2  | B        | 20    |
        | 3  | A        | 30    |
        | 4  | B        | 40    |

    Scenario: EXPLAIN plan for conditional overwrite (REPLACE WHERE category = 'A')
      When query
        """
        EXPLAIN
        INSERT INTO delta_overwrite_basic
        REPLACE WHERE category = 'A'
        SELECT * FROM VALUES
          (5, 'A', 100),
          (6, 'A', 200)
        AS tab(id, category, value)
        """
      Then query plan matches snapshot

    Scenario: Conditional overwrite keeps non-matching rows (REPLACE WHERE)
      Given statement
        """
        INSERT INTO delta_overwrite_basic
        REPLACE WHERE category = 'A'
        SELECT * FROM VALUES
          (5, 'A', 100),
          (6, 'A', 200)
        AS tab(id, category, value)
        """
      Then delta log latest commit info matches snapshot
      When query
        """
        SELECT id, category, value FROM delta_overwrite_basic ORDER BY id
        """
      Then query result ordered
        | id | category | value |
        | 2  | B        | 20    |
        | 4  | B        | 40    |
        | 5  | A        | 100   |
        | 6  | A        | 200   |

    Scenario: EXPLAIN plan for full conditional overwrite (REPLACE WHERE id >= CAST(0 AS BIGINT))
      When query
        """
        EXPLAIN
        INSERT INTO delta_overwrite_basic
        REPLACE WHERE id >= CAST(0 AS BIGINT)
        SELECT * FROM VALUES
          (10, 'C', 999),
          (11, 'D', 111)
        AS tab(id, category, value)
        """
      Then query plan matches snapshot

    Scenario: Conditional overwrite can replace all rows (REPLACE WHERE id >= CAST(0 AS BIGINT))
      Given statement
        """
        INSERT INTO delta_overwrite_basic
        REPLACE WHERE id >= CAST(0 AS BIGINT)
        SELECT * FROM VALUES
          (10, 'C', 999),
          (11, 'D', 111)
        AS tab(id, category, value)
        """
      Then delta log latest commit info matches snapshot
      When query
        """
        SELECT id, category, value FROM delta_overwrite_basic ORDER BY id
        """
      Then query result ordered
        | id | category | value |
        | 10 | C        | 999   |
        | 11 | D        | 111   |

  Rule: Schema overwrite can change partition metadata
    Background:
      Given variable location for temporary directory delta_overwrite_partition_schema
      Given final statement
        """
        DROP TABLE IF EXISTS delta_overwrite_partition_schema
        """

    Scenario: CREATE OR REPLACE TABLE updates Delta partition columns
      Given statement template
        """
        CREATE TABLE delta_overwrite_partition_schema
        USING DELTA
        PARTITIONED BY (id)
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (1, 'Alice'),
          (2, 'Bob')
        AS t(id, name)
        """
      Given statement template
        """
        CREATE OR REPLACE TABLE delta_overwrite_partition_schema
        USING DELTA
        PARTITIONED BY (name)
        OPTIONS (overwriteSchema 'true')
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (1, 'Alice'),
          (2, 'Bob')
        AS t(id, name)
        """
      Then delta log latest effective protocol and metadata contains
        | path                      | value    |
        | metaData.partitionColumns | ["name"] |
      When query
        """
        SELECT id, name FROM delta_overwrite_partition_schema ORDER BY id
        """
      Then query result ordered
        | id | name  |
        | 1  | Alice |
        | 2  | Bob   |

    Scenario: CREATE OR REPLACE TABLE can remove Delta partition columns
      Given statement template
        """
        CREATE TABLE delta_overwrite_partition_schema
        USING DELTA
        PARTITIONED BY (name)
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (1, 'Alice'),
          (2, 'Bob')
        AS t(id, name)
        """
      Given statement template
        """
        CREATE OR REPLACE TABLE delta_overwrite_partition_schema
        USING DELTA
        OPTIONS (overwriteSchema 'true')
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (3, 'Carol'),
          (4, 'Dave')
        AS t(id, name)
        """
      Then delta log latest effective protocol and metadata contains
        | path                      | value |
        | metaData.partitionColumns | []    |
      When query
        """
        SELECT id, name FROM delta_overwrite_partition_schema ORDER BY id
        """
      Then query result ordered
        | id | name  |
        | 3  | Carol |
        | 4  | Dave  |

