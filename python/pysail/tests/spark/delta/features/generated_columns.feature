Feature: Delta Lake Generated Columns

  Rule: Generated columns are computed from generation expressions during inserts

    Background:
      Given variable location for temporary directory gen_col_basic
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_gen_col_basic
        """

    @sail-only
    Scenario: Create table with generated column and insert without specifying generated column
      Given statement template
        """
        CREATE TABLE delta_gen_col_basic (
          id INT,
          event_time TIMESTAMP,
          event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_gen_col_basic (id, event_time)
        VALUES (1, TIMESTAMP '2024-10-15 10:30:00'), (2, TIMESTAMP '2024-10-16 14:00:00')
        """
      When query
        """
        SELECT id, event_time, event_date FROM delta_gen_col_basic ORDER BY id
        """
      Then query result ordered
        | id | event_time          | event_date |
        | 1  | 2024-10-15 10:30:00 | 2024-10-15 |
        | 2  | 2024-10-16 14:00:00 | 2024-10-16 |

    @sail-only
    Scenario: Create table with generated column and insert with explicit correct values
      Given statement template
        """
        CREATE TABLE delta_gen_col_basic (
          id INT,
          event_time TIMESTAMP,
          event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_gen_col_basic
        VALUES (1, TIMESTAMP '2024-10-15 10:30:00', DATE '2024-10-15')
        """
      When query
        """
        SELECT id, event_time, event_date FROM delta_gen_col_basic ORDER BY id
        """
      Then query result ordered
        | id | event_time          | event_date |
        | 1  | 2024-10-15 10:30:00 | 2024-10-15 |

  Rule: Generated column protocol and metadata are recorded in the delta log

    Background:
      Given variable location for temporary directory gen_col_meta
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_gen_col_meta
        """

    @sail-only
    Scenario: Delta log metadata contains generation expression for generated columns
      Given statement template
        """
        CREATE TABLE delta_gen_col_meta (
          id INT,
          event_time TIMESTAMP,
          event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_gen_col_meta (id, event_time)
        VALUES (1, TIMESTAMP '2024-10-15 10:30:00')
        """
      Then delta log first commit protocol and metadata contains
        | path                                                   | value                |
        | protocol.minWriterVersion                              | 7                    |

    @sail-only
    Scenario: Delta log first commit snapshot records generation expression and writerFeatures
      Given statement template
        """
        CREATE TABLE delta_gen_col_meta (
          id INT,
          event_time TIMESTAMP,
          event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_gen_col_meta (id, event_time)
        VALUES (1, TIMESTAMP '2024-10-15 10:30:00')
        """
      Then delta log first commit protocol and metadata matches snapshot

  Rule: Generated columns with different expressions

    Background:
      Given variable location for temporary directory gen_col_year
      Given final statement
        """
        DROP TABLE IF EXISTS delta_gen_col_year
        """

    @sail-only
    Scenario: Generated column using YEAR expression
      Given statement template
        """
        CREATE TABLE delta_gen_col_year (
          id INT,
          event_time TIMESTAMP,
          event_year INT GENERATED ALWAYS AS (YEAR(event_time))
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_gen_col_year (id, event_time)
        VALUES (1, TIMESTAMP '2024-10-15 10:30:00'), (2, TIMESTAMP '2023-05-20 08:00:00')
        """
      When query
        """
        SELECT id, event_year FROM delta_gen_col_year ORDER BY id
        """
      Then query result ordered
        | id | event_year |
        | 1  | 2024       |
        | 2  | 2023       |

  Rule: Generated columns with partitioning

    Background:
      Given variable location for temporary directory gen_col_partition
      Given final statement
        """
        DROP TABLE IF EXISTS delta_gen_col_partition
        """

    @sail-only
    Scenario: Partitioned table with generated column
      Given statement template
        """
        CREATE TABLE delta_gen_col_partition (
          event_id BIGINT,
          data STRING,
          event_time TIMESTAMP,
          event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
        )
        USING DELTA
        LOCATION {{ location.sql }}
        PARTITIONED BY (event_date)
        """
      Given statement
        """
        INSERT INTO delta_gen_col_partition (event_id, data, event_time)
        VALUES
          (1, 'first', TIMESTAMP '2024-10-15 10:30:00'),
          (2, 'second', TIMESTAMP '2024-10-16 14:00:00')
        """
      When query
        """
        SELECT event_id, data, event_date FROM delta_gen_col_partition ORDER BY event_id
        """
      Then query result ordered
        | event_id | data   | event_date |
        | 1        | first  | 2024-10-15 |
        | 2        | second | 2024-10-16 |

  Rule: Multiple generated columns

    Background:
      Given variable location for temporary directory gen_col_multi
      Given final statement
        """
        DROP TABLE IF EXISTS delta_gen_col_multi
        """

    @sail-only
    Scenario: Table with multiple generated columns
      Given statement template
        """
        CREATE TABLE delta_gen_col_multi (
          id INT,
          event_time TIMESTAMP,
          event_year INT GENERATED ALWAYS AS (YEAR(event_time)),
          event_month INT GENERATED ALWAYS AS (MONTH(event_time)),
          event_day INT GENERATED ALWAYS AS (DAY(event_time))
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_gen_col_multi (id, event_time)
        VALUES (1, TIMESTAMP '2024-10-15 10:30:00')
        """
      When query
        """
        SELECT id, event_year, event_month, event_day FROM delta_gen_col_multi ORDER BY id
        """
      Then query result ordered
        | id | event_year | event_month | event_day |
        | 1  | 2024       | 10          | 15        |

  Rule: Type coercion for generated columns and their dependencies

    Background:
      Given variable location for temporary directory gen_col_coerce
      Given final statement
        """
        DROP TABLE IF EXISTS delta_gen_col_coerce
        """

    @sail-only
    Scenario: Insert with BIGINT generated column using arithmetic on BIGINT dependency coerces INT input
      Given statement template
        """
        CREATE TABLE delta_gen_col_coerce (
          id INT,
          value BIGINT,
          doubled BIGINT GENERATED ALWAYS AS (value * 2)
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_gen_col_coerce (id, value)
        VALUES (1, 100), (2, 200)
        """
      When query
        """
        SELECT id, value, doubled FROM delta_gen_col_coerce ORDER BY id
        """
      Then query result ordered
        | id | value | doubled |
        | 1  | 100   | 200     |
        | 2  | 200   | 400     |

    @sail-only
    Scenario: Insert with explicit BIGINT generated column value of compatible type succeeds
      Given statement template
        """
        CREATE TABLE delta_gen_col_coerce (
          id INT,
          value BIGINT,
          doubled BIGINT GENERATED ALWAYS AS (value * 2)
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_gen_col_coerce
        VALUES (1, 100, 200)
        """
      When query
        """
        SELECT id, value, doubled FROM delta_gen_col_coerce ORDER BY id
        """
      Then query result ordered
        | id | value | doubled |
        | 1  | 100   | 200     |

    @sail-only
    Scenario: Insert with incorrect BIGINT generated column value raises mismatch error
      Given statement template
        """
        CREATE TABLE delta_gen_col_coerce (
          id INT,
          value BIGINT,
          doubled BIGINT GENERATED ALWAYS AS (value * 2)
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement with error DELTA_GENERATED_COLUMNS_VALUE_MISMATCH
        """
        INSERT INTO delta_gen_col_coerce
        VALUES (1, 100, 999)
        """

  Rule: Explicit generated column values must match the generation expression

    Background:
      Given variable location for temporary directory gen_col_enforce
      Given final statement
        """
        DROP TABLE IF EXISTS delta_gen_col_enforce
        """

    @sail-only
    Scenario: Insert with correct explicit generated column value succeeds
      Given statement template
        """
        CREATE TABLE delta_gen_col_enforce (
          id INT,
          event_time TIMESTAMP,
          event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_gen_col_enforce
        VALUES (1, TIMESTAMP '2024-05-10 12:00:00', DATE '2024-05-10')
        """
      When query
        """
        SELECT id, event_time, event_date FROM delta_gen_col_enforce ORDER BY id
        """
      Then query result ordered
        | id | event_time          | event_date |
        | 1  | 2024-05-10 12:00:00 | 2024-05-10 |

    @sail-only
    Scenario: Insert with NULL explicit generated column value is replaced by the expression
      Given statement template
        """
        CREATE TABLE delta_gen_col_enforce (
          id INT,
          event_time TIMESTAMP,
          event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_gen_col_enforce
        VALUES (1, TIMESTAMP '2024-05-10 12:00:00', CAST(NULL AS DATE))
        """
      When query
        """
        SELECT id, event_date FROM delta_gen_col_enforce ORDER BY id
        """
      Then query result ordered
        | id | event_date |
        | 1  | 2024-05-10 |

    @sail-only
    Scenario: Insert with incorrect explicit generated column value fails
      Given statement template
        """
        CREATE TABLE delta_gen_col_enforce (
          id INT,
          event_time TIMESTAMP,
          event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement with error DELTA_GENERATED_COLUMNS_VALUE_MISMATCH
        """
        INSERT INTO delta_gen_col_enforce
        VALUES (1, TIMESTAMP '2024-05-10 12:00:00', DATE '2099-01-01')
        """

  Rule: Generated columns are recomputed during MERGE

    Background:
      Given variable location for temporary directory gen_col_merge
      Given variable location2 for temporary directory gen_col_merge_src
      Given final statement
        """
        DROP TABLE IF EXISTS delta_gen_col_merge
        """
      Given final statement
        """
        DROP TABLE IF EXISTS delta_gen_col_merge_src
        """

    @sail-only
    Scenario: MERGE matched update recomputes generated column
      Given statement template
        """
        CREATE TABLE delta_gen_col_merge (
          id INT,
          event_time TIMESTAMP,
          event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement template
        """
        CREATE TABLE delta_gen_col_merge_src (
          id INT,
          event_time TIMESTAMP
        )
        USING DELTA
        LOCATION {{ location2.sql }}
        """
      Given statement
        """
        INSERT INTO delta_gen_col_merge (id, event_time)
        VALUES (1, TIMESTAMP '2024-01-01 00:00:00')
        """
      Given statement
        """
        INSERT INTO delta_gen_col_merge_src VALUES (1, TIMESTAMP '2024-09-01 00:00:00')
        """
      Given statement
        """
        MERGE INTO delta_gen_col_merge AS t
        USING delta_gen_col_merge_src AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.event_time = s.event_time
        """
      When query
        """
        SELECT id, event_time, event_date FROM delta_gen_col_merge ORDER BY id
        """
      Then query result ordered
        | id | event_time          | event_date |
        | 1  | 2024-09-01 00:00:00 | 2024-09-01 |

    @sail-only
    Scenario: MERGE not-matched insert computes generated column
      Given statement template
        """
        CREATE TABLE delta_gen_col_merge (
          id INT,
          event_time TIMESTAMP,
          event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement template
        """
        CREATE TABLE delta_gen_col_merge_src (
          id INT,
          event_time TIMESTAMP
        )
        USING DELTA
        LOCATION {{ location2.sql }}
        """
      Given statement
        """
        INSERT INTO delta_gen_col_merge (id, event_time)
        VALUES (1, TIMESTAMP '2024-01-01 00:00:00')
        """
      Given statement
        """
        INSERT INTO delta_gen_col_merge_src VALUES (2, TIMESTAMP '2024-09-01 00:00:00')
        """
      Given statement
        """
        MERGE INTO delta_gen_col_merge AS t
        USING delta_gen_col_merge_src AS s
        ON t.id = s.id
        WHEN NOT MATCHED THEN INSERT (id, event_time) VALUES (s.id, s.event_time)
        """
      When query
        """
        SELECT id, event_time, event_date FROM delta_gen_col_merge ORDER BY id
        """
      Then query result ordered
        | id | event_time          | event_date |
        | 1  | 2024-01-01 00:00:00 | 2024-01-01 |
        | 2  | 2024-09-01 00:00:00 | 2024-09-01 |

    @sail-only
    Scenario: MERGE insert with explicit correct generated column value succeeds
      Given statement template
        """
        CREATE TABLE delta_gen_col_merge (
          id INT,
          event_time TIMESTAMP,
          event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement template
        """
        CREATE TABLE delta_gen_col_merge_src (
          id INT,
          event_time TIMESTAMP,
          event_date DATE
        )
        USING DELTA
        LOCATION {{ location2.sql }}
        """
      Given statement
        """
        INSERT INTO delta_gen_col_merge (id, event_time)
        VALUES (1, TIMESTAMP '2024-01-01 00:00:00')
        """
      Given statement
        """
        INSERT INTO delta_gen_col_merge_src VALUES (2, TIMESTAMP '2024-09-01 00:00:00', DATE '2024-09-01')
        """
      Given statement
        """
        MERGE INTO delta_gen_col_merge AS t
        USING delta_gen_col_merge_src AS s
        ON t.id = s.id
        WHEN NOT MATCHED THEN INSERT (id, event_time, event_date) VALUES (s.id, s.event_time, s.event_date)
        """
      When query
        """
        SELECT id, event_time, event_date FROM delta_gen_col_merge ORDER BY id
        """
      Then query result ordered
        | id | event_time          | event_date |
        | 1  | 2024-01-01 00:00:00 | 2024-01-01 |
        | 2  | 2024-09-01 00:00:00 | 2024-09-01 |

    @sail-only
    Scenario: MERGE insert with incorrect explicit generated column value fails
      Given statement template
        """
        CREATE TABLE delta_gen_col_merge (
          id INT,
          event_time TIMESTAMP,
          event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement template
        """
        CREATE TABLE delta_gen_col_merge_src (
          id INT,
          event_time TIMESTAMP,
          event_date DATE
        )
        USING DELTA
        LOCATION {{ location2.sql }}
        """
      Given statement
        """
        INSERT INTO delta_gen_col_merge (id, event_time)
        VALUES (1, TIMESTAMP '2024-01-01 00:00:00')
        """
      Given statement
        """
        INSERT INTO delta_gen_col_merge_src VALUES (2, TIMESTAMP '2024-09-01 00:00:00', DATE '2099-01-01')
        """
      Given statement with error DELTA_GENERATED_COLUMNS_VALUE_MISMATCH
        """
        MERGE INTO delta_gen_col_merge AS t
        USING delta_gen_col_merge_src AS s
        ON t.id = s.id
        WHEN NOT MATCHED THEN INSERT (id, event_time, event_date) VALUES (s.id, s.event_time, s.event_date)
        """

  Rule: Generation expression is recovered from delta log metadata when catalog has no column definitions

    Background:
      Given variable location for temporary directory gen_col_recovery
      Given final statement
        """
        DROP TABLE IF EXISTS delta_gen_col_recovery
        """

    @sail-only
    Scenario: INSERT into a schema-only registered table recovers generation expression from delta log
      Given statement template
        """
        CREATE TABLE delta_gen_col_recovery (
          id INT,
          event_time TIMESTAMP,
          event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_gen_col_recovery (id, event_time)
        VALUES (1, TIMESTAMP '2024-03-20 09:00:00')
        """
      Given statement
        """
        DROP TABLE IF EXISTS delta_gen_col_recovery
        """
      Given statement template
        """
        CREATE TABLE delta_gen_col_recovery
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_gen_col_recovery (id, event_time)
        VALUES (2, TIMESTAMP '2024-06-15 18:30:00')
        """
      When query
        """
        SELECT id, event_time, event_date FROM delta_gen_col_recovery ORDER BY id
        """
      Then query result ordered
        | id | event_time          | event_date |
        | 1  | 2024-03-20 09:00:00 | 2024-03-20 |
        | 2  | 2024-06-15 18:30:00 | 2024-06-15 |
