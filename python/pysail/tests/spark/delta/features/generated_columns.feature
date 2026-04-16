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
