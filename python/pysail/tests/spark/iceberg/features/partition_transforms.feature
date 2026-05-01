Feature: Iceberg Partition Transforms

  Rule: Bucket transform partitions data by hash
    Background:
      Given variable location for temporary directory iceberg_bucket_transform
      Given final statement
        """
        DROP TABLE IF EXISTS bucket_test
        """

    Scenario: Bucket transform creates consistent partitions
      Given statement template
        """
        CREATE TABLE bucket_test (
          id INT,
          user_id BIGINT,
          action STRING
        )
        USING iceberg
        PARTITIONED BY (bucket(4, user_id))
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO bucket_test VALUES 
          (1, 1001, 'login'),
          (2, 1002, 'view'),
          (3, 1003, 'purchase'),
          (4, 1004, 'logout')
        """
      Then iceberg metadata matches snapshot
      Then iceberg snapshot operation is append
      When query
        """
        SELECT * FROM bucket_test ORDER BY id
        """
      Then query result ordered
        | id | user_id | action   |
        | 1  | 1001    | login    |
        | 2  | 1002    | view     |
        | 3  | 1003    | purchase |
        | 4  | 1004    | logout   |

    Scenario: Bucket transform equality filter returns correct results
      Given statement template
        """
        CREATE TABLE bucket_test (
          id INT,
          user_id BIGINT,
          payload STRING
        )
        USING iceberg
        PARTITIONED BY (bucket(4, user_id))
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO bucket_test VALUES
          (1, 1001, 'u1001'),
          (2, 1002, 'u1002'),
          (3, 1003, 'u1003'),
          (4, 1004, 'u1004'),
          (5, 1005, 'u1005'),
          (6, 1006, 'u1006')
        """
      When query
        """
        SELECT id, payload FROM bucket_test
        WHERE user_id = 1003
        ORDER BY id
        """
      Then query result ordered
        | id | payload |
        | 3  | u1003   |

  Rule: Truncate transform reduces cardinality
    Background:
      Given variable location for temporary directory iceberg_truncate_transform
      Given final statement
        """
        DROP TABLE IF EXISTS truncate_test
        """

    Scenario: Truncate on string column groups similar values
      Given statement template
        """
        CREATE TABLE truncate_test (
          id INT,
          code STRING,
          value DOUBLE
        )
        USING iceberg
        PARTITIONED BY (truncate(3, code))
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO truncate_test VALUES 
          (1, 'ABC123', 10.0),
          (2, 'ABC456', 20.0),
          (3, 'DEF789', 30.0),
          (4, 'DEF012', 40.0)
        """
      Then iceberg metadata matches snapshot
      Then iceberg partition spec matches snapshot
      When query
        """
        SELECT * FROM truncate_test ORDER BY id
        """
      Then query result ordered
        | id | code   | value |
        | 1  | ABC123 | 10.0  |
        | 2  | ABC456 | 20.0  |
        | 3  | DEF789 | 30.0  |
        | 4  | DEF012 | 40.0  |

    Scenario: Truncate transform on string preserves range filter results
      Given statement template
        """
        CREATE TABLE truncate_test (
          id INT,
          code STRING,
          value DOUBLE
        )
        USING iceberg
        PARTITIONED BY (truncate(3, code))
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO truncate_test VALUES
          (1, 'ABC100', 10.0),
          (2, 'ABC200', 20.0),
          (3, 'DEF100', 30.0),
          (4, 'DEF200', 40.0),
          (5, 'XYZ100', 50.0),
          (6, 'XYZ200', 60.0)
        """
      When query
        """
        SELECT id, code FROM truncate_test
        WHERE code >= 'ABC' AND code < 'ABD'
        ORDER BY id
        """
      Then query result ordered
        | id | code   |
        | 1  | ABC100 |
        | 2  | ABC200 |

    Scenario: Truncate transform on integer preserves range filter results
      Given statement template
        """
        CREATE TABLE truncate_test (
          id INT,
          amount INT,
          payload STRING
        )
        USING iceberg
        PARTITIONED BY (truncate(100, amount))
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO truncate_test VALUES
          (1,  50, 'v050'),
          (2, 150, 'v150'),
          (3, 250, 'v250'),
          (4, 350, 'v350')
        """
      When query
        """
        SELECT id, payload FROM truncate_test
        WHERE amount >= 100 AND amount < 200
        ORDER BY id
        """
      Then query result ordered
        | id | payload |
        | 2  | v150    |

  Rule: Date transforms extract temporal components
    Background:
      Given variable location for temporary directory iceberg_date_transform
      Given final statement
        """
        DROP TABLE IF EXISTS date_transform_test
        """

    Scenario: Year transform partitions by year
      Given statement template
        """
        CREATE TABLE date_transform_test (
          id INT,
          event_date DATE,
          event_type STRING
        )
        USING iceberg
        PARTITIONED BY (years(event_date))
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO date_transform_test VALUES 
          (1, DATE '2023-06-15', 'signup'),
          (2, DATE '2023-12-20', 'purchase'),
          (3, DATE '2024-01-10', 'login'),
          (4, DATE '2024-06-25', 'view')
        """
      Then iceberg metadata matches snapshot
      Then iceberg snapshot operation is append
      Then file tree in location matches
        """
        📂 data
          📂 event_date_year=2023
            📄 *.parquet
          📂 event_date_year=2024
            📄 *.parquet
        📂 metadata
          📄 *.metadata.json
          📄 snap-*.avro
        """
      When query
        """
        SELECT * FROM date_transform_test 
        WHERE event_date >= DATE '2024-01-01'
        ORDER BY id
        """
      Then query result ordered
        | id | event_date | event_type |
        | 3  | 2024-01-10 | login      |
        | 4  | 2024-06-25 | view       |

    Scenario: Month transform partitions by year and month
      Given statement template
        """
        CREATE TABLE month_partition_test (
          id INT,
          created_at DATE,
          status STRING
        )
        USING iceberg
        PARTITIONED BY (months(created_at))
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO month_partition_test VALUES 
          (1, DATE '2024-01-15', 'active'),
          (2, DATE '2024-01-20', 'pending'),
          (3, DATE '2024-02-10', 'completed'),
          (4, DATE '2024-03-05', 'cancelled')
        """
      Then iceberg metadata matches snapshot
      Then iceberg current snapshot summary matches snapshot
      When query
        """
        SELECT * FROM month_partition_test 
        WHERE created_at >= DATE '2024-02-01'
        ORDER BY id
        """
      Then query result ordered
        | id | created_at | status    |
        | 3  | 2024-02-10 | completed |
        | 4  | 2024-03-05 | cancelled |

    Scenario: Day transform partitions by full date
      Given statement template
        """
        CREATE TABLE day_partition_test (
          id INT,
          event_date DATE,
          metric INT
        )
        USING iceberg
        PARTITIONED BY (days(event_date))
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO day_partition_test VALUES 
          (1, DATE '2024-01-01', 100),
          (2, DATE '2024-01-01', 200),
          (3, DATE '2024-01-02', 300),
          (4, DATE '2024-01-03', 400)
        """
      Then iceberg metadata matches snapshot
      Then iceberg snapshot count is 1
      When query
        """
        SELECT * FROM day_partition_test 
        WHERE event_date = DATE '2024-01-01'
        ORDER BY id
        """
      Then query result ordered
        | id | event_date | metric |
        | 1  | 2024-01-01 | 100    |
        | 2  | 2024-01-01 | 200    |

    Scenario: Year transform on timestamp preserves range filter results
      Given statement template
        """
        CREATE TABLE date_transform_test (
          id INT,
          ts TIMESTAMP,
          payload STRING
        )
        USING iceberg
        PARTITIONED BY (years(ts))
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO date_transform_test VALUES
          (1, TIMESTAMP '2023-03-01 00:00:00', 'y2023-a'),
          (2, TIMESTAMP '2023-09-01 00:00:00', 'y2023-b'),
          (3, TIMESTAMP '2024-03-01 00:00:00', 'y2024-a'),
          (4, TIMESTAMP '2024-09-01 00:00:00', 'y2024-b'),
          (5, TIMESTAMP '2025-03-01 00:00:00', 'y2025-a'),
          (6, TIMESTAMP '2025-09-01 00:00:00', 'y2025-b')
        """
      When query
        """
        SELECT id, payload FROM date_transform_test
        WHERE ts >= TIMESTAMP '2024-01-01 00:00:00'
          AND ts < TIMESTAMP '2025-01-01 00:00:00'
        ORDER BY id
        """
      Then query result ordered
        | id | payload |
        | 3  | y2024-a |
        | 4  | y2024-b |

    Scenario: Month transform on timestamp preserves range filter results
      Given statement template
        """
        CREATE TABLE date_transform_test (
          id INT,
          ts TIMESTAMP,
          payload STRING
        )
        USING iceberg
        PARTITIONED BY (months(ts))
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO date_transform_test VALUES
          (1, TIMESTAMP '2024-01-10 00:00:00', 'jan-a'),
          (2, TIMESTAMP '2024-01-25 00:00:00', 'jan-b'),
          (3, TIMESTAMP '2024-02-10 00:00:00', 'feb-a'),
          (4, TIMESTAMP '2024-02-25 00:00:00', 'feb-b'),
          (5, TIMESTAMP '2024-03-10 00:00:00', 'mar-a'),
          (6, TIMESTAMP '2024-03-25 00:00:00', 'mar-b')
        """
      When query
        """
        SELECT id, payload FROM date_transform_test
        WHERE ts >= TIMESTAMP '2024-02-01 00:00:00'
          AND ts < TIMESTAMP '2024-03-01 00:00:00'
        ORDER BY id
        """
      Then query result ordered
        | id | payload |
        | 3  | feb-a   |
        | 4  | feb-b   |

  Rule: Timestamp transforms handle time-based partitioning
    Background:
      Given variable location for temporary directory iceberg_timestamp_transform
      Given final statement
        """
        DROP TABLE IF EXISTS timestamp_transform_test
        """

    Scenario: Hour transform partitions by hour
      Given statement template
        """
        CREATE TABLE timestamp_transform_test (
          id INT,
          event_time TIMESTAMP,
          event_name STRING
        )
        USING iceberg
        PARTITIONED BY (hours(event_time))
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO timestamp_transform_test VALUES 
          (1, TIMESTAMP '2024-01-01 10:00:00', 'start'),
          (2, TIMESTAMP '2024-01-01 10:30:00', 'progress'),
          (3, TIMESTAMP '2024-01-01 11:00:00', 'milestone'),
          (4, TIMESTAMP '2024-01-01 12:00:00', 'complete')
        """
      Then iceberg metadata matches snapshot
      Then iceberg snapshot operation is append
      When query
        """
        SELECT id, event_name FROM timestamp_transform_test 
        WHERE hour(event_time) = 10
        ORDER BY id
        """
      Then query result ordered
        | id | event_name |
        | 1  | start      |
        | 2  | progress   |

    Scenario: Day transform on timestamp preserves range filter results
      Given statement template
        """
        CREATE TABLE timestamp_transform_test (
          id INT,
          payload_timestamp TIMESTAMP,
          payload STRING
        )
        USING iceberg
        PARTITIONED BY (days(payload_timestamp))
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO timestamp_transform_test VALUES
          (1, TIMESTAMP '2024-01-01 10:00:00', 'day1-a'),
          (2, TIMESTAMP '2024-01-01 11:00:00', 'day1-b'),
          (3, TIMESTAMP '2024-01-02 10:00:00', 'day2-a'),
          (4, TIMESTAMP '2024-01-02 11:00:00', 'day2-b'),
          (5, TIMESTAMP '2024-01-03 10:00:00', 'day3-a'),
          (6, TIMESTAMP '2024-01-03 11:00:00', 'day3-b')
        """
      When query
        """
        SELECT id, payload FROM timestamp_transform_test
        WHERE payload_timestamp >= TIMESTAMP '2024-01-02 00:00:00'
          AND payload_timestamp < TIMESTAMP '2024-01-03 00:00:00'
        ORDER BY id
        """
      Then query result ordered
        | id | payload |
        | 3  | day2-a  |
        | 4  | day2-b  |

    Scenario: Hour transform on timestamp preserves range filter results
      Given statement template
        """
        CREATE TABLE timestamp_transform_test (
          id INT,
          ts TIMESTAMP,
          payload STRING
        )
        USING iceberg
        PARTITIONED BY (hours(ts))
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO timestamp_transform_test VALUES
          (1, TIMESTAMP '2024-01-01 10:00:00', 'h10-a'),
          (2, TIMESTAMP '2024-01-01 10:30:00', 'h10-b'),
          (3, TIMESTAMP '2024-01-01 11:00:00', 'h11-a'),
          (4, TIMESTAMP '2024-01-01 11:30:00', 'h11-b'),
          (5, TIMESTAMP '2024-01-01 12:00:00', 'h12-a'),
          (6, TIMESTAMP '2024-01-01 12:30:00', 'h12-b')
        """
      When query
        """
        SELECT id, payload FROM timestamp_transform_test
        WHERE ts >= TIMESTAMP '2024-01-01 11:00:00'
          AND ts < TIMESTAMP '2024-01-01 12:00:00'
        ORDER BY id
        """
      Then query result ordered
        | id | payload |
        | 3  | h11-a   |
        | 4  | h11-b   |

  Rule: Multi-column transform creates nested partitions
    Background:
      Given variable location for temporary directory iceberg_multi_transform
      Given final statement
        """
        DROP TABLE IF EXISTS multi_transform_test
        """

    Scenario: Combined year and category partitioning
      Given statement template
        """
        CREATE TABLE multi_transform_test (
          id INT,
          event_date DATE,
          category STRING,
          value DOUBLE
        )
        USING iceberg
        PARTITIONED BY (years(event_date), category)
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO multi_transform_test VALUES 
          (1, DATE '2023-06-15', 'A', 10.0),
          (2, DATE '2023-12-20', 'B', 20.0),
          (3, DATE '2024-01-10', 'A', 30.0),
          (4, DATE '2024-06-25', 'B', 40.0)
        """
      Then iceberg metadata matches snapshot
      Then iceberg partition spec matches snapshot
      Then file tree in location matches
        """
        📂 data
          📂 event_date_year=2023
            📂 category=A
              📄 *.parquet
            📂 category=B
              📄 *.parquet
          📂 event_date_year=2024
            📂 category=A
              📄 *.parquet
            📂 category=B
              📄 *.parquet
        📂 metadata
          📄 *.metadata.json
          📄 snap-*.avro
        """
      When query
        """
        SELECT * FROM multi_transform_test 
        WHERE years(event_date) = 2024 AND category = 'A'
        ORDER BY id
        """
      Then query result ordered
        | id | event_date | category | value |
        | 3  | 2024-01-10 | A        | 30.0  |

  Rule: Partition evolution changes transform strategy
    Background:
      Given variable location for temporary directory iceberg_partition_evolution
      Given final statement
        """
        DROP TABLE IF EXISTS partition_evolution_test
        """
