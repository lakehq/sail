Feature: Delta Lake CREATE OR REPLACE TABLE

  @sail-only
  Rule: CREATE OR REPLACE TABLE tombstones existing data in the Delta log

    Background:
      Given variable location for temporary directory delta_create_or_replace
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_create_or_replace_test
        """
      Given statement template
        """
        CREATE TABLE delta_create_or_replace_test (id INT, value STRING)
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_create_or_replace_test VALUES (1, 'a'), (2, 'b')
        """

    Scenario: CREATE OR REPLACE TABLE on an existing Delta location removes all existing data
      Given statement template
        """
        CREATE OR REPLACE TABLE delta_create_or_replace_test (id INT, value STRING)
        USING DELTA
        LOCATION {{ location.sql }}
        """
      When query
        """
        SELECT * FROM delta_create_or_replace_test
        """
      Then query result
        | id | value |
      Then file tree in delta_log matches
        """
        📄 00000000000000000000.crc
        📄 00000000000000000000.json
        📄 00000000000000000001.crc
        📄 00000000000000000001.json
        📄 00000000000000000002.crc
        📄 00000000000000000002.json
        """

    Scenario: After CREATE OR REPLACE TABLE, new writes start from the replaced empty state
      Given statement template
        """
        CREATE OR REPLACE TABLE delta_create_or_replace_test (id INT, value STRING)
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_create_or_replace_test VALUES (10, 'x')
        """
      When query
        """
        SELECT * FROM delta_create_or_replace_test ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 10 | x     |

  @sail-only
  Rule: CREATE OR REPLACE TABLE records Delta operation metadata

    Scenario: CREATE OR REPLACE TABLE on a new Delta location records overwrite mode
      Given variable location for temporary directory delta_create_or_replace_new
      Given final statement
        """
        DROP TABLE IF EXISTS delta_create_or_replace_new_test
        """
      Given statement template
        """
        CREATE OR REPLACE TABLE delta_create_or_replace_new_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Then delta log latest commit info contains
        | path                     | value                       |
        | operation                | "CREATE OR REPLACE TABLE"   |
        | operationParameters.mode | "Overwrite"                 |

    Scenario: CREATE OR REPLACE TABLE preserves the existing Delta protocol
      Given variable location for temporary directory delta_create_or_replace_protocol
      Given final statement
        """
        DROP TABLE IF EXISTS delta_create_or_replace_protocol_test
        """
      Given statement template
        """
        CREATE TABLE delta_create_or_replace_protocol_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.feature.appendOnly' = 'supported')
        """
      Given statement template
        """
        CREATE OR REPLACE TABLE delta_create_or_replace_protocol_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Then delta log latest effective protocol and metadata contains
        | path                       | value          |
        | protocol.minWriterVersion  | 7              |
        | protocol.writerFeatures    | ["appendOnly"] |

  @sail-only
  Rule: CREATE TABLE schema materialization

    Scenario: STRUCT with Variant physical layout is not marked as Variant
      Given variable location for temporary directory delta_create_table_variant_shape
      Given final statement
        """
        DROP TABLE IF EXISTS delta_create_table_variant_shape_test
        """
      Given statement template
        """
        CREATE TABLE delta_create_table_variant_shape_test (
          payload STRUCT<metadata: BINARY NOT NULL, value: BINARY NOT NULL>
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Then delta log first commit protocol and metadata contains
        | path                      | value |
        | protocol.minReaderVersion | 1     |
        | protocol.minWriterVersion | 2     |
