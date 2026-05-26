Feature: Delta Lake CHECK Constraints

  @sail-only
  Rule: CHECK constraints are enforced on writes

    Background:
      Given variable location for temporary directory delta_check_constraints
      Given final statement
        """
        DROP TABLE IF EXISTS delta_check_constraints_test
        """

    Scenario: INSERT rejects false and null CHECK constraint results
      Given statement template
        """
        CREATE TABLE delta_check_constraints_test (
          id INT,
          value STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.constraints.positive_id' = 'id > 0')
        """
      Given statement
        """
        INSERT INTO delta_check_constraints_test VALUES (1, 'ok')
        """
      Given statement with error DELTA_CHECK_CONSTRAINT_VIOLATED
        """
        INSERT INTO delta_check_constraints_test VALUES (0, 'bad')
        """
      Given statement with error DELTA_CHECK_CONSTRAINT_VIOLATED
        """
        INSERT INTO delta_check_constraints_test VALUES (NULL, 'bad')
        """
      When query
        """
        SELECT id, value FROM delta_check_constraints_test ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | ok    |

  @sail-only
  Rule: CHECK constraints are recorded in Delta protocol metadata

    Background:
      Given variable location for temporary directory delta_check_constraints_metadata
      Given final statement
        """
        DROP TABLE IF EXISTS delta_check_constraints_metadata_test
        """

    Scenario: First commit contains the checkConstraints writer feature and constraint property
      Given statement template
        """
        CREATE TABLE delta_check_constraints_metadata_test (
          id INT,
          value STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.constraints.positive_id' = 'id > 0')
        """
      Given statement
        """
        INSERT INTO delta_check_constraints_metadata_test VALUES (1, 'ok')
        """
      Then delta log first commit protocol and metadata contains
        | path                                                   | value                |
        | protocol.minWriterVersion                              | 7                    |
        | protocol.writerFeatures                                | ["checkConstraints"] |
        | metaData.configuration['delta.constraints.positive_id'] | "id > 0"             |

  @sail-only
  Rule: MERGE respects CHECK constraints

    Background:
      Given variable location for temporary directory delta_check_constraints_merge
      Given final statement
        """
        DROP TABLE IF EXISTS delta_check_constraints_merge_test
        """
      Given statement template
        """
        CREATE TABLE delta_check_constraints_merge_test (
          id INT,
          value STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.constraints.positive_id' = 'id > 0')
        """
      Given statement
        """
        INSERT INTO delta_check_constraints_merge_test VALUES (1, 'existing')
        """

    Scenario: MERGE insert branch rejects rows that violate CHECK constraints
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW src_check_constraints_merge AS
        SELECT * FROM VALUES
          (0, 'bad')
        AS src(id, value)
        """
      When query
        """
        MERGE INTO delta_check_constraints_merge_test AS t
        USING src_check_constraints_merge AS s
        ON t.id = s.id
        WHEN NOT MATCHED THEN INSERT *
        """
      Then query error DELTA_CHECK_CONSTRAINT_VIOLATED
