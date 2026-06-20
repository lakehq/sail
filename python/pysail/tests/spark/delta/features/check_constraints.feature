Feature: Delta Lake CHECK Constraints

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
      Given statement with error DELTA_VIOLATE_CONSTRAINT_WITH_VALUES
        """
        INSERT INTO delta_check_constraints_test VALUES (0, 'bad')
        """
      Given statement with error DELTA_VIOLATE_CONSTRAINT_WITH_VALUES
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

    Scenario: ADD CONSTRAINT validates existing data and enforces future writes
      Given statement template
        """
        CREATE TABLE delta_check_constraints_test (
          id INT,
          value STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_check_constraints_test VALUES (1, 'ok')
        """
      Given statement
        """
        ALTER TABLE delta_check_constraints_test
        ADD CONSTRAINT positive_id CHECK (id > 0)
        """
      Given statement with error DELTA_VIOLATE_CONSTRAINT_WITH_VALUES
        """
        INSERT INTO delta_check_constraints_test VALUES (0, 'bad')
        """
      Then delta log latest effective protocol and metadata matches snapshot for paths
        | path                                                   |
        | protocol.minWriterVersion                              |
        | metaData.configuration['delta.constraints.positive_id'] |

    Scenario: ADD CONSTRAINT rejects existing violating data
      Given statement template
        """
        CREATE TABLE delta_check_constraints_test (
          id INT,
          value STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_check_constraints_test VALUES (0, 'bad')
        """
      Given statement with error DELTA_NEW_CHECK_CONSTRAINT_VIOLATION
        """
        ALTER TABLE delta_check_constraints_test
        ADD CONSTRAINT positive_id CHECK (id > 0)
        """
      When query
        """
        SELECT id, value FROM delta_check_constraints_test
        """
      Then query result ordered
        | id | value |
        | 0  | bad   |

    Scenario: Direct mutation of delta.constraints table properties is rejected
      Given statement template
        """
        CREATE TABLE delta_check_constraints_test (
          id INT,
          value STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_check_constraints_test VALUES (1, 'ok')
        """
      Given statement with error DELTA_ADD_CONSTRAINTS
        """
        ALTER TABLE delta_check_constraints_test
        SET TBLPROPERTIES ('delta.constraints.positive_id' = 'id > 0')
        """

    Scenario: NOT NULL constraints reject nulls and persist non-nullable schema
      Given statement template
        """
        CREATE TABLE delta_check_constraints_test (
          id INT NOT NULL,
          value STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_check_constraints_test VALUES (1, 'ok')
        """
      Given statement with error DELTA_NOT_NULL_CONSTRAINT_VIOLATED
        """
        INSERT INTO delta_check_constraints_test VALUES (NULL, 'bad')
        """
      Then delta log first commit protocol and metadata matches snapshot for paths
        | path                                    |
        | protocol.minWriterVersion               |
        | metaData.schemaString.fields[0].name    |
        | metaData.schemaString.fields[0].nullable |

  Rule: CHECK constraints are recorded in Delta protocol metadata

    Background:
      Given variable location for temporary directory delta_check_constraints_metadata
      Given final statement
        """
        DROP TABLE IF EXISTS delta_check_constraints_metadata_test
        """

    Scenario: First commit contains the legacy CHECK constraints writer protocol and constraint property
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
      Then delta log first commit protocol and metadata matches snapshot for paths
        | path                                                   |
        | protocol.minWriterVersion                              |
        | metaData.configuration['delta.constraints.positive_id'] |

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
      Then query error DELTA_VIOLATE_CONSTRAINT_WITH_VALUES
