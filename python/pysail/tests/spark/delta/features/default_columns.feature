Feature: Delta Lake Default Columns

  Rule: Default columns are materialized during inserts

    Background:
      Given variable location for temporary directory default_col_basic
      Given final statement
        """
        DROP TABLE IF EXISTS delta_default_col_basic
        """

    Scenario: Insert omitted columns and explicit DEFAULT values
      Given statement template
        """
        CREATE TABLE delta_default_col_basic (
          id INT,
          status STRING DEFAULT 'new',
          qty INT DEFAULT 10
        )
        USING DELTA
        TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_default_col_basic (id)
        VALUES (1), (2)
        """
      Given statement
        """
        INSERT INTO delta_default_col_basic
        VALUES (3, DEFAULT, 5), (4, 'done', DEFAULT)
        """
      When query
        """
        SELECT id, status, qty FROM delta_default_col_basic ORDER BY id
        """
      Then query result ordered
        | id | status | qty |
        | 1  | new    | 10  |
        | 2  | new    | 10  |
        | 3  | new    | 5   |
        | 4  | done   | 10  |

    Scenario: Explicit DEFAULT for a column without a default writes NULL
      Given statement template
        """
        CREATE TABLE delta_default_col_basic (
          id INT,
          note STRING
        )
        USING DELTA
        TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_default_col_basic
        VALUES (1, DEFAULT)
        """
      When query
        """
        SELECT id, note FROM delta_default_col_basic ORDER BY id
        """
      Then query result ordered
        | id | note |
        | 1  | NULL |

  Rule: Defaults can be altered on existing Delta columns

    Background:
      Given variable location for temporary directory default_col_alter
      Given final statement
        """
        DROP TABLE IF EXISTS delta_default_col_alter
        """

    Scenario: Alter column SET DEFAULT and DROP DEFAULT
      Given statement template
        """
        CREATE TABLE delta_default_col_alter (
          id INT,
          status STRING
        )
        USING DELTA
        TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        ALTER TABLE delta_default_col_alter ALTER COLUMN status SET DEFAULT 'set'
        """
      Given statement
        """
        INSERT INTO delta_default_col_alter (id)
        VALUES (1)
        """
      Given statement
        """
        ALTER TABLE delta_default_col_alter ALTER COLUMN status DROP DEFAULT
        """
      Given statement
        """
        INSERT INTO delta_default_col_alter
        VALUES (2, DEFAULT)
        """
      When query
        """
        SELECT id, status FROM delta_default_col_alter ORDER BY id
        """
      Then query result ordered
        | id | status |
        | 1  | set    |
        | 2  | NULL   |

  Rule: Delta log metadata records default column protocol features

    Background:
      Given variable location for temporary directory default_col_meta
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_default_col_meta
        """

    @sail-only
    Scenario: Default columns enable allowColumnDefaults and CURRENT_DEFAULT metadata
      Given statement template
        """
        CREATE TABLE delta_default_col_meta (
          id INT,
          qty INT DEFAULT 10
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_default_col_meta (id)
        VALUES (1)
        """
      Then delta log first commit protocol and metadata contains
        | path                                                 | value                   |
        | protocol.minWriterVersion                            | 7                       |
        | protocol.writerFeatures                              | ["allowColumnDefaults"] |
        | metaData.schemaString.fields[1].metadata.CURRENT_DEFAULT | "10"                    |

  Rule: DEFAULT is restricted to standalone INSERT values

    Background:
      Given variable location for temporary directory default_col_errors
      Given final statement
        """
        DROP TABLE IF EXISTS delta_default_col_errors
        """

    @sail-only
    Scenario: DEFAULT cannot be used inside an expression
      Given statement template
        """
        CREATE TABLE delta_default_col_errors (
          id INT,
          qty INT DEFAULT 10
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      When query
        """
        INSERT INTO delta_default_col_errors
        VALUES (1, DEFAULT + 1)
        """
      Then query error DEFAULT must be a standalone INSERT value
