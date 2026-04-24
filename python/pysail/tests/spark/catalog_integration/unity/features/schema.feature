Feature: Unity Catalog schema (database) operations

  Scenario: Create a schema with metadata
    Given statement
      """
      CREATE SCHEMA test_create_schema
      COMMENT 'test comment'
      WITH DBPROPERTIES (key1 = 'value1')
      """
    Given final statement
      """
      DROP SCHEMA IF EXISTS test_create_schema
      """
    When query
      """
      SHOW SCHEMAS LIKE 'sail_test_catalog.test_create_schema'
      """
    Then query result
      | name                                  | catalog | description  | locationUri |
      | sail_test_catalog.test_create_schema  | sail    | test comment | NULL        |

  Scenario: Create duplicate schema fails
    Given statement
      """
      CREATE SCHEMA dup_schema_unity
      """
    Given final statement
      """
      DROP SCHEMA IF EXISTS dup_schema_unity
      """
    Given statement with error .*
      """
      CREATE SCHEMA dup_schema_unity
      """

  Scenario: Create schema with IF NOT EXISTS does not raise error
    Given statement
      """
      CREATE SCHEMA ine_schema_unity
      """
    Given final statement
      """
      DROP SCHEMA IF EXISTS ine_schema_unity
      """
    Given statement
      """
      CREATE SCHEMA IF NOT EXISTS ine_schema_unity COMMENT 'should be ignored'
      """

  Scenario: Create schema with IF NOT EXISTS preserves original metadata
    Given statement
      """
      CREATE SCHEMA ine_keep_meta_unity COMMENT 'original comment'
      """
    Given final statement
      """
      DROP SCHEMA IF EXISTS ine_keep_meta_unity
      """
    Given statement
      """
      CREATE SCHEMA IF NOT EXISTS ine_keep_meta_unity COMMENT 'should be ignored'
      """
    When query
      """
      SHOW SCHEMAS LIKE 'sail_test_catalog.ine_keep_meta_unity'
      """
    Then query result
      | name                                  | catalog | description      | locationUri |
      | sail_test_catalog.ine_keep_meta_unity | sail    | original comment | NULL        |

  Scenario: Create schema with LOCATION
    Given statement
      """
      CREATE SCHEMA schema_with_location_unity
      COMMENT 'with location'
      LOCATION 's3://bucket/path'
      """
    Given final statement
      """
      DROP SCHEMA IF EXISTS schema_with_location_unity
      """
    When query
      """
      SHOW SCHEMAS LIKE 'sail_test_catalog.schema_with_location_unity'
      """
    Then query result
      | name                                       | catalog | description   | locationUri      |
      | sail_test_catalog.schema_with_location_unity | sail    | with location | s3://bucket/path |

  Scenario: Non-existent schema does not appear in listing
    When query
      """
      SHOW SCHEMAS LIKE 'nonexistent_schema_unity'
      """
    Then query result
      | name | catalog | description | locationUri |

  Scenario: Created schema appears in listing
    Given statement
      """
      CREATE SCHEMA get_schema_unity
      WITH DBPROPERTIES (owner = 'Lake', community = 'Sail')
      """
    Given final statement
      """
      DROP SCHEMA IF EXISTS get_schema_unity
      """
    When query
      """
      SHOW SCHEMAS LIKE 'sail_test_catalog.get_schema_unity'
      """
    Then query result
      | name                              | catalog | description | locationUri |
      | sail_test_catalog.get_schema_unity | sail    | NULL        | NULL        |

  Scenario: List multiple schemas
    Given statement
      """
      CREATE SCHEMA IF NOT EXISTS list_ios_unity
      """
    Given statement
      """
      CREATE SCHEMA IF NOT EXISTS list_macos_unity
      """
    Given final statement
      """
      DROP SCHEMA IF EXISTS list_ios_unity
      """
    Given final statement
      """
      DROP SCHEMA IF EXISTS list_macos_unity
      """
    When query
      """
      SHOW SCHEMAS LIKE 'sail_test_catalog.list_ios_unity'
      """
    Then query result
      | name                            | catalog | description | locationUri |
      | sail_test_catalog.list_ios_unity | sail    | NULL        | NULL        |
    When query
      """
      SHOW SCHEMAS LIKE 'sail_test_catalog.list_macos_unity'
      """
    Then query result
      | name                              | catalog | description | locationUri |
      | sail_test_catalog.list_macos_unity | sail    | NULL        | NULL        |

  Scenario: Drop existing schema removes it
    Given statement
      """
      CREATE SCHEMA drop_schema_unity
      """
    Given final statement
      """
      DROP SCHEMA IF EXISTS drop_schema_unity
      """
    Given statement
      """
      DROP SCHEMA drop_schema_unity
      """
    When query
      """
      SHOW SCHEMAS LIKE 'drop_schema_unity'
      """
    Then query result
      | name | catalog | description | locationUri |

  Scenario: Drop non-existent schema fails
    Given statement with error .*
      """
      DROP SCHEMA nonexistent_drop_schema_unity
      """

  Scenario: Drop non-existent schema with IF EXISTS does not raise error
    Given statement
      """
      DROP SCHEMA IF EXISTS nonexistent_drop_schema_unity
      """

  Scenario: Drop schema with CASCADE removes schema and its tables
    Given statement
      """
      CREATE SCHEMA cascade_drop_unity
      """
    Given final statement
      """
      DROP SCHEMA IF EXISTS cascade_drop_unity CASCADE
      """
    Given statement
      """
      CREATE TABLE cascade_drop_unity.t1 (id INT)
      USING delta
      LOCATION 's3://deltadata/cascade_test'
      """
    Given statement
      """
      DROP SCHEMA cascade_drop_unity CASCADE
      """
    When query
      """
      SHOW SCHEMAS LIKE 'cascade_drop_unity'
      """
    Then query result
      | name | catalog | description | locationUri |

  Scenario: Describe non-existent schema raises error
    When query
      """
      DESCRIBE SCHEMA nonexistent_describe_schema
      """
    Then query error .*

  Scenario: Describe an existing schema
    Given statement
      """
      CREATE SCHEMA describe_schema_unity
      COMMENT 'describe test'
      """
    Given final statement
      """
      DROP SCHEMA IF EXISTS describe_schema_unity
      """
    When query
      """
      DESCRIBE SCHEMA describe_schema_unity
      """
    Then query result has row where "info_name" is "Namespace Name"
    Then query result row where "info_name" is "Namespace Name" has "info_value" containing "describe_schema_unity"
    Then query result row where "info_name" is "Comment" has "info_value" equal to "describe test"

  Scenario: Drop non-empty schema without CASCADE fails
    Given statement
      """
      CREATE SCHEMA no_cascade_drop_unity
      """
    Given final statement
      """
      DROP SCHEMA IF EXISTS no_cascade_drop_unity CASCADE
      """
    Given statement
      """
      CREATE TABLE no_cascade_drop_unity.t1 (id INT)
      USING delta
      LOCATION 's3://deltadata/no_cascade_test'
      """
    Given statement with error .*
      """
      DROP SCHEMA no_cascade_drop_unity
      """

  Scenario: Drop empty schema without CASCADE succeeds
    Given statement
      """
      CREATE SCHEMA empty_drop_unity
      """
    Given statement
      """
      DROP SCHEMA empty_drop_unity
      """
    When query
      """
      SHOW SCHEMAS LIKE 'sail_test_catalog.empty_drop_unity'
      """
    Then query result
      | name | catalog | description | locationUri |

  Scenario: Create schema without COMMENT or DBPROPERTIES
    Given statement
      """
      CREATE SCHEMA minimal_schema_unity
      """
    Given final statement
      """
      DROP SCHEMA IF EXISTS minimal_schema_unity
      """
    When query
      """
      SHOW SCHEMAS LIKE 'sail_test_catalog.minimal_schema_unity'
      """
    Then query result
      | name                                  | catalog | description | locationUri |
      | sail_test_catalog.minimal_schema_unity | sail    | NULL        | NULL        |
