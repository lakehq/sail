Feature: Iceberg REST catalog namespace (database) operations

  Scenario: Create a namespace with metadata
    Given statement
      """
      CREATE DATABASE test_create_ns
      COMMENT 'test comment'
      LOCATION 's3://bucket/path'
      WITH DBPROPERTIES (key1 = 'value1')
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS test_create_ns
      """
    When query
      """
      SHOW DATABASES LIKE 'test_create_ns'
      """
    Then query result
      | name           | catalog | description | locationUri |
      | test_create_ns | sail    | NULL        | NULL        |

  Scenario: Create duplicate namespace fails
    Given statement
      """
      CREATE DATABASE dup_ns_iceberg
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS dup_ns_iceberg
      """
    Given statement with error .*
      """
      CREATE DATABASE dup_ns_iceberg
      """

  Scenario: Create namespace with IF NOT EXISTS does not raise error
    Given statement
      """
      CREATE DATABASE ine_ns_iceberg
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS ine_ns_iceberg
      """
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS ine_ns_iceberg COMMENT 'should be ignored'
      """

  Scenario: Create namespace with IF NOT EXISTS preserves original metadata
    Given statement
      """
      CREATE DATABASE ine_keep_ns_iceberg
      WITH DBPROPERTIES (owner = 'original_owner')
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS ine_keep_ns_iceberg
      """
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS ine_keep_ns_iceberg
      WITH DBPROPERTIES (owner = 'should_be_ignored')
      """
    When query
      """
      SHOW DATABASES LIKE 'ine_keep_ns_iceberg'
      """
    Then query result
      | name                | catalog | description | locationUri |
      | ine_keep_ns_iceberg | sail    | NULL        | NULL        |

  Scenario: Listing child namespaces of an empty parent returns no rows
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS empty_parent_ns
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS empty_parent_ns
      """
    When query
      """
      SHOW DATABASES LIKE 'empty_parent_ns.%'
      """
    Then query result
      | name | catalog | description | locationUri |

  Scenario: Non-existent namespace does not appear in listing
    When query
      """
      SHOW DATABASES LIKE 'nonexistent_ns_iceberg'
      """
    Then query result
      | name | catalog | description | locationUri |

  Scenario: Created namespace appears in listing
    Given statement
      """
      CREATE DATABASE get_ns_iceberg
      WITH DBPROPERTIES (owner = 'Lake', community = 'Sail')
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS get_ns_iceberg
      """
    When query
      """
      SHOW DATABASES LIKE 'get_ns_iceberg'
      """
    Then query result
      | name           | catalog | description | locationUri |
      | get_ns_iceberg | sail    | NULL        | NULL        |

  Scenario: Multiple namespaces all appear in listing
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS ns_apple
      """
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS ns_ios
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS ns_apple
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS ns_ios
      """
    When query
      """
      SHOW DATABASES LIKE 'ns_apple'
      """
    Then query result
      | name     | catalog | description | locationUri |
      | ns_apple | sail    | NULL        | NULL        |
    When query
      """
      SHOW DATABASES LIKE 'ns_ios'
      """
    Then query result
      | name   | catalog | description | locationUri |
      | ns_ios | sail    | NULL        | NULL        |

  Scenario: List multiple namespaces
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS list_ns_one
      """
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS list_ns_two
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS list_ns_one
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS list_ns_two
      """
    When query
      """
      SHOW DATABASES LIKE 'list_ns_one'
      """
    Then query result
      | name        | catalog | description | locationUri |
      | list_ns_one | sail    | NULL        | NULL        |
    When query
      """
      SHOW DATABASES LIKE 'list_ns_two'
      """
    Then query result
      | name        | catalog | description | locationUri |
      | list_ns_two | sail    | NULL        | NULL        |

  Scenario: Listing with pattern matching returns empty when no match
    When query
      """
      SHOW DATABASES LIKE 'never_created_ns_%'
      """
    Then query result
      | name | catalog | description | locationUri |

  Scenario: Created namespace appears in root listing
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS root_test_ns
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS root_test_ns
      """
    When query
      """
      SHOW DATABASES LIKE 'root_test_ns'
      """
    Then query result
      | name         | catalog | description | locationUri |
      | root_test_ns | sail    | NULL        | NULL        |

  Scenario: Drop existing namespace removes it
    Given statement
      """
      CREATE DATABASE drop_ns_iceberg
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS drop_ns_iceberg
      """
    Given statement
      """
      DROP DATABASE drop_ns_iceberg
      """
    When query
      """
      SHOW DATABASES LIKE 'drop_ns_iceberg'
      """
    Then query result
      | name | catalog | description | locationUri |

  Scenario: Drop non-existent namespace fails
    Given statement with error .*
      """
      DROP DATABASE nonexistent_drop_ns_iceberg
      """

  Scenario: Drop non-existent namespace with IF EXISTS does not raise error
    Given statement
      """
      DROP DATABASE IF EXISTS nonexistent_drop_ns_iceberg
      """

  Scenario: Describe an existing namespace
    Given statement
      """
      CREATE DATABASE describe_ns_iceberg
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS describe_ns_iceberg
      """
    When query
      """
      DESCRIBE DATABASE describe_ns_iceberg
      """
    Then query result ordered
      | info_name      | info_value                            |
      | Namespace Name | describe_ns_iceberg                   |
      | Comment        |                                       |
      | Location       | s3://icebergdata/demo/describe_ns_iceberg |

  Scenario: Describe non-existent namespace raises error
    When query
      """
      DESCRIBE DATABASE nonexistent_describe_ns
      """
    Then query error .*

  Scenario: DBPROPERTIES round-trip through DESCRIBE DATABASE EXTENDED
    Given statement
      """
      CREATE DATABASE props_ns_iceberg
      WITH DBPROPERTIES (owner = 'Lake', team = 'data-eng')
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS props_ns_iceberg
      """
    When query
      """
      DESCRIBE DATABASE EXTENDED props_ns_iceberg
      """
    Then query result row where "info_name" is "Namespace Name" has "info_value" equal to "props_ns_iceberg"
    Then query result row where "info_name" is "Properties" has "info_value" containing "owner,Lake"
    Then query result row where "info_name" is "Properties" has "info_value" containing "team,data-eng"

  Scenario: SHOW DATABASES without filter lists created namespaces
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS all_ns_iceberg_a
      """
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS all_ns_iceberg_b
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS all_ns_iceberg_a
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS all_ns_iceberg_b
      """
    When query
      """
      SHOW DATABASES
      """
    Then query result has row where "name" is "all_ns_iceberg_a"
    Then query result has row where "name" is "all_ns_iceberg_b"

  Scenario: Create a child namespace under a parent
    Given statement
      """
      CREATE DATABASE multi_parent_ns
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS multi_parent_ns CASCADE
      """
    Given statement
      """
      CREATE DATABASE multi_parent_ns.child_a
      """
    When query
      """
      DESCRIBE DATABASE multi_parent_ns.child_a
      """
    Then query result row where "info_name" is "Namespace Name" has "info_value" equal to "multi_parent_ns.child_a"

  Scenario: Describe a multi-level namespace
    Given statement
      """
      CREATE DATABASE multi_desc_ns
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS multi_desc_ns CASCADE
      """
    Given statement
      """
      CREATE DATABASE multi_desc_ns.leaf
      WITH DBPROPERTIES (owner = 'Lake')
      """
    When query
      """
      DESCRIBE DATABASE EXTENDED multi_desc_ns.leaf
      """
    Then query result row where "info_name" is "Namespace Name" has "info_value" equal to "multi_desc_ns.leaf"
    Then query result row where "info_name" is "Properties" has "info_value" containing "owner,Lake"

  Scenario: Drop a child namespace without affecting the parent
    Given statement
      """
      CREATE DATABASE multi_drop_ns
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS multi_drop_ns CASCADE
      """
    Given statement
      """
      CREATE DATABASE multi_drop_ns.temp
      """
    Given statement
      """
      DROP DATABASE multi_drop_ns.temp
      """
    When query
      """
      SHOW DATABASES LIKE 'multi_drop_ns'
      """
    Then query result has row where "name" is "multi_drop_ns"
    When query
      """
      SHOW DATABASES LIKE 'multi_drop_ns.%'
      """
    Then query result
      | name | catalog | description | locationUri |
