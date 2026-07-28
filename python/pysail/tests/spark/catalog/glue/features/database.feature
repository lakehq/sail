Feature: Glue catalog database operations

  Scenario: Create a database with metadata
    Given statement
      """
      CREATE DATABASE test_create_db
      COMMENT 'test comment'
      LOCATION 's3://bucket/path'
      WITH DBPROPERTIES (key1 = 'value1')
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS test_create_db
      """
    When query
      """
      SHOW DATABASES LIKE 'test_create_db'
      """
    Then query result
      | name           | catalog | description  | locationUri      |
      | test_create_db | sail    | test comment | s3://bucket/path |
    When query
      """
      DESCRIBE DATABASE EXTENDED test_create_db
      """
    Then query result ordered
      | info_name      | info_value       |
      | Namespace Name | test_create_db   |
      | Comment        | test comment     |
      | Location       | s3://bucket/path |
      | Properties     | ((key1,value1))  |

  Scenario: Create duplicate database fails
    Given statement
      """
      CREATE DATABASE dup_db
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS dup_db
      """
    Given statement with error .*
      """
      CREATE DATABASE dup_db
      """

  Scenario: Create database with IF NOT EXISTS does not raise error
    Given statement
      """
      CREATE DATABASE ine_db COMMENT 'original'
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS ine_db
      """
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS ine_db COMMENT 'new comment'
      """
    When query
      """
      SHOW DATABASES LIKE 'ine_db'
      """
    Then query result
      | name   | catalog | description | locationUri |
      | ine_db | sail    | original    | NULL        |

  Scenario: Non-existent database does not appear in listing
    When query
      """
      SHOW DATABASES LIKE 'nonexistent_db_glue'
      """
    Then query result
      | name | catalog | description | locationUri |

  Scenario: Get an existing database
    Given statement
      """
      CREATE DATABASE get_test_db
      COMMENT 'Get test description'
      LOCATION 's3://bucket/get-test'
      WITH DBPROPERTIES (owner = 'test_user', team = 'data_eng')
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS get_test_db
      """
    When query
      """
      SHOW DATABASES LIKE 'get_test_db'
      """
    Then query result
      | name        | catalog | description          | locationUri          |
      | get_test_db | sail    | Get test description | s3://bucket/get-test |

  Scenario: Drop non-existent database fails
    Given statement with error .*
      """
      DROP DATABASE nonexistent_drop_db
      """

  Scenario: Drop non-existent database with IF EXISTS does not raise error
    Given statement
      """
      DROP DATABASE IF EXISTS nonexistent_drop_db
      """

  Scenario: Drop existing database removes it
    Given statement
      """
      CREATE DATABASE drop_target_db COMMENT 'To be dropped'
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS drop_target_db
      """
    Given statement
      """
      DROP DATABASE drop_target_db
      """
    When query
      """
      SHOW DATABASES LIKE 'drop_target_db'
      """
    Then query result
      | name | catalog | description | locationUri |

  Scenario: List multiple databases
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS list_db_one
      """
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS list_db_two
      """
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS list_other_db
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS list_db_one
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS list_db_two
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS list_other_db
      """
    When query
      """
      SHOW DATABASES LIKE 'list_db_one'
      """
    Then query result
      | name        | catalog | description | locationUri |
      | list_db_one | sail    | NULL        | NULL        |
    When query
      """
      SHOW DATABASES LIKE 'list_db_two'
      """
    Then query result
      | name        | catalog | description | locationUri |
      | list_db_two | sail    | NULL        | NULL        |
    When query
      """
      SHOW DATABASES LIKE 'list_other_db'
      """
    Then query result
      | name          | catalog | description | locationUri |
      | list_other_db | sail    | NULL        | NULL        |

  Scenario: Database with only properties and no location is retrievable
    Given statement
      """
      CREATE DATABASE props_only_db
      WITH DBPROPERTIES (env = 'test', team = 'data')
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS props_only_db
      """
    When query
      """
      DESCRIBE DATABASE EXTENDED props_only_db
      """
    Then query result row where "info_name" is "Namespace Name" has "info_value" equal to "props_only_db"
    Then query result row where "info_name" is "Properties" has "info_value" containing "env,test"
    Then query result row where "info_name" is "Properties" has "info_value" containing "team,data"

  Scenario: Dropping a non-empty database with CASCADE removes it
    Given statement
      """
      CREATE DATABASE cascade_drop_db
      """
    Given statement
      """
      CREATE TABLE cascade_drop_db.inner_tbl (id INT)
      USING parquet
      LOCATION 's3://bucket/cascade_drop_db_inner'
      """
    Given statement
      """
      DROP DATABASE cascade_drop_db CASCADE
      """
    When query
      """
      SHOW DATABASES LIKE 'cascade_drop_db'
      """
    Then query result
      | name | catalog | description | locationUri |
