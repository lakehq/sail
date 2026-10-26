Feature: Database, schema, and namespace aliases

  @sail-only
  Scenario: Creating a nested memory database creates intermediate databases
    Given final statement
      """
      DROP DATABASE IF EXISTS implicit_parent CASCADE
      """
    Given statement
      """
      CREATE DATABASE implicit_parent.child.leaf COMMENT 'leaf database'
      """
    When query
      """
      SHOW DATABASES LIKE 'implicit_parent'
      """
    Then query result
      | name            | catalog | description | locationUri |
      | implicit_parent | sail    | NULL        | NULL        |
    When query
      """
      SHOW DATABASES IN implicit_parent
      """
    Then query result
      | name                  | catalog | description | locationUri |
      | implicit_parent.child | sail    | NULL        | NULL        |
    When query
      """
      SHOW DATABASES IN implicit_parent.child
      """
    Then query result
      | name                       | catalog | description   | locationUri |
      | implicit_parent.child.leaf | sail    | leaf database | NULL        |

  @sail-only
  Scenario: Creating intermediate databases preserves existing database metadata
    Given final statement
      """
      DROP DATABASE IF EXISTS existing_parent CASCADE
      """
    Given statement
      """
      CREATE DATABASE existing_parent COMMENT 'original parent'
      """
    Given statement
      """
      CREATE DATABASE existing_parent.child.leaf COMMENT 'original leaf'
      """
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS existing_parent.child.leaf COMMENT 'ignored comment'
      """
    Given statement with error .*
      """
      CREATE DATABASE existing_parent.child.leaf
      """
    When query
      """
      SHOW DATABASES LIKE 'existing_parent'
      """
    Then query result
      | name            | catalog | description     | locationUri |
      | existing_parent | sail    | original parent | NULL        |
    When query
      """
      SHOW DATABASES IN existing_parent.child
      """
    Then query result
      | name                       | catalog | description   | locationUri |
      | existing_parent.child.leaf | sail    | original leaf | NULL        |

  Scenario Outline: Database, schema, and namespace keywords are interchangeable
    Given statement
      """
      CREATE <create_keyword> <name> COMMENT '<comment>'
      """
    Given final statement
      """
      DROP <drop_keyword> IF EXISTS <name>
      """
    When query
      """
      SHOW <show_keyword> LIKE '<name>'
      """
    Then query result
      | name   | catalog | description | locationUri |
      | <name> | sail    | <comment>   | NULL        |
    When query
      """
      DESCRIBE <describe_keyword> EXTENDED <name>
      """
    Then query result row where "info_name" is "Namespace Name" has "info_value" equal to "<name>"
    Then query result row where "info_name" is "Comment" has "info_value" equal to "<comment>"
    Given statement
      """
      DROP <drop_keyword> <name>
      """
    When query
      """
      SHOW <show_keyword> LIKE '<name>'
      """
    Then query result
      | name | catalog | description | locationUri |

    Examples:
      | create_keyword | show_keyword | describe_keyword | drop_keyword | name              | comment              |
      | DATABASE       | NAMESPACES    | SCHEMA           | NAMESPACE    | alias_database_ns | database via aliases |
      | SCHEMA         | DATABASES     | NAMESPACE        | DATABASE     | alias_schema_ns   | schema via aliases   |
      | NAMESPACE      | SCHEMAS       | DATABASE         | SCHEMA       | alias_namespace   | namespace alias      |

  Scenario: Duplicate database, schema, and namespace names conflict
    Given statement
      """
      CREATE DATABASE alias_duplicate
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS alias_duplicate
      """
    Given statement with error .*
      """
      CREATE SCHEMA alias_duplicate
      """
    Given statement with error .*
      """
      CREATE NAMESPACE alias_duplicate
      """
