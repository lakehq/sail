Feature: Database, schema, and namespace aliases

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
