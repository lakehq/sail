Feature: Catalog SHOW TBLPROPERTIES compatibility

  Scenario Outline: Show properties accepts Java lookaround in the redaction setting
    Given variable location for temporary directory show_properties_redaction
    Given final statement
      """
      DROP TABLE IF EXISTS show_properties_redaction
      """
    Given statement template
      """
      CREATE TABLE show_properties_redaction (id INT) USING parquet
      LOCATION {{ location.sql }}
      TBLPROPERTIES ('custom.a' = 'plain', 'api.token' = 'private')
      """
    Given config spark.sql.redaction.options.regex = <pattern>
    When query
      """
      SHOW TBLPROPERTIES show_properties_redaction
      """
    Then query result
      | key | value |
      | api.token | *********(redacted) |
      | custom.a | <value> |

    Examples:
      | pattern | value |
      | (?!) | plain |
      | (?i)custom(?=[.]) | *********(redacted) |
      | (?<=custom)[.]a | *********(redacted) |

  Scenario Outline: Show properties resolves databases named after a lakehouse format
    Given variable location for temporary directory show_properties_database
    Given statement template
      """
      CREATE DATABASE <database> LOCATION {{ location.sql }}
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS <database> CASCADE
      """
    Given statement
      """
      CREATE TABLE <database>.show_properties (id INT) USING parquet
      TBLPROPERTIES ('custom.a' = 'catalog')
      """
    When query
      """
      SHOW TBLPROPERTIES <database>.show_properties ('custom.a')
      """
    Then query result
      | key | value |
      | custom.a | catalog |
    When query
      """
      SHOW TBLPROPERTIES <database>.missing_table
      """
    Then query error TABLE_OR_VIEW_NOT_FOUND

    Examples:
      | database |
      | delta |
      | iceberg |
