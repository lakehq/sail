Feature: HMS table property inspection

  Background:
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS show_properties_db LOCATION 's3://hms-warehouse/show_properties'
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS show_properties_db CASCADE
      """

  Scenario Outline: Show catalog table properties reads HMS tables before and after writes
    Given statement
      """
      CREATE TABLE sail.show_properties_db.show_properties (id INT) USING <format>
      
      TBLPROPERTIES ('custom.show' = 'initial')
      """
    When query
      """
      SHOW TBLPROPERTIES sail.show_properties_db.show_properties
      """
    Then query result row where "key" is "custom.show" has "value" equal to "initial"
    Then query schema
      """
      root
       |-- key: string (nullable = false)
       |-- value: string (nullable = false)
      """
    When query
      """
      SHOW TBLPROPERTIES sail.show_properties_db.show_properties ('custom.show')
      """
    Then query result
      | key | value |
      | custom.show | initial |
    When query
      """
      SHOW TBLPROPERTIES sail.show_properties_db.show_properties ('custom.missing')
      """
    Then query result row where "key" is "custom.missing" has "value" containing "does not have property: custom.missing"
    Given statement
      """
      INSERT INTO sail.show_properties_db.show_properties VALUES (1)
      """
    When query
      """
      SHOW TBLPROPERTIES sail.show_properties_db.show_properties ('custom.show')
      """
    Then query result
      | key | value |
      | custom.show | initial |

    Examples:
      | format |
      | delta |
      | iceberg |
