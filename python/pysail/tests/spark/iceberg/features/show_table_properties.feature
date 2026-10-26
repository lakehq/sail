Feature: iceberg SHOW TBLPROPERTIES

  Background:
    Given variable location for temporary directory iceberg_show_properties
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_show_properties_test
      """
    Given statement template
      """
      CREATE TABLE iceberg_show_properties_test (id INT) USING iceberg
      LOCATION {{ location.sql }}
      TBLPROPERTIES ('custom.z' = 'last', 'custom.a' = 'first')
      """

  Scenario: Show all iceberg properties in key order before any data is written
    When query
      """
      SHOW TBLPROPERTIES iceberg_show_properties_test
      """
    Then query schema
      """
      root
       |-- key: string (nullable = false)
       |-- value: string (nullable = false)
      """
    Then query result collected ordered
      | key | value |
      | current-snapshot-id | none |
      | custom.a | first |
      | custom.z | last |
      | format | iceberg/parquet |
      | format-version | 2 |

  Scenario Outline: Show a single iceberg property using different key syntax
    When query
      """
      SHOW TBLPROPERTIES iceberg_show_properties_test (<key>)
      """
    Then query result
      | key | value |
      | custom.a | first |

    Examples:
      | key |
      | 'custom.a' |
      | custom.a |
      | `custom.a` |

  Scenario: Show iceberg path properties after an alteration
    Given statement
      """
      ALTER TABLE iceberg_show_properties_test SET TBLPROPERTIES ('custom.a' = 'changed')
      """
    When query template
      """
      SHOW TBLPROPERTIES iceberg.`{{ location.string }}` ('custom.a')
      """
    Then query result
      | key | value |
      | custom.a | changed |

  Scenario: Show iceberg properties reads storage metadata through another registration
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_show_properties_test_alias
      """
    Given statement template
      """
      CREATE TABLE iceberg_show_properties_test_alias USING iceberg LOCATION {{ location.sql }}
      """
    Given statement
      """
      ALTER TABLE iceberg_show_properties_test SET TBLPROPERTIES ('custom.a' = 'fresh')
      """
    When query
      """
      SHOW TBLPROPERTIES iceberg_show_properties_test_alias ('custom.a')
      """
    Then query result
      | key | value |
      | custom.a | fresh |

  Scenario: Show an unset iceberg property returns a missing property message
    Given statement
      """
      ALTER TABLE iceberg_show_properties_test UNSET TBLPROPERTIES ('custom.a')
      """
    When query
      """
      SHOW TBLPROPERTIES iceberg_show_properties_test ('custom.a')
      """
    Then query result row where "key" is "custom.a" has "value" containing "does not have property: custom.a"

  Scenario: Show properties of a missing iceberg table fails
    When query
      """
      SHOW TBLPROPERTIES iceberg_show_properties_test_missing
      """
    Then query error TABLE_OR_VIEW_NOT_FOUND

  Scenario: Show Iceberg format version follows metadata after an upgrade
    Given statement
      """
      ALTER TABLE iceberg_show_properties_test SET TBLPROPERTIES ('format-version' = '3')
      """
    When query
      """
      SHOW TBLPROPERTIES iceberg_show_properties_test ('format-version')
      """
    Then query result
      | key | value |
      | format-version | 3 |

  Scenario: Show Iceberg properties respects the redaction configuration
    Given config spark.sql.redaction.options.regex = (?i)custom
    When query
      """
      SHOW TBLPROPERTIES iceberg_show_properties_test ('custom.a')
      """
    Then query result
      | key | value |
      | custom.a | *********(redacted) |
