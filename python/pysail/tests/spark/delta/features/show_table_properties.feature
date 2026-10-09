Feature: delta SHOW TBLPROPERTIES

  Background:
    Given variable location for temporary directory delta_show_properties
    Given final statement
      """
      DROP TABLE IF EXISTS delta_show_properties_test
      """
    Given statement template
      """
      CREATE TABLE delta_show_properties_test (id INT) USING delta
      LOCATION {{ location.sql }}
      TBLPROPERTIES ('custom.z' = 'last', 'custom.a' = 'first')
      """

  Scenario: Show all delta properties in key order before any data is written
    When query
      """
      SHOW TBLPROPERTIES delta_show_properties_test
      """
    Then query schema
      """
      root
       |-- key: string (nullable = false)
       |-- value: string (nullable = false)
      """
    Then query result collected ordered
      | key | value |
      | custom.a | first |
      | custom.z | last |
      | delta.minReaderVersion | 1 |
      | delta.minWriterVersion | 2 |

  Scenario Outline: Show a single delta property using different key syntax
    When query
      """
      SHOW TBLPROPERTIES delta_show_properties_test (<key>)
      """
    Then query result
      | key | value |
      | custom.a | first |

    Examples:
      | key |
      | 'custom.a' |
      | custom.a |
      | `custom.a` |

  Scenario: Show delta path properties after an alteration
    Given statement
      """
      ALTER TABLE delta_show_properties_test SET TBLPROPERTIES ('custom.a' = 'changed')
      """
    When query template
      """
      SHOW TBLPROPERTIES delta.`{{ location.string }}` ('custom.a')
      """
    Then query result
      | key | value |
      | custom.a | changed |

  Scenario: Show delta properties reads storage metadata through another registration
    Given final statement
      """
      DROP TABLE IF EXISTS delta_show_properties_test_alias
      """
    Given statement template
      """
      CREATE TABLE delta_show_properties_test_alias USING delta LOCATION {{ location.sql }}
      """
    Given statement
      """
      ALTER TABLE delta_show_properties_test SET TBLPROPERTIES ('custom.a' = 'fresh')
      """
    When query
      """
      SHOW TBLPROPERTIES delta_show_properties_test_alias ('custom.a')
      """
    Then query result
      | key | value |
      | custom.a | fresh |

  Scenario: Show an unset delta property returns a missing property message
    Given statement
      """
      ALTER TABLE delta_show_properties_test UNSET TBLPROPERTIES ('custom.a')
      """
    When query
      """
      SHOW TBLPROPERTIES delta_show_properties_test ('custom.a')
      """
    Then query result row where "key" is "custom.a" has "value" containing "does not have property: custom.a"

  Scenario: Show properties of a missing delta table fails
    When query
      """
      SHOW TBLPROPERTIES delta_show_properties_test_missing
      """
    Then query error TABLE_OR_VIEW_NOT_FOUND

  Scenario: Show Delta protocol features after enabling in commit timestamps
    Given statement
      """
      ALTER TABLE delta_show_properties_test SET TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true')
      """
    When query
      """
      SHOW TBLPROPERTIES delta_show_properties_test ('delta.feature.inCommitTimestamp')
      """
    Then query result
      | key | value |
      | delta.feature.inCommitTimestamp | supported |

  Scenario: Show Delta change data feed properties reflects enablement and protocol
    Given statement
      """
      ALTER TABLE delta_show_properties_test SET TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.enableInCommitTimestamps' = 'true'
      )
      """
    Given statement
      """
      INSERT INTO delta_show_properties_test VALUES (1)
      """
    When query
      """
      SHOW TBLPROPERTIES delta_show_properties_test ('delta.enableChangeDataFeed')
      """
    Then query result
      | key | value |
      | delta.enableChangeDataFeed | true |
    When query template
      """
      SHOW TBLPROPERTIES delta.`{{ location.string }}` ('delta.feature.changeDataFeed')
      """
    Then query result
      | key | value |
      | delta.feature.changeDataFeed | supported |
    Given statement
      """
      ALTER TABLE delta_show_properties_test SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'false')
      """
    When query
      """
      SHOW TBLPROPERTIES delta_show_properties_test
      """
    Then query result row where "key" is "delta.enableChangeDataFeed" has "value" equal to "false"
    Then query result row where "key" is "delta.feature.changeDataFeed" has "value" equal to "supported"

  Scenario: Show Delta property keys are case sensitive
    When query
      """
      SHOW TBLPROPERTIES delta_show_properties_test ('CUSTOM.A')
      """
    Then query result row where "key" is "CUSTOM.A" has "value" containing "does not have property: CUSTOM.A"

  Scenario: Show Delta properties redacts sensitive keys and values
    Given statement
      """
      ALTER TABLE delta_show_properties_test SET TBLPROPERTIES ('api.token' = 'private-value', 'custom.a' = 'password=private-value')
      """
    When query
      """
      SHOW TBLPROPERTIES delta_show_properties_test
      """
    Then query result row where "key" is "api.token" has "value" equal to "*********(redacted)"
    Then query result row where "key" is "custom.a" has "value" equal to "*********(redacted)"

  Scenario: Show Delta catalog properties includes persisted storage options
    Given variable options_location for temporary directory show_properties_options
    Given final statement
      """
      DROP TABLE IF EXISTS show_properties_options
      """
    Given statement template
      """
      CREATE TABLE show_properties_options (id INT) USING delta
      OPTIONS ('compression' = 'gzip', 'fs.custom' = 'private', 'url' = 'https://example.invalid')
      LOCATION {{ options_location.sql }}
      """
    When query
      """
      SHOW TBLPROPERTIES show_properties_options ('option.compression')
      """
    Then query result
      | key | value |
      | option.compression | gzip |
    When query
      """
      SHOW TBLPROPERTIES show_properties_options
      """
    Then query result
      | key | value |
      | delta.minReaderVersion | 1 |
      | delta.minWriterVersion | 2 |
      | option.compression | gzip |
      | option.url | *********(redacted) |
