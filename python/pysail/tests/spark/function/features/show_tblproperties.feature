@show_tblproperties
Feature: SHOW TBLPROPERTIES

  Rule: Show all properties

    Scenario: show properties of a table with properties
      Given statement
        """
        CREATE TABLE test_show_props1 (id INT) USING parquet TBLPROPERTIES ('key1' = 'value1', 'key2' = 'value2')
        """
      And final statement
        """
        DROP TABLE IF EXISTS test_show_props1
        """
      When query
        """
        SHOW TBLPROPERTIES test_show_props1
        """
      Then query result
        | key  | value  |
        | key1 | value1 |
        | key2 | value2 |

    Scenario: show properties of a table without properties
      Given statement
        """
        CREATE TABLE test_show_props2 (id INT) USING parquet
        """
      And final statement
        """
        DROP TABLE IF EXISTS test_show_props2
        """
      When query
        """
        SHOW TBLPROPERTIES test_show_props2
        """
      Then query result
        | key | value |

  Rule: Show specific property by key

    Scenario: show a specific property that exists
      Given statement
        """
        CREATE TABLE test_show_props3 (id INT) USING parquet TBLPROPERTIES ('mykey' = 'myval')
        """
      And final statement
        """
        DROP TABLE IF EXISTS test_show_props3
        """
      When query
        """
        SHOW TBLPROPERTIES test_show_props3 ('mykey')
        """
      Then query result
        | key   | value |
        | mykey | myval |

  Rule: Error cases

    Scenario: show properties on nonexistent table errors
      When query
        """
        SHOW TBLPROPERTIES nonexistent_table_xyz
        """
      Then query error (TABLE_OR_VIEW_NOT_FOUND|table not found|view not found)
