Feature: Data source functions preserve data type metadata

  Scenario Outline: <function> preserves interval qualifiers
    When query
      """
      SELECT <function>(value, 'a INTERVAL HOUR TO SECOND, b INTERVAL MONTH') AS result
      FROM VALUES (<input>), (CAST(NULL AS STRING)) AS t(value)
      """
    Then query schema
      """
      root
       |-- result: struct (nullable = true)
       |    |-- a: interval hour to second (nullable = true)
       |    |-- b: interval month (nullable = true)
      """

    Examples:
      | function  | input       |
      | from_csv  | ','         |
      | from_json | '{}'        |
      | from_xml  | '<r></r>'   |

  Scenario Outline: <function> preserves interval qualifiers in nested fields
    When query
      """
      SELECT <function>(value, 's STRUCT<a: INTERVAL MINUTE TO SECOND>') AS result
      FROM VALUES (<input>), (CAST(NULL AS STRING)) AS t(value)
      """
    Then query schema
      """
      root
       |-- result: struct (nullable = true)
       |    |-- s: struct (nullable = true)
       |    |    |-- a: interval minute to second (nullable = true)
      """

    Examples:
      | function  | input     |
      | from_json | '{}'      |
      | from_xml  | '<r></r>' |
