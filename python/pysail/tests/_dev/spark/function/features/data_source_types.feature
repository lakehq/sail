Feature: Data source functions preserve data type metadata

  @sail-only
  Scenario Outline: from_csv rejects geospatial schemas before parsing values
    When query
      """
      SELECT from_csv(<input>, 'g <type>') AS result
      """
    Then query error Unsupported data type in from_csv schema

    Examples:
      | type                           | input                |
      | GEOMETRY(4326)                 | 'not-wkb'            |
      | GEOGRAPHY(4326)                | 'not-wkb'            |
      | GEOMETRY(4326)                 | ''                   |
      | GEOGRAPHY(4326)                | ''                   |
      | GEOMETRY(4326)                 | CAST(NULL AS STRING) |
      | GEOGRAPHY(4326)                | CAST(NULL AS STRING) |
      | STRUCT<value: GEOMETRY(4326)>  | ''                   |
      | STRUCT<value: GEOGRAPHY(4326)> | ''                   |
      | ARRAY<GEOMETRY(4326)>          | ''                   |
      | ARRAY<GEOGRAPHY(4326)>         | ''                   |
      | MAP<STRING, GEOMETRY(4326)>    | ''                   |
      | MAP<GEOGRAPHY(4326), STRING>   | ''                   |

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
