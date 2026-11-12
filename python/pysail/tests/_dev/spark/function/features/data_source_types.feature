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

  Scenario Outline: from_csv parses day-time interval literals: <case>
    When query
      """
      SELECT from_csv(value, 'a INTERVAL <qualifier>').a <=> <expected> AS matches
      FROM VALUES (<input>) AS t(value)
      """
    Then query result
      | matches |
      | true    |

    Examples:
      | case                        | input                                        | qualifier        | expected                                   |
      | whole days                  | "INTERVAL '1' DAY"                           | DAY              | INTERVAL '1' DAY                           |
      | negative days               | "INTERVAL '-1' DAY"                          | DAY              | INTERVAL '-1' DAY                          |
      | days and hours              | "INTERVAL '1 12' DAY TO HOUR"                | DAY TO HOUR      | INTERVAL '1 12' DAY TO HOUR                |
      | days through seconds        | "INTERVAL '1 02:00:03.000004' DAY TO SECOND" | DAY TO SECOND    | INTERVAL '1 02:00:03.000004' DAY TO SECOND |
      | hours through seconds       | "INTERVAL '02:03:04.000005' HOUR TO SECOND"  | HOUR TO SECOND   | INTERVAL '02:03:04.000005' HOUR TO SECOND  |
      | minutes through seconds     | "INTERVAL '02:03.000004' MINUTE TO SECOND"   | MINUTE TO SECOND | INTERVAL '02:03.000004' MINUTE TO SECOND   |
      | microseconds                | "INTERVAL '0.000001' SECOND"                 | SECOND           | INTERVAL '0.000001' SECOND                 |
      | negative fractional seconds | "INTERVAL '-1.000001' SECOND"                | SECOND           | INTERVAL '-1.000001' SECOND                |
      | zero                        | "INTERVAL '0' SECOND"                        | SECOND           | INTERVAL '0' SECOND                        |
      | empty field                 | ''                                           | DAY              | CAST(NULL AS INTERVAL DAY)                 |
      | null input                  | CAST(NULL AS STRING)                         | DAY              | CAST(NULL AS INTERVAL DAY)                 |

  Scenario: from_csv rejects overflowing interval literals in FAILFAST mode
    When query
      """
      SELECT from_csv(
        "INTERVAL '999999999999999999999999999' DAY",
        'a INTERVAL DAY', map('mode', 'FAILFAST')
      ) AS result
      """
    Then query error (?i)invalid argument|malformed records

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
