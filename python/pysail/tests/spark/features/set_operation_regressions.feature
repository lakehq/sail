Feature: Set operation coercion preserves matching and Spark result types
  Background:
    Given config spark.sql.ansi.enabled = true
    Given config spark.sql.session.timeZone = UTC

  Scenario Outline: ANSI intersections compare numeric values instead of their string forms
    When query
      """
      SELECT <left> AS v <operation> SELECT <right> AS v
      """
    Then query column types
      | column | type   |
      | v      | <type> |
    Then query result collected
      | v       |
      | <value> |

    Examples:
      | left                     | right                    | operation     | type   | value |
      | 3                        | '03'                     | INTERSECT     | bigint | 3     |
      | '03'                     | 3                        | INTERSECT     | bigint | 3     |
      | 3                        | ' 3'                     | INTERSECT ALL | bigint | 3     |
      | ' 3'                     | 3                        | INTERSECT ALL | bigint | 3     |
      | CAST(3 AS DECIMAL(5,2))   | '3'                      | INTERSECT     | double | 3.0   |
      | '3'                      | CAST(3 AS DECIMAL(5,2))   | INTERSECT ALL | double | 3.0   |
      | CAST(3 AS DOUBLE)         | '3'                      | INTERSECT ALL | double | 3.0   |
      | '3'                      | CAST(3 AS DOUBLE)         | INTERSECT     | double | 3.0   |
      | array(3)                 | array('03')              | INTERSECT ALL | array<bigint> | [3] |
      | named_struct('a', '03')   | named_struct('a', 3)      | INTERSECT     | struct<a:bigint> | Row(a=3) |

  Scenario Outline: ANSI differences remove numerically equal strings
    When query
      """
      SELECT <left> AS v <operation> SELECT <right> AS v
      """
    Then query column types
      | column | type   |
      | v      | <type> |
    Then query result collected
      | v |

    Examples:
      | left                   | right                  | operation  | type   |
      | 3                      | '03'                   | EXCEPT     | bigint |
      | '03'                   | 3                      | EXCEPT ALL | bigint |
      | CAST(3 AS DECIMAL(5,2)) | '3'                    | EXCEPT ALL | double |
      | '3'                    | CAST(3 AS DOUBLE)      | EXCEPT     | double |

  Scenario Outline: ANSI set operations reject malformed numeric strings
    When query
      """
      SELECT <left> AS v <operation> SELECT <right> AS v
      """
    Then query error (?i)(cast|invalid|malformed)

    Examples:
      | left  | right | operation     |
      | 3     | 'abc' | EXCEPT        |
      | 'abc' | 3     | EXCEPT ALL    |
      | 3     | 'abc' | INTERSECT     |
      | 'abc' | 3     | INTERSECT ALL |

  Scenario Outline: Legacy set operations retain string matching
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT count(*) AS n FROM (SELECT 3 AS v <operation> SELECT '03' AS v)
      """
    Then query result collected
      | n       |
      | <count> |

    Examples:
      | operation     | count |
      | EXCEPT        | 1     |
      | EXCEPT ALL    | 1     |
      | INTERSECT     | 0     |
      | INTERSECT ALL | 0     |

  Scenario Outline: ANSI set operation multiplicity follows numeric coercion
    When query
      """
      SELECT v FROM VALUES ('03'), ('3') t(v)
      <operation> SELECT 3 AS v
      """
    Then query result collected
      | v |
      | 3 |
    Then query column types
      | column | type   |
      | v      | bigint |

    Examples:
      | operation     |
      | INTERSECT     |
      | INTERSECT ALL |
      | EXCEPT ALL    |

  Scenario Outline: Temporal intersections expose microsecond Spark timestamps
    Given config spark.sql.session.timeZone = <timezone>
    When query
      """
      SELECT <left> AS v <operation> SELECT <right> AS v
      """
    Then query column types
      | column | type   |
      | v      | <type> |
    When query
      """
      SELECT CAST(v AS STRING) AS v FROM (
        SELECT <left> AS v <operation> SELECT <right> AS v
      )
      """
    Then query result collected
      | v                   |
      | <value>             |

    Examples:
      | timezone         | left                             | right                                    | operation     | type          | value               |
      | UTC              | DATE '2024-01-01'                | TIMESTAMP '2024-01-01 00:00:00'           | INTERSECT     | timestamp     | 2024-01-01 00:00:00 |
      | America/New_York | DATE '2024-01-01'                | TIMESTAMP '2024-01-01 00:00:00'           | INTERSECT ALL | timestamp     | 2024-01-01 00:00:00 |
      | America/New_York | TIMESTAMP '2024-01-01 00:00:00'  | DATE '2024-01-01'                         | INTERSECT     | timestamp     | 2024-01-01 00:00:00 |
      | America/New_York | DATE '2024-01-01'                | TIMESTAMP_NTZ '2024-01-01 00:00:00'       | INTERSECT ALL | timestamp_ntz | 2024-01-01 00:00:00 |
      | UTC              | TIMESTAMP_NTZ '2024-01-01 00:00:00' | DATE '2024-01-01'                      | INTERSECT     | timestamp_ntz | 2024-01-01 00:00:00 |
      | UTC              | TIMESTAMP '2024-01-01 00:00:00'  | '2024-01-01 00:00:00'                    | INTERSECT ALL | timestamp     | 2024-01-01 00:00:00 |
      | America/New_York | '2024-01-01 00:00:00'            | TIMESTAMP '2024-01-01 00:00:00'           | INTERSECT     | timestamp     | 2024-01-01 00:00:00 |
      | America/New_York | TIMESTAMP_NTZ '2024-01-01 00:00:00' | '2024-01-01 00:00:00'                  | INTERSECT ALL | timestamp_ntz | 2024-01-01 00:00:00 |
      | UTC              | DATE '1500-01-01'                | TIMESTAMP '1500-01-01 00:00:00'           | INTERSECT ALL | timestamp     | 1500-01-01 00:00:00 |
      | America/Havana   | DATE '2020-03-08'                | TIMESTAMP '2020-03-08 01:00:00'           | INTERSECT     | timestamp     | 2020-03-08 01:00:00 |

  Scenario Outline: Empty temporal differences retain a representable schema
    Given config spark.sql.session.timeZone = <timezone>
    When query
      """
      SELECT DATE '2024-01-01' AS v <operation> SELECT TIMESTAMP '2024-01-01 00:00:00' AS v
      """
    Then query column types
      | column | type      |
      | v      | timestamp |
    Then query result collected
      | v |

    Examples:
      | timezone         | operation  |
      | UTC              | EXCEPT     |
      | UTC              | EXCEPT ALL |
      | America/New_York | EXCEPT     |
      | America/New_York | EXCEPT ALL |

  Scenario Outline: Nested temporal intersections retain timestamp precision and values
    When query
      """
      SELECT <left> AS v INTERSECT ALL SELECT <right> AS v
      """
    Then query column types
      | column | type   |
      | v      | <type> |
    When query
      """
      SELECT CAST(v AS STRING) AS v FROM (
        SELECT <left> AS v INTERSECT ALL SELECT <right> AS v
      )
      """
    Then query result collected
      | v       |
      | <value> |

    Examples:
      | left                               | right                                                    | type                | value                 |
      | array(DATE '2024-01-01')            | array(TIMESTAMP '2024-01-01 00:00:00')                    | array<timestamp>    | [2024-01-01 00:00:00] |
      | named_struct('a', DATE '2024-01-01') | named_struct('a', TIMESTAMP_NTZ '2024-01-01 00:00:00')    | struct<a:timestamp_ntz> | {2024-01-01 00:00:00} |

  Scenario Outline: Legacy temporal strings use the session's display values
    Given config spark.sql.ansi.enabled = false
    Given config spark.sql.session.timeZone = America/New_York
    When query
      """
      SELECT <left> AS v INTERSECT SELECT <right> AS v
      """
    Then query column types
      | column | type   |
      | v      | string |
    Then query result collected
      | v       |
      | <value> |

    Examples:
      | left                           | right                           | value               |
      | TIMESTAMP '2024-01-01 00:00:00' | '2024-01-01 00:00:00'           | 2024-01-01 00:00:00 |
      | '2024-01-01 00:00:00'          | TIMESTAMP '2024-01-01 00:00:00'  | 2024-01-01 00:00:00 |
      | DATE '2024-01-01'             | '2024-01-01'                    | 2024-01-01          |

  Scenario Outline: Legacy nested temporal strings use Spark formatting at every level
    Given config spark.sql.ansi.enabled = false
    Given config spark.sql.session.timeZone = America/New_York
    When query
      """
      SELECT <left> AS v INTERSECT SELECT <right> AS v
      """
    Then query column types
      | column | type   |
      | v      | <type> |
    Then query result collected
      | v       |
      | <value> |

    Examples:
      | left                                                                   | right                                                                | type                             | value                                  |
      | array(TIMESTAMP '2024-01-01 00:00:00')                                 | array('2024-01-01 00:00:00')                                         | array<string>                    | ['2024-01-01 00:00:00']                |
      | array('2024-01-01 00:00:00')                                           | array(TIMESTAMP '2024-01-01 00:00:00')                               | array<string>                    | ['2024-01-01 00:00:00']                |
      | named_struct('a', TIMESTAMP '2024-01-01 00:00:00.123456')              | named_struct('a', '2024-01-01 00:00:00.123456')                      | struct<a:string>                 | Row(a='2024-01-01 00:00:00.123456')    |
      | named_struct('a', '2024-01-01 00:00:00.123456')                        | named_struct('a', TIMESTAMP '2024-01-01 00:00:00.123456')            | struct<a:string>                 | Row(a='2024-01-01 00:00:00.123456')    |
      | array(array(TIMESTAMP_NTZ '2024-01-01 00:00:00'))                      | array(array('2024-01-01 00:00:00'))                                  | array<array<string>>             | [['2024-01-01 00:00:00']]              |
      | array(named_struct('a', array(TIMESTAMP '2024-01-01 00:00:00', NULL))) | array(named_struct('a', array('2024-01-01 00:00:00', NULL)))         | array<struct<a:array<string>>>   | [Row(a=['2024-01-01 00:00:00', None])] |
      | named_struct('a', array(DATE '2024-01-01', NULL), 'b', 3)              | named_struct('a', array('2024-01-01', NULL), 'b', CAST(3 AS BIGINT)) | struct<a:array<string>,b:bigint> | Row(a=['2024-01-01', None], b=3)       |

  Scenario Outline: Legacy nested temporal coercion precedes set operation multiplicity
    Given config spark.sql.ansi.enabled = false
    Given config spark.sql.session.timeZone = America/New_York
    When query
      """
      SELECT count(*) AS n FROM (
        SELECT v FROM VALUES (<left>), (<left>) t(v)
        <operation> SELECT <right> AS v
      )
      """
    Then query result collected
      | n       |
      | <count> |

    Examples:
      | left                                     | right                                              | operation     | count |
      | array(TIMESTAMP '2024-01-01 00:00:00')   | array('2024-01-01 00:00:00')                       | INTERSECT     | 1     |
      | array(TIMESTAMP '2024-01-01 00:00:00')   | array('2024-01-01 00:00:00')                       | INTERSECT ALL | 1     |
      | array(TIMESTAMP '2024-01-01 00:00:00')   | array('2024-01-01 00:00:00')                       | EXCEPT        | 0     |
      | array(TIMESTAMP '2024-01-01 00:00:00')   | array('2024-01-01 00:00:00')                       | EXCEPT ALL    | 1     |
      | named_struct('a', '2024-01-01 00:00:00') | named_struct('a', TIMESTAMP '2024-01-01 00:00:00') | INTERSECT     | 1     |
      | named_struct('a', '2024-01-01 00:00:00') | named_struct('a', TIMESTAMP '2024-01-01 00:00:00') | INTERSECT ALL | 1     |
      | named_struct('a', '2024-01-01 00:00:00') | named_struct('a', TIMESTAMP '2024-01-01 00:00:00') | EXCEPT        | 0     |
      | named_struct('a', '2024-01-01 00:00:00') | named_struct('a', TIMESTAMP '2024-01-01 00:00:00') | EXCEPT ALL    | 1     |

  Scenario: Legacy nested temporal coercion preserves null structs and null fields
    Given config spark.sql.ansi.enabled = false
    Given config spark.sql.session.timeZone = America/New_York
    When query
      """
      SELECT v FROM VALUES
        (CAST(NULL AS STRUCT<a:TIMESTAMP>)),
        (named_struct('a', CAST(NULL AS TIMESTAMP))),
        (named_struct('a', TIMESTAMP '2024-01-01 00:00:00')) t(v)
      INTERSECT
      SELECT v FROM VALUES
        (CAST(NULL AS STRUCT<a:STRING>)),
        (named_struct('a', CAST(NULL AS STRING))),
        (named_struct('a', '2024-01-01 00:00:00')) t(v)
      """
    Then query column types
      | column | type             |
      | v      | struct<a:string> |
    Then query result collected
      | v                            |
      | NULL                         |
      | Row(a=None)                  |
      | Row(a='2024-01-01 00:00:00') |

  Scenario: Legacy nested temporal coercion preserves null and empty arrays
    Given config spark.sql.ansi.enabled = false
    Given config spark.sql.session.timeZone = America/New_York
    When query
      """
      SELECT v FROM VALUES
        (CAST(NULL AS ARRAY<TIMESTAMP>)),
        (CAST(array() AS ARRAY<TIMESTAMP>)),
        (array(TIMESTAMP '2024-01-01 00:00:00', NULL)) t(v)
      INTERSECT
      SELECT v FROM VALUES
        (CAST(NULL AS ARRAY<STRING>)),
        (CAST(array() AS ARRAY<STRING>)),
        (array('2024-01-01 00:00:00', NULL)) t(v)
      """
    Then query column types
      | column | type          |
      | v      | array<string> |
    Then query result collected
      | v                             |
      | NULL                          |
      | []                            |
      | ['2024-01-01 00:00:00', None] |
