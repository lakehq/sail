Feature: CASE validation and supported result types

  Rule: Every condition and result is validated before unreachable branches are removed

    Scenario Outline: CASE rejects an invalid condition after TRUE with ANSI <ansi>: <condition>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CASE WHEN true THEN 1 WHEN <condition> THEN 2 ELSE 3 END AS result
        """
      Then query error (?i)(boolean|bool|unexpected_input_type)

      Examples:
        | ansi  | condition |
        | false | 42        |
        | true  | 42        |
        | false | 'true'    |
        | true  | 'true'    |
        | false | NULL      |
        | true  | NULL      |
        | false | NOT 42    |
        | true  | NOT 42    |

    Scenario Outline: CASE rejects incompatible result types after TRUE with ANSI <ansi>: <left>, <right>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CASE WHEN true THEN <left> ELSE <right> END AS result
        """
      Then query error (?i)(type|coerc)

      Examples:
        | ansi  | left                 | right                        |
        | false | named_struct('a', 1) | named_struct('b', 2)          |
        | true  | named_struct('a', 1) | named_struct('b', 2)          |
        | false | named_struct('a', 1) | named_struct('a', 2, 'b', 3)  |
        | true  | named_struct('a', 1) | named_struct('a', 2, 'b', 3)  |
        | false | true                 | 1                            |
        | true  | true                 | 1                            |
        | false | 1                    | array(2)                     |
        | true  | 1                    | array(2)                     |
        | false | 'true'               | false                        |
        | false | 'x'                  | X'79'                        |

  Rule: Scalar result branches use the common Spark type

    Scenario Outline: CASE promotes string and integer branches in both orders with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT result, reversed, typeof(result) AS result_type, typeof(reversed) AS reversed_type
        FROM (
          SELECT CASE WHEN p THEN '1' ELSE 2 END AS result,
                 CASE p WHEN true THEN 2 ELSE '1' END AS reversed
          FROM VALUES (true), (false), (CAST(NULL AS BOOLEAN)) AS t(p)
        )
        """
      Then query schema
        """
        root
         |-- result: <schema_type> (nullable = <nullable>)
         |-- reversed: <schema_type> (nullable = <nullable>)
         |-- result_type: string (nullable = false)
         |-- reversed_type: string (nullable = false)
        """
      And query result collected
        | result | reversed | result_type | reversed_type |
        | 1      | 2        | <type>      | <type>        |
        | 2      | 1        | <type>      | <type>        |
        | 2      | 1        | <type>      | <type>        |

      Examples:
        | ansi  | schema_type | type   | nullable |
        | false | string      | string | false    |
        | true  | long        | bigint | true     |

    Scenario: ANSI CASE promotes string branches to Boolean and binary
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT result, typeof(result) AS result_type,
               binary_result, typeof(binary_result) AS binary_type
        FROM (
          SELECT CASE WHEN p THEN 'true' ELSE false END AS result,
                 CASE WHEN p THEN 'x' ELSE X'79' END AS binary_result
          FROM VALUES (true), (false) AS t(p)
        )
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
         |-- result_type: string (nullable = false)
         |-- binary_result: binary (nullable = false)
         |-- binary_type: string (nullable = false)
        """
      And query result
        | result | result_type | binary_result | binary_type |
        | true   | boolean     | [78] | binary      |
        | false  | boolean     | [79] | binary      |

    Scenario Outline: CASE merges date and timestamp types in both orders with ANSI <ansi>: <left_type>, <right_type>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CASE WHEN p THEN <left> ELSE <right> END AS result,
               CASE p WHEN true THEN <right> ELSE <left> END AS reversed
        FROM VALUES (true), (false) AS t(p)
        """
      Then query schema
        """
        root
         |-- result: <type> (nullable = <nullable>)
         |-- reversed: <type> (nullable = <nullable>)
        """
      And query result
        | result              | reversed            |
        | 2020-01-01 00:00:00 | 2020-01-02 03:04:05 |
        | 2020-01-02 03:04:05 | 2020-01-01 00:00:00 |

      Examples:
        | ansi  | left_type     | right_type    | left                                | right                               | type          | nullable |
        | false | date          | timestamp     | DATE '2020-01-01'                   | TIMESTAMP '2020-01-02 03:04:05'      | timestamp     | false    |
        | true  | date          | timestamp     | DATE '2020-01-01'                   | TIMESTAMP '2020-01-02 03:04:05'      | timestamp     | false    |
        | false | date          | timestamp_ntz | DATE '2020-01-01'                   | TIMESTAMP_NTZ '2020-01-02 03:04:05'  | timestamp_ntz | true     |
        | true  | date          | timestamp_ntz | DATE '2020-01-01'                   | TIMESTAMP_NTZ '2020-01-02 03:04:05'  | timestamp_ntz | true     |
        | false | timestamp_ntz | timestamp     | TIMESTAMP_NTZ '2020-01-01 00:00:00' | TIMESTAMP '2020-01-02 03:04:05'      | timestamp     | false    |
        | true  | timestamp_ntz | timestamp     | TIMESTAMP_NTZ '2020-01-01 00:00:00' | TIMESTAMP '2020-01-02 03:04:05'      | timestamp     | false    |

    Scenario Outline: CASE timestamp coercion matches explicit casts across DST with ANSI <ansi>: <local_time>
      Given config spark.sql.ansi.enabled = <ansi>
      And config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT result, reversed, unix_micros(result) AS instant, unix_micros(reversed) AS reversed_instant
        FROM (
          SELECT CASE WHEN p THEN TIMESTAMP_NTZ '<local_time>'
                      ELSE TIMESTAMP '2024-01-02 03:04:05' END AS result,
                 CASE p WHEN true THEN TIMESTAMP '2024-01-02 03:04:05'
                        ELSE TIMESTAMP_NTZ '<local_time>' END AS reversed
          FROM VALUES (true), (false) AS t(p)
        )
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
         |-- reversed: timestamp (nullable = false)
         |-- instant: long (nullable = false)
         |-- reversed_instant: long (nullable = false)
        """
      And query result
        | result               | reversed             | instant          | reversed_instant |
        | <display_time>       | 2024-01-02 03:04:05  | <instant>        | 1704193445000000 |
        | 2024-01-02 03:04:05  | <display_time>       | 1704193445000000 | <instant>        |

      Examples:
        | ansi  | local_time          | display_time        | instant          |
        | false | 2024-03-10 02:30:00 | 2024-03-10 03:30:00 | 1710066600000000 |
        | true  | 2024-03-10 02:30:00 | 2024-03-10 03:30:00 | 1710066600000000 |
        | false | 2024-11-03 01:30:00 | 2024-11-03 01:30:00 | 1730622600000000 |
        | true  | 2024-11-03 01:30:00 | 2024-11-03 01:30:00 | 1730622600000000 |

  Rule: Collection result types are merged recursively

    Scenario Outline: CASE merges array elements in both orders with ANSI <ansi>: <left>, <right>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT result, reversed, typeof(result) AS result_type
        FROM (
          SELECT CASE WHEN p THEN <left> ELSE <right> END AS result,
                 CASE p WHEN true THEN <right> ELSE <left> END AS reversed
          FROM VALUES (true), (false) AS t(p)
        )
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: <element_type> (containsNull = <contains_null>)
         |-- reversed: array (nullable = false)
         |    |-- element: <element_type> (containsNull = <contains_null>)
         |-- result_type: string (nullable = false)
        """
      And query result collected
        | result        | reversed      | result_type |
        | <left_value>  | <right_value> | <type>      |
        | <right_value> | <left_value>  | <type>      |

      Examples:
        | ansi  | left           | right                 | left_value | right_value    | element_type | contains_null | type          |
        | false | array(1, NULL) | array(4294967296L, 2L) | [1, None]  | [4294967296, 2] | long         | true          | array<bigint> |
        | true  | array(1, NULL) | array(4294967296L, 2L) | [1, None]  | [4294967296, 2] | long         | true          | array<bigint> |
        | false | array(1)       | array(2.5F)           | [1.0]      | [2.5]          | float        | false         | array<float>  |
        | true  | array(1)       | array(2.5F)           | [1.0]      | [2.5]          | double       | false         | array<double> |
        | false | array(1.25BD)  | array(2.5F)           | [1.25]     | [2.5]          | double       | false         | array<double> |
        | true  | array(1.25BD)  | array(2.5F)           | [1.25]     | [2.5]          | double       | false         | array<double> |
        | false | array('1')     | array(2)              | ['1']      | ['2']          | string       | false         | array<string> |
        | true  | array('1')     | array(2)              | [1]        | [2]            | long         | true          | array<bigint> |
        | false | array()        | array(1)              | []         | [1]            | integer      | false         | array<int>    |
        | true  | array()        | array(1)              | []         | [1]            | integer      | false         | array<int>    |

    Scenario: CASE matches struct field names without case sensitivity and preserves the first spelling
      Given config spark.sql.caseSensitive = false
      When query
        """
        SELECT CASE WHEN p THEN named_struct('A', n)
                    ELSE named_struct('a', CAST(n AS BIGINT)) END AS result,
               CASE p WHEN true THEN named_struct('a', CAST(n AS BIGINT))
                      ELSE named_struct('A', n) END AS reversed,
               CASE WHEN p THEN array(named_struct('A', n), NULL)
                    ELSE array(named_struct('a', CAST(n AS BIGINT)), NULL) END AS nested
        FROM VALUES (true, 1), (false, 2), (false, NULL) AS t(p, n)
        """
      Then query schema
        """
        root
         |-- result: struct (nullable = false)
         |    |-- A: long (nullable = true)
         |-- reversed: struct (nullable = false)
         |    |-- a: long (nullable = true)
         |-- nested: array (nullable = false)
         |    |-- element: struct (containsNull = true)
         |    |    |-- A: long (nullable = true)
        """
      And query result collected
        | result      | reversed    | nested        |
        | Row(A=1)    | Row(a=1)    | [Row(A=1), None]    |
        | Row(A=2)    | Row(a=2)    | [Row(A=2), None]    |
        | Row(A=None) | Row(a=None) | [Row(A=None), None] |

    Scenario: CASE rejects differently capitalized struct fields when case sensitivity is enabled
      Given config spark.sql.caseSensitive = true
      When query
        """
        SELECT CASE WHEN true THEN named_struct('A', 1)
                    ELSE named_struct('a', 2L) END AS result
        """
      Then query error (?i)(type|coerc)

    Scenario Outline: CASE merges nullable struct fields and exposes their widened type with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT result, reversed, result.a AS field, typeof(result) AS result_type
        FROM (
          SELECT CASE WHEN p THEN named_struct('a', n)
                      ELSE named_struct('a', CAST(n AS BIGINT)) END AS result,
                 CASE p WHEN true THEN named_struct('a', CAST(n AS BIGINT))
                        ELSE named_struct('a', n) END AS reversed
          FROM VALUES (true, 1), (false, 2), (false, NULL) AS t(p, n)
        )
        """
      Then query schema
        """
        root
         |-- result: struct (nullable = false)
         |    |-- a: long (nullable = true)
         |-- reversed: struct (nullable = false)
         |    |-- a: long (nullable = true)
         |-- field: long (nullable = true)
         |-- result_type: string (nullable = false)
        """
      And query result collected
        | result      | reversed    | field | result_type      |
        | Row(a=1)    | Row(a=1)    | 1     | struct<a:bigint> |
        | Row(a=2)    | Row(a=2)    | 2     | struct<a:bigint> |
        | Row(a=None) | Row(a=None) | NULL  | struct<a:bigint> |

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario Outline: CASE merges map keys and nullable values with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT result, reversed, typeof(result) AS result_type
        FROM (
          SELECT CASE WHEN n = 0 THEN map(1, CAST(NULL AS INT))
                      WHEN n = 1 THEN map(4294967296L, 2L) END AS result,
                 CASE n WHEN 0 THEN map(4294967296L, 2L)
                        WHEN 1 THEN map(1, CAST(NULL AS INT)) END AS reversed
          FROM VALUES (0), (1), (2) AS t(n)
        )
        """
      Then query schema
        """
        root
         |-- result: map (nullable = true)
         |    |-- key: long
         |    |-- value: long (valueContainsNull = true)
         |-- reversed: map (nullable = true)
         |    |-- key: long
         |    |-- value: long (valueContainsNull = true)
         |-- result_type: string (nullable = false)
        """
      And query result collected
        | result          | reversed        | result_type        |
        | {1: None}       | {4294967296: 2} | map<bigint,bigint> |
        | {4294967296: 2} | {1: None}       | map<bigint,bigint> |
        | NULL            | NULL            | map<bigint,bigint> |

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario: Legacy CASE promotes map keys to string when every key cast is safe
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CASE WHEN p THEN map('1', 10) WHEN NOT p THEN map(2, 20) END AS result
        FROM VALUES (true), (false) AS t(p)
        """
      Then query schema
        """
        root
         |-- result: map (nullable = true)
         |    |-- key: string
         |    |-- value: integer (valueContainsNull = false)
        """
      And query result collected
        | result    |
        | {'1': 10} |
        | {'2': 20} |

    Scenario: ANSI CASE rejects map key coercion that could introduce NULL keys
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN p THEN map('1', 10) WHEN NOT p THEN map(2, 20) END AS result
        FROM VALUES (true), (false) AS t(p)
        """
      Then query error (?i)(type|coerc)

    Scenario Outline: CASE retains nested nullability and widening from unreachable results with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CASE WHEN true THEN array(1) ELSE array(CAST(NULL AS INT)) END AS nullable_elements,
               CASE WHEN true THEN array(1) ELSE array(4294967296L) END AS widened,
               CASE WHEN true THEN array(array(1)) ELSE array(array(4294967296L)) END AS nested
        FROM VALUES (0) AS t(id)
        """
      Then query schema
        """
        root
         |-- nullable_elements: array (nullable = false)
         |    |-- element: integer (containsNull = true)
         |-- widened: array (nullable = false)
         |    |-- element: long (containsNull = false)
         |-- nested: array (nullable = false)
         |    |-- element: array (containsNull = false)
         |    |    |-- element: long (containsNull = false)
        """
      And query result collected
        | nullable_elements | widened | nested |
        | [1]               | [1]     | [[1]]  |

      Examples:
        | ansi  |
        | false |
        | true  |
