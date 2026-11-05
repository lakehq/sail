Feature: nvl output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to nvl yields the schema Spark declares
      When query
        """
        SELECT nvl(NULL, array('2')) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = false)
        """

  Rule: The result has the common type of the two arguments, as with coalesce

    Scenario Outline: nvl keeps the type of <case>
      When query
        """
        SELECT nvl(<left>, <right>) AS result, typeof(nvl(<left>, <right>)) AS result_type
        """
      Then query result
        | result  | result_type |
        | <value> | <type>      |

      Examples:
        | case           | left                         | right                                                            | value                                   | type           |
        | DECIMAL(38,18) | CAST(NULL AS DECIMAL(38,18)) | CAST('12345678901234567890.123456789012345678' AS DECIMAL(38,18)) | 12345678901234567890.123456789012345678 | decimal(38,18) |
        | DATE           | CAST(NULL AS DATE)           | DATE '2026-01-02'                                                | 2026-01-02                              | date           |
        | ARRAY<INT>     | CAST(NULL AS ARRAY<INT>)     | array(1)                                                         | [1]                                     | array<int>     |
        | STRUCT<a:INT>  | CAST(NULL AS STRUCT<a:INT>)  | named_struct('a', 1)                                             | {1}                                     | struct<a:int>  |

    Scenario: nvl takes exactly two arguments
      When query
        """
        SELECT nvl(1, 2, 3) AS result
        """
      Then query error (?i)(two values expected|requires 2 parameters)
