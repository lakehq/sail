Feature: what CAST(... AS STRING) prints, and where it parts from show

  # Spark prints a value two different ways on purpose. `show` goes through `ToPrettyString`,
  # which writes a null inside a collection as `NULL`; `CAST(... AS STRING)` goes through
  # `Cast`, which writes the same null as `null` (`ToStringBase.scala`, the two implementations
  # of `nullString`). So one row holds both spellings, and a test that looks at a single lens
  # cannot tell an engine that renders one of them wrong.
  # Measured on the Spark 4.2 JVM over Spark Connect.

  Rule: a null inside a collection is NULL in show and null in a cast

    @sail-bug
    Scenario Outline: <case>
      When query template
        """
        SELECT <expr> AS v
        """
      Then stored and printed result
        | stored | type   | shown   | cast   |
        | -      | <type> | <shown> | <cast> |

      Examples:
        | case                                 | expr                                            | type                   | shown        | cast         |
        | an array with a null                 | array(1, NULL, 3)                               | array<int>             | [1, NULL, 3] | [1, null, 3] |
        | an array of strings with a null      | array('a', NULL)                                | array<string>          | [a, NULL]    | [a, null]    |
        | an array of only nulls               | array(CAST(NULL AS INT))                        | array<int>             | [NULL]       | [null]       |
        | a nested array with a null           | array(array(1, NULL))                           | array<array<int>>      | [[1, NULL]]  | [[1, null]]  |
        | a struct with a null field           | named_struct('a', CAST(NULL AS INT))            | struct<a:int>          | {NULL}       | {null}       |
        | a struct with a null among values    | named_struct('a', 1, 'b', CAST(NULL AS STRING)) | struct<a:int,b:string> | {1, NULL}    | {1, null}    |
        | a map with a null value              | map('k', CAST(NULL AS INT))                     | map<string,int>        | {k -> NULL}  | {k -> null}  |
        | a struct inside an array with a null | array(named_struct('a', CAST(NULL AS INT)))     | array<struct<a:int>>   | [{NULL}]     | [{null}]     |

  # A Spark string is a byte string: `Cast` hands the bytes over untouched, so casting binary
  # that is not valid UTF-8 gives a string holding those very bytes rather than an error.
  Rule: casting binary to string keeps the bytes

    Scenario: binary that is valid UTF-8
      When query
        """
        SELECT CAST(CAST('abc' AS BINARY) AS STRING) AS result
        """
      Then query result collected
        | result |
        | abc    |

    @sail-bug
    Scenario: binary that is not valid UTF-8
      When query
        """
        SELECT hex(CAST(X'00FF10' AS STRING)) AS hex, length(CAST(X'00FF10' AS STRING)) AS len
        """
      Then query result collected
        | hex    | len |
        | 00FF10 | 3   |
