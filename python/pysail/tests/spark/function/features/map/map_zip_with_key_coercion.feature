@function(lambda)
Feature: map_zip_with coerces keys using Spark's wider types

  Background:
    Given config spark.sql.ansi.enabled = true

  Rule: Nullable nested casts do not make the outer map key nullable

    Scenario Outline: ANSI strings widen inside <case> map keys
      When query
        """
        SELECT map_values(map_zip_with(map(<left>, 1), map(<right>, 2),
                                       (k, x, y) -> x + y)) AS result
        """
      Then query result
        | result |
        | [3]    |

      Examples:
        | case                     | left                   | right                  |
        | array with string first  | array('1')             | array(1)               |
        | array with string last   | array(1)               | array('1')             |
        | struct with string first | named_struct('x', '1') | named_struct('x', 1)   |
        | struct with string last  | named_struct('x', 1)   | named_struct('x', '1') |

    Scenario: ANSI string coercion makes array key elements nullable
      When query
        """
        SELECT map_zip_with(map(array('1'), 1), map(array(1), 2),
                            (k, x, y) -> x + y) AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: array
         |    |    |-- element: long (containsNull = true)
         |    |-- value: integer (valueContainsNull = true)
        """

    Scenario: ANSI string coercion makes struct key fields nullable
      When query
        """
        SELECT map_zip_with(map(named_struct('x', '1'), 1), map(named_struct('x', 1), 2),
                            (k, x, y) -> x + y) AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: struct
         |    |    |-- x: long (nullable = true)
         |    |-- value: integer (valueContainsNull = true)
        """

  Rule: ANSI string to binary key casts cannot produce null

    Scenario Outline: ANSI string and binary keys merge with string <order>
      When query
        """
        SELECT map_values(map_zip_with(map(<left>, 1), map(<right>, 2),
                                       (k, x, y) -> x + y)) AS result
        """
      Then query result
        | result |
        | [3]    |

      Examples:
        | order | left        | right       |
        | first | '1'         | unhex('31') |
        | last  | unhex('31') | '1'         |

    Scenario: Legacy mode rejects string and binary key coercion
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT map_zip_with(map('1', 1), map(unhex('31'), 2), (k, x, y) -> x + y)
        """
      Then query error (?i)key|types

  Rule: Nullable casts remain invalid for outer map keys

    Scenario Outline: ANSI string and numeric outer keys are incompatible with string <order>
      When query
        """
        SELECT map_zip_with(map(<left>, 1), map(<right>, 2), (k, x, y) -> x + y)
        """
      Then query error (?i)key|types

      Examples:
        | order | left | right |
        | first | '1'  | 1     |
        | last  | 1    | '1'   |

  Rule: Comparison coercion does not allow incompatible map key families

    Scenario Outline: Incompatible <case> map keys are rejected with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT map_zip_with(map(<left>, 1), map(<right>, 2),
                            (k, x, y) -> coalesce(x, 0) + coalesce(y, 0))
        """
      Then query error (?i)key|types

      Examples:
        | case                  | ansi  | left               | right              |
        | date and integer      | true  | DATE'1970-01-02'   | 1                  |
        | integer and date      | true  | 1                  | DATE'1970-01-02'   |
        | date and integer      | false | DATE'1970-01-02'   | 1                  |
        | integer and date      | false | 1                  | DATE'1970-01-02'   |
        | mixed interval family | true  | INTERVAL '1' MONTH | INTERVAL '1' DAY   |
        | mixed interval family | true  | INTERVAL '1' DAY   | INTERVAL '1' MONTH |
        | mixed interval family | false | INTERVAL '1' MONTH | INTERVAL '1' DAY   |
        | mixed interval family | false | INTERVAL '1' DAY   | INTERVAL '1' MONTH |

  Rule: Map zip keys must support Spark ordering

    Scenario: Calendar interval map keys are rejected
      When query
        """
        SELECT map_values(map_zip_with(map(make_interval(0, 1, 0, 2, 0, 0, 0), 1),
                                       map(make_interval(0, 1, 0, 2, 0, 0, 0), 2),
                                       (k, x, y) -> x + y))
        """
      Then query error (?i)order|key|types
