@function(lambda)
Feature: map_zip_with respects floating-key modes and TIME key coercion

  Background:
    Given config spark.sql.ansi.enabled = true
    Given config spark.sql.mapZipWithUsesJavaCollections = true

  Rule: Legacy floating-key equality applies only to top-level keys

    Scenario Outline: Legacy map zip keeps each top-level <type> NaN key
      Given config spark.sql.mapZipWithUsesJavaCollections = false
      When query
        """
        SELECT map_values(map_zip_with(map(CAST('NaN' AS <type>), 1),
                                       map(CAST('NaN' AS <type>), 2),
                                       (k, x, y) -> coalesce(x, 0) + coalesce(y, 0))) AS result
        """
      Then query result
        | result |
        | [1, 2] |

      Examples:
        | type   |
        | FLOAT  |
        | DOUBLE |

    Scenario Outline: Legacy map zip still merges NaNs in <shape> keys
      Given config spark.sql.mapZipWithUsesJavaCollections = false
      When query
        """
        SELECT map_values(map_zip_with(map(<key>, 1), map(<key>, 2),
                                       (k, x, y) -> x + y)) AS result
        """
      Then query result
        | result |
        | [3]    |

      Examples:
        | shape         | key                                   |
        | float array   | array(CAST('NaN' AS FLOAT))            |
        | double array  | array(CAST('NaN' AS DOUBLE))           |
        | float struct  | named_struct('x', CAST('NaN' AS FLOAT)) |
        | double struct | named_struct('x', CAST('NaN' AS DOUBLE))|

  Rule: TIME keys use Spark's string promotion rules

    Scenario Outline: Nested TIME and string keys merge in <shape> keys with ANSI <ansi>
      Given config spark.sql.timeType.enabled = true
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT map_values(map_zip_with(map(<left>, 1), map(<right>, 2),
                                       (k, x, y) -> x + y)) AS result
        """
      Then query result
        | result |
        | [3]    |

      Examples:
        | shape               | ansi  | left                                      | right                                      |
        | array time first    | true  | array(make_time(1, 2, 3.123456))            | array('01:02:03.123456')                    |
        | array time last     | true  | array('01:02:03.123456')                    | array(make_time(1, 2, 3.123456))            |
        | struct time first   | true  | named_struct('x', make_time(1, 2, 3.123456)) | named_struct('x', '01:02:03.123456')         |
        | struct time last    | true  | named_struct('x', '01:02:03.123456')         | named_struct('x', make_time(1, 2, 3.123456)) |
        | array time first    | false | array(make_time(1, 2, 3.123456))            | array('01:02:03.123456')                    |
        | array time last     | false | array('01:02:03.123456')                    | array(make_time(1, 2, 3.123456))            |
        | struct time first   | false | named_struct('x', make_time(1, 2, 3.123456)) | named_struct('x', '01:02:03.123456')         |
        | struct time last    | false | named_struct('x', '01:02:03.123456')         | named_struct('x', make_time(1, 2, 3.123456)) |

    Scenario: Legacy map zip widens top-level TIME keys to strings
      Given config spark.sql.timeType.enabled = true
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT map_zip_with(map(make_time(1, 2, 3.123456), 1), map('01:02:03.123456', 2),
                            (k, x, y) -> x + y) AS result
        """
      Then query result
        | result                  |
        | {01:02:03.123456 -> 3} |

    Scenario: ANSI map zip rejects a nullable outer string-to-TIME cast
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT map_zip_with(map(make_time(1, 2, 3), 1), map('01:02:03', 2),
                            (k, x, y) -> x + y)
        """
      Then query error (?i)key|types
