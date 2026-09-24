Feature: Set operations (INTERSECT, EXCEPT)

  Rule: INTERSECT DISTINCT

    Scenario: intersect distinct two tables
      When query
        """
        SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS a(id)
        INTERSECT
        SELECT * FROM (VALUES (3), (4), (5), (6), (7)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 3  |
        | 4  |
        | 5  |

    Scenario: intersect distinct removes duplicates
      When query
        """
        SELECT * FROM (VALUES (1), (1), (2), (2), (3)) AS a(id)
        INTERSECT DISTINCT
        SELECT * FROM (VALUES (1), (2), (2), (4)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |

    Scenario: intersect distinct three tables
      When query
        """
        SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS a(id)
        INTERSECT
        SELECT * FROM (VALUES (2), (3), (4), (5), (6)) AS b(id)
        INTERSECT
        SELECT * FROM (VALUES (3), (4), (5), (6), (7)) AS c(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 3  |
        | 4  |
        | 5  |

  # Note: INTERSECT ALL tests excluded — DataFusion's LogicalPlanBuilder::intersect(is_all=true)
  # produces extra duplicates compared to Spark. Pre-existing upstream bug.
  # https://github.com/apache/datafusion/issues/12955

  Rule: EXCEPT DISTINCT

    Scenario: except distinct two tables
      When query
        """
        SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS a(id)
        EXCEPT
        SELECT * FROM (VALUES (3), (4), (5), (6), (7)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |

    Scenario: except distinct removes duplicates
      When query
        """
        SELECT * FROM (VALUES (1), (1), (2), (2), (3)) AS a(id)
        EXCEPT DISTINCT
        SELECT * FROM (VALUES (2), (4)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 3  |

    Scenario: except distinct three tables
      When query
        """
        SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS a(id)
        EXCEPT
        SELECT * FROM (VALUES (4), (5), (6)) AS b(id)
        EXCEPT
        SELECT * FROM (VALUES (1), (7)) AS c(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 2  |
        | 3  |

  Rule: EXCEPT ALL

    Scenario: except all preserves duplicates
      When query
        """
        SELECT * FROM (VALUES (1), (1), (2), (2), (3)) AS a(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (1), (2)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
        | 3  |

    Scenario: except all three tables
      When query
        """
        SELECT * FROM (VALUES (1), (1), (1), (2), (2), (3), (3)) AS a(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (1), (2), (3)) AS b(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (1), (3)) AS c(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |

    Scenario: except all subtracts matching count
      When query
        """
        SELECT * FROM (VALUES (1), (1), (1)) AS a(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (1), (1)) AS b(id)
        """
      Then query result
        | id |
        | 1  |

  Rule: Wide table set operations

    Scenario: intersect distinct with multiple columns
      When query
        """
        SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS a(id, name)
        INTERSECT
        SELECT * FROM (VALUES (2, 'b'), (3, 'c'), (4, 'd')) AS b(id, name)
        ORDER BY id
        """
      Then query result ordered
        | id | name |
        | 2  | b    |
        | 3  | c    |

    Scenario: except all with multiple columns
      When query
        """
        SELECT * FROM (VALUES (1, 'a'), (1, 'a'), (2, 'b'), (3, 'c')) AS a(id, name)
        EXCEPT ALL
        SELECT * FROM (VALUES (1, 'a'), (3, 'c')) AS b(id, name)
        ORDER BY id
        """
      Then query result ordered
        | id | name |
        | 1  | a    |
        | 2  | b    |

  Rule: Null handling

    Scenario: intersect with nulls
      When query
        """
        SELECT * FROM (VALUES (1), (NULL), (3)) AS a(id)
        INTERSECT
        SELECT * FROM (VALUES (NULL), (3), (4)) AS b(id)
        ORDER BY id ASC NULLS LAST
        """
      Then query result ordered
        | id   |
        | 3    |
        | NULL |

    Scenario: except all with nulls
      When query
        """
        SELECT * FROM (VALUES (1), (NULL), (NULL), (3)) AS a(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (NULL), (3)) AS b(id)
        ORDER BY id ASC NULLS LAST
        """
      Then query result ordered
        | id   |
        | 1    |
        | NULL |

  Rule: Empty results

    Scenario: intersect with no common rows
      When query
        """
        SELECT * FROM (VALUES (1), (2)) AS a(id)
        INTERSECT
        SELECT * FROM (VALUES (3), (4)) AS b(id)
        """
      Then query result
        | id |

    Scenario: except all removing everything
      When query
        """
        SELECT * FROM (VALUES (1), (2)) AS a(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (1), (2), (3)) AS b(id)
        """
      Then query result
        | id |

  Rule: A set operation reconciles the column types of both sides

    Scenario: the wider type of the two sides is the type of the result
      When query
        """
        SELECT CAST(1 AS INT) AS a UNION ALL SELECT CAST(2 AS BIGINT) AS a
        """
      Then query schema
        """
        root
         |-- a: long (nullable = false)
        """

    @sail-bug
    Scenario: a value that cannot be cast to the reconciled type is rejected
      # The query IS rejected, and only the condition differs: the cast fails with DataFusion's
      # own message rather than with `CAST_INVALID_INPUT` (the TODO on `cast_to_spark_type` in
      # `resolver/expression/cast.rs`). On `main` the query is not rejected at all and answers
      # `1` and `x` under a schema that says the column is an integer.
      When query
        """
        SELECT 1 AS a UNION ALL SELECT 'x' AS a
        """
      Then query error CAST_INVALID_INPUT

  Rule: A set operation rejects a map-typed column

    Scenario: selecting distinct rows of a map column is rejected
      When query
        """
        SELECT DISTINCT m FROM (SELECT map('a', 1) AS m UNION ALL SELECT map('a', 1))
        """
      Then query error SET_OPERATION_ON_MAP_TYPE

  Rule: The type in the message is written the way Spark writes a type in SQL

    # Spark renders it with `toSQLType`, which is `DataType.sql`, and not with the upper cased
    # `simpleString` a plan schema is rendered with: a comma is followed by a space, and a struct
    # field keeps the case it was declared with and is quoted when it needs to be.
    Scenario Outline: the type of <case>
      When query
        """
        SELECT <value> AS m UNION SELECT <other>
        """
      Then query error is "<rendered>"\.

      Examples:
        | case             | value                 | other                 | rendered                      |
        | a map            | map('a', 1)           | map('b', 2)           | MAP<STRING, INT>              |
        | a map of maps    | map('a', map('b', 1)) | map('c', map('d', 2)) | MAP<STRING, MAP<STRING, INT>> |
        | an array of maps | array(map('a', 1))    | array(map('b', 2))    | ARRAY<MAP<STRING, INT>>       |

    # A struct literal builds fields that cannot be null, and the type says so with ` NOT NULL`.
    Scenario Outline: the type of <case> says which of its fields cannot be null
      When query
        """
        SELECT <value> AS m UNION SELECT <other>
        """
      Then query error is "<rendered>"\.

      Examples:
        | case                   | value                                  | other                                  | rendered                                              |
        | a struct of one field  | named_struct('MiCampo', map('a', 1))   | named_struct('MiCampo', map('b', 2))   | STRUCT<MiCampo: MAP<STRING, INT> NOT NULL>            |
        | a struct of two fields | named_struct('x', map('a', 1), 'y', 2) | named_struct('x', map('b', 2), 'y', 3) | STRUCT<x: MAP<STRING, INT> NOT NULL, y: INT NOT NULL> |
        | a field needing quotes | named_struct('mi campo', map('a', 1))  | named_struct('mi campo', map('b', 2))  | STRUCT<`mi campo`: MAP<STRING, INT> NOT NULL>         |

    Scenario Outline: <case> of a map column is rejected
      # Every operation that has to compare whole rows reaches the same check, including the ALL
      # forms, which keep the duplicates but still compare.
      When query
        """
        SELECT m FROM (SELECT map('a', 1) AS m) <operation> SELECT map('a', 1)
        """
      Then query error SET_OPERATION_ON_MAP_TYPE

      Examples:
        | case          | operation     |
        | a union       | UNION         |
        | an intersect  | INTERSECT     |
        | an intersect all | INTERSECT ALL |
        | an except     | EXCEPT        |
        | an except all | EXCEPT ALL    |

    Scenario: a union all of a map column is allowed
      # `UNION ALL` keeps every row as it is and compares nothing, so the map is not a problem.
      When query
        """
        SELECT m FROM (SELECT map('a', 1) AS m) UNION ALL SELECT map('b', 2)
        """
      Then query result
        | m          |
        | {a -> 1}   |
        | {b -> 2}   |

    Scenario: the map is found inside a struct
      When query
        """
        SELECT DISTINCT s FROM (SELECT named_struct('m', map('a', 1)) AS s)
        """
      Then query error SET_OPERATION_ON_MAP_TYPE

    Scenario: the map is found inside an array
      When query
        """
        SELECT DISTINCT a FROM (SELECT array(map('a', 1)) AS a)
        """
      Then query error SET_OPERATION_ON_MAP_TYPE

    Scenario: a map beside the columns that are compared is not a problem
      # Only the columns that are compared have to be ordered, so a map that rides along is fine.
      When query
        """
        SELECT DISTINCT k FROM (SELECT 1 AS k, map('a', 1) AS m)
        """
      Then query result
        | k |
        | 1 |

  Rule: The inputs widen to a decimal that the maximum precision can hold

    Scenario Outline: the integral digits are kept with ANSI <ansi>
      # Past the maximum precision the digits of the integral part are kept and the fraction is
      # cut, which rounds the value (`DecimalType.boundedPreferIntegralDigits`).
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(1.5 AS DECIMAL(38,20)) AS a
        UNION ALL
        SELECT CAST(2 AS DECIMAL(38,0)) AS a
        """
      Then query result
        | a |
        | 2 |
        | 2 |

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario Outline: the fraction is kept instead with the legacy setting and ANSI <ansi>
      # The setting is read where the type is bounded, so it applies whatever the mode
      # (`DecimalPrecisionTypeCoercion.bounded`).
      Given config spark.sql.legacy.decimal.retainFractionDigitsOnTruncate = true
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(1.5 AS DECIMAL(38,20)) AS a
        UNION ALL
        SELECT CAST(2 AS DECIMAL(38,0)) AS a
        """
      Then query result
        | a                      |
        | 1.50000000000000000000 |
        | 2.00000000000000000000 |

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario: a union that removes duplicates compares the widened values
      # Cutting the fraction makes the two values one.
      When query
        """
        SELECT CAST(1.5 AS DECIMAL(38,20)) AS a
        UNION
        SELECT CAST(2 AS DECIMAL(38,0)) AS a
        """
      Then query result
        | a |
        | 2 |

    Scenario: the legacy setting keeps the two values apart
      Given config spark.sql.legacy.decimal.retainFractionDigitsOnTruncate = true
      When query
        """
        SELECT CAST(1.5 AS DECIMAL(38,20)) AS a
        UNION
        SELECT CAST(2 AS DECIMAL(38,0)) AS a
        """
      Then query result
        | a                      |
        | 1.50000000000000000000 |
        | 2.00000000000000000000 |

    Scenario: an intersection finds the rounded value on both sides
      When query
        """
        SELECT CAST(1.5 AS DECIMAL(38,20)) AS a
        INTERSECT
        SELECT CAST(2 AS DECIMAL(38,0)) AS a
        """
      Then query result
        | a |
        | 2 |

    Scenario: an intersection finds nothing with the legacy setting
      Given config spark.sql.legacy.decimal.retainFractionDigitsOnTruncate = true
      When query
        """
        SELECT CAST(1.5 AS DECIMAL(38,20)) AS a
        INTERSECT
        SELECT CAST(2 AS DECIMAL(38,0)) AS a
        """
      Then query result
        | a |

    Scenario: a difference removes the rounded value
      When query
        """
        SELECT CAST(1.5 AS DECIMAL(38,20)) AS a
        EXCEPT ALL
        SELECT CAST(2 AS DECIMAL(38,0)) AS a
        """
      Then query result
        | a |

    Scenario: a difference keeps the value with the legacy setting
      Given config spark.sql.legacy.decimal.retainFractionDigitsOnTruncate = true
      When query
        """
        SELECT CAST(1.5 AS DECIMAL(38,20)) AS a
        EXCEPT ALL
        SELECT CAST(2 AS DECIMAL(38,0)) AS a
        """
      Then query result
        | a                      |
        | 1.50000000000000000000 |

    Scenario: a decimal that the maximum precision holds is not cut either way
      Given config spark.sql.legacy.decimal.retainFractionDigitsOnTruncate = true
      When query
        """
        SELECT CAST(1.5 AS DECIMAL(12,2)) AS a
        UNION ALL
        SELECT CAST(2 AS DECIMAL(10,0)) AS a
        """
      Then query result
        | a    |
        | 1.50 |
        | 2.00 |

    Scenario: the setting reaches a decimal inside an array
      Given config spark.sql.legacy.decimal.retainFractionDigitsOnTruncate = true
      When query
        """
        SELECT array(CAST(1.5 AS DECIMAL(38,20))) AS a
        UNION ALL
        SELECT array(CAST(2 AS DECIMAL(38,0))) AS a
        """
      Then query result
        | a                        |
        | [1.50000000000000000000] |
        | [2.00000000000000000000] |

    Scenario: the setting reaches a decimal inside a struct
      Given config spark.sql.legacy.decimal.retainFractionDigitsOnTruncate = true
      When query
        """
        SELECT named_struct('x', CAST(1.5 AS DECIMAL(38,20))) AS a
        UNION ALL
        SELECT named_struct('x', CAST(2 AS DECIMAL(38,0))) AS a
        """
      Then query result
        | a                        |
        | {1.50000000000000000000} |
        | {2.00000000000000000000} |

    Scenario: the setting reaches the value of a map
      Given config spark.sql.legacy.decimal.retainFractionDigitsOnTruncate = true
      When query
        """
        SELECT map('k', CAST(1.5 AS DECIMAL(38,20))) AS a
        UNION ALL
        SELECT map('k', CAST(2 AS DECIMAL(38,0))) AS a
        """
      Then query result
        | a                             |
        | {k -> 1.50000000000000000000} |
        | {k -> 2.00000000000000000000} |

    Scenario: the second input is the one that holds the fraction
      Given config spark.sql.legacy.decimal.retainFractionDigitsOnTruncate = true
      When query
        """
        SELECT CAST(2 AS DECIMAL(38,0)) AS a
        UNION ALL
        SELECT CAST(1.5 AS DECIMAL(38,20)) AS a
        """
      Then query result
        | a                      |
        | 1.50000000000000000000 |
        | 2.00000000000000000000 |

    Scenario: a decimal key that the wider type could turn NULL has no wider type
      # A map is refused rather than widened when its key would have to be cast
      # (`findTypeForComplex`).
      Given config spark.sql.legacy.decimal.retainFractionDigitsOnTruncate = true
      When query
        """
        SELECT map(CAST(1.5 AS DECIMAL(38,20)), 1) AS a
        UNION ALL
        SELECT map(CAST(2 AS DECIMAL(38,0)), 1) AS a
        """
      Then query error INCOMPATIBLE_COLUMN_TYPE

    @sail-bug
    Scenario: a value that does not fit the wider decimal is NULL without ANSI mode
      # TODO: Sail raises where Spark reads the value as NULL, since a cast that overflows is an
      #   error in Sail whatever the mode. The same query fails on `main` too.
      #
      #   This is not the widening of a set operation: the cast the union inserts fails the same
      #   way when it is written by hand, so the fix belongs to the cast and not here. Measured
      #   without ANSI mode, where Spark reads an overflow as NULL for a decimal and WRAPS it for
      #   an integral, and Sail raises for both:
      #     SELECT CAST(CAST(9999999999999999999 AS DECIMAL(38,0)) AS DECIMAL(38,20))  -- NULL
      #     SELECT CAST(99999 AS TINYINT)                                              -- -97
      Given config spark.sql.legacy.decimal.retainFractionDigitsOnTruncate = true
      And config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(9999999999999999999 AS DECIMAL(38,0)) AS a
        UNION ALL
        SELECT CAST(1.5 AS DECIMAL(38,20)) AS a
        """
      Then query result
        | a                      |
        | NULL                   |
        | 1.50000000000000000000 |

    @sail-bug
    Scenario: an integral value that does not fit the wider decimal is NULL as well
      # TODO: the same gap as above, in the cast rather than in the widening, reached here by
      #   widening an integral type into a decimal whose digits are all fraction.
      Given config spark.sql.legacy.decimal.retainFractionDigitsOnTruncate = true
      And config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(0.5 AS DECIMAL(38,38)) AS a
        UNION ALL
        SELECT CAST(2 AS INT) AS a
        """
      Then query result
        | a                                        |
        | 0.50000000000000000000000000000000000000 |
        | NULL                                     |

  Rule: Two intervals of one kind widen to the one that spans both

    Scenario Outline: <case> widen to <result>
      # `findWiderTypeForTwo` takes the fields both ends span, which Arrow does not carry in the
      # type itself, so they travel as metadata.
      When query
        """
        SELECT INTERVAL <left> AS a
        UNION ALL
        SELECT INTERVAL <right> AS a
        """
      Then query schema
        """
        root
         |-- a: <result> (nullable = false)
        """

      Examples:
        | case            | left            | right           | result                 |
        | year and months | '1' YEAR        | '1-2' YEAR TO MONTH | interval year to month |
        | the other way   | '1-2' YEAR TO MONTH | '1' YEAR    | interval year to month |
        | months and year | '2' MONTH       | '1' YEAR        | interval year to month |
        | days and hours  | '1' DAY         | '1 2' DAY TO HOUR | interval day to hour |
        | hours and minutes | '1' HOUR      | '2' MINUTE      | interval hour to minute |

    @spark-4
    Scenario: two intervals that span the same fields are left alone
      # The tag is for the client, not for the engine: an interval that spans one field is written
      # `interval year to year` by PySpark 3.5 and `interval year` from 4.0 on, and the scenarios
      # around this one all span two fields, which both write the same way.
      When query
        """
        SELECT INTERVAL '1' YEAR AS a
        UNION ALL
        SELECT INTERVAL '2' YEAR AS a
        """
      Then query schema
        """
        root
         |-- a: interval year (nullable = false)
        """

    Scenario: an intersection widens its intervals as well
      When query
        """
        SELECT INTERVAL '1' DAY AS a
        INTERSECT
        SELECT INTERVAL '1 0' DAY TO HOUR AS a
        """
      Then query schema
        """
        root
         |-- a: interval day to hour (nullable = false)
        """

    Scenario: an interval inside a struct widens too
      When query
        """
        SELECT named_struct('i', INTERVAL '1' YEAR) AS a
        UNION ALL
        SELECT named_struct('i', INTERVAL '1-2' YEAR TO MONTH) AS a
        """
      Then query schema
        """
        root
         |-- a: struct (nullable = false)
         |    |-- i: interval year to month (nullable = false)
        """

  Rule: A variant read as another type can hold NULL

    @spark-4
    Scenario: a variant and a string widen to a string that can be NULL
      # Reading a variant as another type can make a NULL, so what holds it can
      # (`Cast.forceNullable`, whose variant arm comes before the one for a string).
      # The tag is on the scenario rather than on the rule, since only this one needs a variant
      # and `parse_json` arrived in Spark 4.0.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT array(parse_json('1')) AS a
        UNION ALL
        SELECT array('x') AS a
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: string (containsNull = true)
        """

    Scenario: two strings widen to one that cannot
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT array('1') AS a
        UNION ALL
        SELECT array('x') AS a
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: string (containsNull = false)
        """
