Feature: map_from_entries output schema

  Rule: Duplicate key policy

    Scenario: duplicate keys raise an error under the default EXCEPTION policy
      When query
        """
        SELECT map_from_entries(array(struct(1, 'a'), struct(1, 'b'))) AS result
        """
      Then query error .*\[DUPLICATED_MAP_KEY\].*

    Scenario: LAST_WIN keeps the final value
      Given config spark.sql.mapKeyDedupPolicy = LAST_WIN
      When query
        """
        SELECT map_from_entries(array(struct(1, 'a'), struct(1, 'b'))) AS result
        """
      Then query result
        | result   |
        | {1 -> b} |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to map_from_entries yields the schema Spark declares
      When query
        """
        SELECT map_from_entries(array(struct(1, 'a'), struct(2, 'b'))) AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: integer
         |    |-- value: string (valueContainsNull = false)
        """

  Rule: Result values (migrated from test_map_from_entries.txt doctests)

    Scenario: map_from_entries doctest #1 (result)
      When query
        """
        SELECT map_from_entries(data) as result, typeof(map_from_entries(data)) as type
from VALUES
    (NULL),
    (
        array(struct(1 as a, 'b' as b),
        struct(2 as a, NULL as b),
        struct(3 as a, 'd' as b))
    ),
    (array()),
    (cast(NULL as array<struct<a: int, b: string>>))
as tab(data)
        """
      Then query result
        | result                      | type            |
        | NULL                        | map<int,string> |
        | {1 -> b, 2 -> NULL, 3 -> d} | map<int,string> |
        | {}                          | map<int,string> |
        | NULL                        | map<int,string> |

    Scenario: map_from_entries doctest #2 (result)
      When query
        """
        SELECT map_from_entries(data) as result, typeof(map_from_entries(data)) as type
from values
    (array(
        struct(1 as a, "a" as b),
        cast(NULL as struct<a: int, b: string>),
        cast(NULL as struct<a: int, b: string>)
    )),
    (NULL),
    (array(
        struct(2 as a, "b" as b),
        struct(3 as a, "c" as b)
    )),
    (array(
        struct(4 as a, "d" as b),
        cast(NULL as struct<a: int, b: string>),
        struct(5 as a, "e" as b),
        struct(6 as a, "f" as b)
    ))
as tab(data)
        """
      Then query result
        | result           | type            |
        | NULL             | map<int,string> |
        | NULL             | map<int,string> |
        | {2 -> b, 3 -> c} | map<int,string> |
        | NULL             | map<int,string> |
