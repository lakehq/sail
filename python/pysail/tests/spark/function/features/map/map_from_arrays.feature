Feature: map_from_arrays output schema

  Rule: Duplicate key policy

    Scenario: duplicate keys raise an error under the default EXCEPTION policy
      When query
        """
        SELECT map_from_arrays(array(1, 1), array('a', 'b')) AS result
        """
      Then query error .*\[DUPLICATED_MAP_KEY\].*

    Scenario: LAST_WIN keeps the final value
      Given config spark.sql.mapKeyDedupPolicy = LAST_WIN
      When query
        """
        SELECT map_from_arrays(array(1, 1), array('a', 'b')) AS result
        """
      Then query result
        | result   |
        | {1 -> b} |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to map_from_arrays yields the schema Spark declares
      When query
        """
        SELECT map_from_arrays(array(1.0, 3.0), array('2', '4')) AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: decimal(2,1)
         |    |-- value: string (valueContainsNull = false)
        """

    Scenario: a nullable column input to map_from_arrays stays nullable
      When query
        """
        SELECT map_from_arrays(c, array('2', '4')) AS result FROM VALUES (array(1.0, 3.0)), (CAST(NULL AS ARRAY<DECIMAL(2,1)>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: map (nullable = true)
         |    |-- key: decimal(2,1)
         |    |-- value: string (valueContainsNull = false)
        """

  Rule: Result values (migrated from test_map_from_arrays.txt doctests)

    Scenario: map_from_arrays doctest #1 (result)
      When query
        """
        select map_from_arrays(a, b) as result, typeof(map_from_arrays(a, b)) as type
   from values
       (array(1), array('a')),
       (NULL, NULL),
       (array(1,2,3), NULL),
       (NULL, array('b', 'c')),
       (array(4, 5), array('d', 'e')),
       (array(), array()),
       (array(6, 7, 8), array('f', 'g', 'h'))
   as tab(a, b)
        """
      Then query result
        | result                   | type            |
        | {1 -> a}                 | map<int,string> |
        | NULL                     | map<int,string> |
        | NULL                     | map<int,string> |
        | NULL                     | map<int,string> |
        | {4 -> d, 5 -> e}         | map<int,string> |
        | {}                       | map<int,string> |
        | {6 -> f, 7 -> g, 8 -> h} | map<int,string> |

    Scenario: map_from_arrays doctest #2 (result)
      When query
        """
        select map_from_arrays(NULL, NULL) as result, typeof(map_from_arrays(NULL, NULL)) as type
        """
      Then query result
        | result | type           |
        | NULL   | map<void,void> |
