@function(lambda)
Feature: higher-order functions reject a wrong number of arguments

  Rule: No arguments

    Scenario Outline: wrong count with no arguments: <call>
      When query
        """
        SELECT <call>
        """
      Then query error \[WRONG_NUM_ARGS.*The `<name>` requires <accepted> parameters but the actual number is <actual>

      Examples:
        | call               | name             | accepted | actual |
        | transform()        | transform        | 2        | 0      |
        | filter()           | filter           | 2        | 0      |
        | exists()           | exists           | 2        | 0      |
        | forall()           | forall           | 2        | 0      |
        | map_filter()       | map_filter       | 2        | 0      |
        | array_sort()       | array_sort       | .1, 2.   | 0      |
        | aggregate()        | aggregate        | .3, 4.   | 0      |
        | reduce()           | reduce           | .3, 4.   | 0      |
        | transform_keys()   | transform_keys   | 2        | 0      |
        | transform_values() | transform_values | 2        | 0      |

  Rule: Plain value arguments

    Scenario Outline: wrong count with plain arguments: <call>
      When query
        """
        SELECT <call>
        """
      Then query error \[WRONG_NUM_ARGS.*The `<name>` requires <accepted> parameters but the actual number is <actual>

      Examples:
        | call                                 | name             | accepted | actual |
        | transform(array(1))                  | transform        | 2        | 1      |
        | transform(array(1), 1, 1)            | transform        | 2        | 3      |
        | transform(array(1), 1, 1, 1)         | transform        | 2        | 4      |
        | transform(array(1), 1, 1, 1, 1)      | transform        | 2        | 5      |
        | filter(array(1))                     | filter           | 2        | 1      |
        | filter(array(1), 1, 1)               | filter           | 2        | 3      |
        | filter(array(1), 1, 1, 1)            | filter           | 2        | 4      |
        | filter(array(1), 1, 1, 1, 1)         | filter           | 2        | 5      |
        | exists(array(1))                     | exists           | 2        | 1      |
        | exists(array(1), 1, 1)               | exists           | 2        | 3      |
        | exists(array(1), 1, 1, 1)            | exists           | 2        | 4      |
        | exists(array(1), 1, 1, 1, 1)         | exists           | 2        | 5      |
        | forall(array(1))                     | forall           | 2        | 1      |
        | forall(array(1), 1, 1)               | forall           | 2        | 3      |
        | forall(array(1), 1, 1, 1)            | forall           | 2        | 4      |
        | forall(array(1), 1, 1, 1, 1)         | forall           | 2        | 5      |
        | map_filter(map(1, 2))                | map_filter       | 2        | 1      |
        | map_filter(map(1, 2), 1, 1)          | map_filter       | 2        | 3      |
        | map_filter(map(1, 2), 1, 1, 1)       | map_filter       | 2        | 4      |
        | map_filter(map(1, 2), 1, 1, 1, 1)    | map_filter       | 2        | 5      |
        | array_sort(array(1), 1, 1)           | array_sort       | .1, 2.   | 3      |
        | array_sort(array(1), 1, 1, 1)        | array_sort       | .1, 2.   | 4      |
        | array_sort(array(1), 1, 1, 1, 1)     | array_sort       | .1, 2.   | 5      |
        | aggregate(array(1))                  | aggregate        | .3, 4.   | 1      |
        | aggregate(array(1), 1)               | aggregate        | .3, 4.   | 2      |
        | aggregate(array(1), 1, 1, 1, 1)      | aggregate        | .3, 4.   | 5      |
        | reduce(array(1))                     | reduce           | .3, 4.   | 1      |
        | reduce(array(1), 1)                  | reduce           | .3, 4.   | 2      |
        | reduce(array(1), 1, 1, 1, 1)         | reduce           | .3, 4.   | 5      |
        | transform_keys(map(1, 2))            | transform_keys   | 2        | 1      |
        | transform_keys(map(1, 2), 1, 1)      | transform_keys   | 2        | 3      |
        | transform_keys(map(1, 2), 1, 1, 1)   | transform_keys   | 2        | 4      |
        | transform_values(map(1, 2))          | transform_values | 2        | 1      |
        | transform_values(map(1, 2), 1, 1)    | transform_values | 2        | 3      |
        | transform_values(map(1, 2), 1, 1, 1) | transform_values | 2        | 4      |

  Rule: Lambda arguments

    Scenario Outline: wrong count with lambda arguments: <call>
      When query
        """
        SELECT <call>
        """
      Then query error \[WRONG_NUM_ARGS.*The `<name>` requires <accepted> parameters but the actual number is <actual>

      Examples:
        | call                                                              | name             | accepted | actual |
        | transform(array(1), x -> true, x -> true)                         | transform        | 2        | 3      |
        | transform(array(1), x -> true, x -> true, x -> true)              | transform        | 2        | 4      |
        | transform(array(1), x -> true, x -> true, x -> true, x -> true)   | transform        | 2        | 5      |
        | filter(array(1), x -> true, x -> true)                            | filter           | 2        | 3      |
        | filter(array(1), x -> true, x -> true, x -> true)                 | filter           | 2        | 4      |
        | filter(array(1), x -> true, x -> true, x -> true, x -> true)      | filter           | 2        | 5      |
        | exists(array(1), x -> true, x -> true)                            | exists           | 2        | 3      |
        | exists(array(1), x -> true, x -> true, x -> true)                 | exists           | 2        | 4      |
        | exists(array(1), x -> true, x -> true, x -> true, x -> true)      | exists           | 2        | 5      |
        | forall(array(1), x -> true, x -> true)                            | forall           | 2        | 3      |
        | forall(array(1), x -> true, x -> true, x -> true)                 | forall           | 2        | 4      |
        | forall(array(1), x -> true, x -> true, x -> true, x -> true)      | forall           | 2        | 5      |
        | map_filter(map(1, 2), x -> true, x -> true)                       | map_filter       | 2        | 3      |
        | map_filter(map(1, 2), x -> true, x -> true, x -> true)            | map_filter       | 2        | 4      |
        | map_filter(map(1, 2), x -> true, x -> true, x -> true, x -> true) | map_filter       | 2        | 5      |
        | array_sort(array(1), x -> true, x -> true)                        | array_sort       | .1, 2.   | 3      |
        | array_sort(array(1), x -> true, x -> true, x -> true)             | array_sort       | .1, 2.   | 4      |
        | array_sort(array(1), x -> true, x -> true, x -> true, x -> true)  | array_sort       | .1, 2.   | 5      |
        | aggregate(array(1), x -> true)                                    | aggregate        | .3, 4.   | 2      |
        | aggregate(array(1), x -> true, x -> true, x -> true, x -> true)   | aggregate        | .3, 4.   | 5      |
        | reduce(array(1), x -> true)                                       | reduce           | .3, 4.   | 2      |
        | reduce(array(1), x -> true, x -> true, x -> true, x -> true)      | reduce           | .3, 4.   | 5      |
        | transform_keys(map(1, 2), (k, v) -> k, (k, v) -> v)               | transform_keys   | 2        | 3      |
        | transform_values(map(1, 2), (k, v) -> k, (k, v) -> v)             | transform_values | 2        | 3      |

  Rule: Subquery arguments: the count is checked first

    Scenario Outline: wrong count with subquery arguments: <call>
      When query
        """
        SELECT <call>
        """
      Then query error \[WRONG_NUM_ARGS.*The `<name>` requires <accepted> parameters but the actual number is <actual>

      Examples:
        | call                                                                  | name             | accepted | actual |
        | transform(array(1), (SELECT 1), (SELECT 1))                           | transform        | 2        | 3      |
        | transform(array(1), (SELECT 1), (SELECT 1), (SELECT 1))               | transform        | 2        | 4      |
        | transform(array(1), (SELECT 1), (SELECT 1), (SELECT 1), (SELECT 1))   | transform        | 2        | 5      |
        | filter(array(1), (SELECT 1), (SELECT 1))                              | filter           | 2        | 3      |
        | filter(array(1), (SELECT 1), (SELECT 1), (SELECT 1))                  | filter           | 2        | 4      |
        | filter(array(1), (SELECT 1), (SELECT 1), (SELECT 1), (SELECT 1))      | filter           | 2        | 5      |
        | exists(array(1), (SELECT 1), (SELECT 1))                              | exists           | 2        | 3      |
        | exists(array(1), (SELECT 1), (SELECT 1), (SELECT 1))                  | exists           | 2        | 4      |
        | exists(array(1), (SELECT 1), (SELECT 1), (SELECT 1), (SELECT 1))      | exists           | 2        | 5      |
        | forall(array(1), (SELECT 1), (SELECT 1))                              | forall           | 2        | 3      |
        | forall(array(1), (SELECT 1), (SELECT 1), (SELECT 1))                  | forall           | 2        | 4      |
        | forall(array(1), (SELECT 1), (SELECT 1), (SELECT 1), (SELECT 1))      | forall           | 2        | 5      |
        | map_filter(map(1, 2), (SELECT 1), (SELECT 1))                         | map_filter       | 2        | 3      |
        | map_filter(map(1, 2), (SELECT 1), (SELECT 1), (SELECT 1))             | map_filter       | 2        | 4      |
        | map_filter(map(1, 2), (SELECT 1), (SELECT 1), (SELECT 1), (SELECT 1)) | map_filter       | 2        | 5      |
        | array_sort(array(1), (SELECT 1), (SELECT 1))                          | array_sort       | .1, 2.   | 3      |
        | array_sort(array(1), (SELECT 1), (SELECT 1), (SELECT 1))              | array_sort       | .1, 2.   | 4      |
        | array_sort(array(1), (SELECT 1), (SELECT 1), (SELECT 1), (SELECT 1))  | array_sort       | .1, 2.   | 5      |
        | aggregate(array(1), (SELECT 1))                                       | aggregate        | .3, 4.   | 2      |
        | aggregate(array(1), (SELECT 1), (SELECT 1), (SELECT 1), (SELECT 1))   | aggregate        | .3, 4.   | 5      |
        | reduce(array(1), (SELECT 1))                                          | reduce           | .3, 4.   | 2      |
        | reduce(array(1), (SELECT 1), (SELECT 1), (SELECT 1), (SELECT 1))      | reduce           | .3, 4.   | 5      |
        | transform_keys(map(1, 2), (SELECT 1), (SELECT 1))                     | transform_keys   | 2        | 3      |
        | transform_values(map(1, 2), (SELECT 1), (SELECT 1))                   | transform_values | 2        | 3      |

  Rule: The function name is matched in any letter case

    Scenario Outline: wrong count with letter case: <call>
      When query
        """
        SELECT <call>
        """
      Then query error \[WRONG_NUM_ARGS.*The `<name>` requires <accepted> parameters but the actual number is <actual>

      Examples:
        | call                                | name             | accepted | actual |
        | TRANSFORM(array(1), 1, 2, 3, 4, 5)  | transform        | 2        | 6      |
        | Transform(array(1), 1, 2, 3, 4, 5)  | transform        | 2        | 6      |
        | ARRAY_SORT(array(1), 1, 2, 3, 4, 5) | array_sort       | .1, 2.   | 6      |
        | Reduce(array(1), 1, 2, 3, 4, 5)     | reduce           | .3, 4.   | 6      |
        | TRANSFORM_KEYS(map(1, 2), 1, 1)     | transform_keys   | 2        | 3      |
        | TRANSFORM_VALUES(map(1, 2), 1, 1)   | transform_values | 2        | 3      |
