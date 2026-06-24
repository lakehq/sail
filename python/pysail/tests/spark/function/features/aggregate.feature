@aggregate
Feature: aggregate higher-order function

  Rule: Array aggregation with lambda functions

    Scenario: aggregate sums integer array with identity finish
      When query
        """
        SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | 6      |

    Scenario: aggregate applies explicit finish lambda
      When query
        """
        SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x, acc -> acc * 10) AS result
        """
      Then query result
        | result |
        | 60     |

    Scenario: aggregate computes average with struct accumulator
      When query
        """
        SELECT aggregate(
          array(1, 2, 3, 4),
          named_struct('sum', 0, 'cnt', 0),
          (acc, x) -> named_struct('sum', acc.sum + x, 'cnt', acc.cnt + 1),
          acc -> acc.sum / acc.cnt
        ) AS avg
        """
      Then query result
        | avg |
        | 2.5 |

    Scenario: aggregate applies finish to initial value for empty array
      When query
        """
        SELECT aggregate(CAST(array() AS ARRAY<INT>), 0, (acc, x) -> acc + x, acc -> acc * 10) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: aggregate returns NULL for NULL array
      When query
        """
        SELECT aggregate(CAST(NULL AS ARRAY<INT>), 0, (acc, x) -> acc + x, acc -> acc * 10) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: aggregate handles NULL elements through merge lambda
      When query
        """
        SELECT aggregate(array(1, NULL, 3), 0, (acc, x) -> acc + coalesce(x, 0)) AS result
        """
      Then query result
        | result |
        | 4      |

    Scenario: aggregate can capture outer columns per row
      When query
        """
        SELECT aggregate(arr, base, (acc, x) -> acc + x) AS result
        FROM VALUES
          (array(1, 2), 10),
          (array(3), 20)
        AS t(arr, base)
        """
      Then query result
        | result |
        | 13     |
        | 23     |

    Scenario: aggregate supports struct accumulator and finish conversion
      When query
        """
        SELECT aggregate(
          array(
            CAST(20.0 AS DOUBLE),
            CAST(4.0 AS DOUBLE),
            CAST(2.0 AS DOUBLE),
            CAST(6.0 AS DOUBLE),
            CAST(10.0 AS DOUBLE)
          ),
          named_struct('count', 0, 'sum', CAST(0.0 AS DOUBLE)),
          (acc, x) -> named_struct('count', acc.count + 1, 'sum', acc.sum + x),
          acc -> acc.sum / acc.count
        ) AS result
        """
      Then query result
        | result |
        | 8.4    |

    Scenario: aggregate merge can reference the element without the accumulator
      When query
        """
        SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> x) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: aggregate merge references the element and a captured column
      When query
        """
        SELECT aggregate(arr, 0, (acc, x) -> x + base) AS result
        FROM VALUES
          (array(1, 2), 10),
          (array(3), 20)
        AS t(arr, base)
        """
      Then query result
        | result |
        | 12     |
        | 23     |

    Scenario: reduce is an alias for aggregate
      When query
        """
        SELECT reduce(array(1, 2, 3), 0, (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | 6      |
