Feature: Equivalence properties in distributed query plans
  Rule: Equivalence properties for window aggregation
    The physical optimizer may simplify sorting in the window specification
    if the input data satisfies certain equivalence properties.
    These equivalence properties should preserved when creating distributed query plans,
    otherwise query execution may fail.

    If the partition key is constant, the input does not need to be sorted by the
    `PARTITION BY` columns and only needs to be sorted by the `ORDER BY` columns.

    Scenario: Constant partition keys
      When query
        """
        SELECT g, x, first_value(x) OVER (
          PARTITION BY g ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS fx
        FROM (SELECT 'a' AS g, 0 AS x UNION ALL SELECT 'a', 1)
        """
      Then query result
        | g | x | fx |
        | a | 0 | 0  |
        | a | 1 | 0  |

    Scenario: Filtered partition keys
      When query
        """
        SELECT g, x, first_value(x) OVER (
          PARTITION BY g ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS fx
        FROM VALUES ('a', 0), ('b', 1) AS t(g, x)
        WHERE g = 'a'
        """
      Then query result
        | g | x | fx |
        | a | 0 | 0  |
