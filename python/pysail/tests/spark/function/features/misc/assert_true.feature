Feature: assert_true output schema

  @function(nullability) @spark-4
  Rule: Output schema

    Scenario: a non-null literal input to assert_true yields the schema Spark declares
      When query
        """
        SELECT assert_true(0 < 1) AS result
        """
      Then query schema
        """
        root
         |-- result: void (nullable = true)
        """

    Scenario: a nullable column input to assert_true stays nullable
      When query
        """
        SELECT assert_true(c) AS result FROM VALUES (0 < 1), (CAST(NULL AS BOOLEAN)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: void (nullable = true)
        """

  Rule: Grouped assertions are evaluated only for produced rows

    Scenario: A grouped count assertion succeeds
      When query
        """
        SELECT assert_true(count(*) > 0) AS v
        FROM range(1)
        GROUP BY id
        """
      Then query result
        | v    |
        | NULL |

    Scenario: A false grouped assertion raises its message
      When query
        """
        SELECT assert_true(count(*) > 1, 'grouped assertion evaluated') AS v
        FROM range(1)
        GROUP BY id
        """
      Then query error grouped assertion evaluated

    Scenario Outline: An unexecuted grouped assertion does not raise: <case>
      When query
        """
        SELECT assert_true(count(*) > 1, 'unused grouped assertion') AS v
        FROM range(<rows>)
        GROUP BY id
        <limit>
        """
      Then query result
        | v |

      Examples:
        | case         | rows | limit   |
        | empty groups | 0    |         |
        | zero limit   | 1    | LIMIT 0 |
