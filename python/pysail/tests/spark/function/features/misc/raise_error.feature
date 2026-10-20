Feature: raise_error output schema

  Rule: Grouped error expressions preserve execution and validation

    Scenario Outline: An unused grouped raise_error branch is accepted: <case>
      When query
        """
        SELECT <expression> AS v
        FROM range(1)
        GROUP BY id
        """
      Then query result
        | v |
        | 1 |

      Examples:
        | case               | expression                                                                   |
        | aggregate result   | CASE WHEN count(*) > 0 THEN 1 ELSE raise_error('unused grouped error') END     |
        | coalesce result    | coalesce(sum(id) + 1, raise_error('unused grouped error'))                     |
        | aggregate argument | max(CASE WHEN id = 0 THEN 1 ELSE raise_error('unused grouped error') END)      |

    Scenario: An executed grouped raise_error retains its message
      When query
        """
        SELECT raise_error('grouped error evaluated') AS v
        FROM range(1)
        GROUP BY id
        """
      Then query error grouped error evaluated

    Scenario: A grouped raise_error still checks its volatile argument
      When query
        """
        SELECT max(CAST(raise_error(CAST(rand(1) AS STRING)) AS INT)) AS v
        FROM range(1)
        GROUP BY id
        """
      Then query error (?i)(non-deterministic|nondeterministic)

    Scenario: A grouped assertion does not exempt another volatile aggregate argument
      When query
        """
        SELECT assert_true(count(*) > 0) AS v, max(rand(1)) AS random_value
        FROM range(1)
        GROUP BY id
        """
      Then query error (?i)(non-deterministic|nondeterministic)

  @function(nullability) @spark-4
  Rule: Output schema

    Scenario: a non-null literal input to raise_error yields the schema Spark declares
      When query
        """
        SELECT raise_error('custom error message') AS result
        """
      Then query schema
        """
        root
         |-- result: void (nullable = true)
        """

    Scenario: a non-null column input to raise_error yields the schema Spark declares
      When query
        """
        SELECT raise_error(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: void (nullable = true)
        """

    Scenario: a nullable column input to raise_error stays nullable
      When query
        """
        SELECT raise_error(c) AS result FROM VALUES ('custom error message'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: void (nullable = true)
        """
