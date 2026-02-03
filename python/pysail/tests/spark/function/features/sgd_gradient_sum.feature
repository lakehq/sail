Feature: sgd_gradient_sum function for distributed linear regression

  Rule: Basic gradient computation for y = 2x

    Scenario: Compute gradient with zero coefficients
      When query
      """
      SELECT sgd_gradient_sum(
          ARRAY(CAST(x AS DOUBLE)),
          CAST(y AS DOUBLE),
          ARRAY(CAST(0.0 AS DOUBLE))
      ) AS result
      FROM VALUES (1.0, 2.0), (2.0, 4.0), (3.0, 6.0) AS t(x, y)
      """
      Then query result
      | result             |
      | {[-28.0], 3, 56.0} |

  Rule: Gradient computation with optimal coefficients

    Scenario: Compute gradient at optimal solution (should be zero)
      When query
      """
      SELECT sgd_gradient_sum(
          ARRAY(CAST(x AS DOUBLE)),
          CAST(y AS DOUBLE),
          ARRAY(CAST(2.0 AS DOUBLE))
      ) AS result
      FROM VALUES (1.0, 2.0), (2.0, 4.0), (3.0, 6.0) AS t(x, y)
      """
      Then query result
      | result          |
      | {[0.0], 3, 0.0} |
