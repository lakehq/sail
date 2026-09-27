@function(lambda)
Feature: Zip functions follow Spark's subquery analyzer rules

  @spark-4
  Scenario Outline: <function> rejects a subquery in the <position>
    Given config spark.sql.analyzer.allowSubqueryExpressionsInLambdasOrHigherOrderFunctions = false
    When query
      """
      SELECT <function>(<arguments>) AS result
      """
    Then query error (?i)subquery.*higher-order

    Examples:
      | function     | position           | arguments                                                          |
      | zip_with     | scalar lambda body | array(1), array(2), (x, y) -> (SELECT 7)                            |
      | zip_with     | exists lambda body | array(1), array(2), (x, y) -> EXISTS(SELECT 1)                       |
      | zip_with     | in lambda body     | array(1), array(2), (x, y) -> 1 IN (SELECT 1)                        |
      | zip_with     | plain body         | array(1), array(2), (SELECT 7)                                      |
      | zip_with     | left collection    | (SELECT array(1)), array(2), (x, y) -> x + y                        |
      | zip_with     | right collection   | array(1), (SELECT array(2)), (x, y) -> x + y                        |
      | zip_with     | typeof lambda body | array(1), array(2), (x, y) -> typeof((SELECT 7))                    |
      | zip_with     | typeof plain body  | array(1), array(2), typeof((SELECT 7))                              |
      | map_zip_with | scalar lambda body | map(1, 2), map(1, 3), (k, x, y) -> (SELECT 7)                       |
      | map_zip_with | exists lambda body | map(1, 2), map(1, 3), (k, x, y) -> EXISTS(SELECT 1)                  |
      | map_zip_with | in lambda body     | map(1, 2), map(1, 3), (k, x, y) -> 1 IN (SELECT 1)                   |
      | map_zip_with | plain body         | map(1, 2), map(1, 3), (SELECT 7)                                    |
      | map_zip_with | left collection    | (SELECT map(1, 2)), map(1, 3), (k, x, y) -> x + y                   |
      | map_zip_with | right collection   | map(1, 2), (SELECT map(1, 3)), (k, x, y) -> x + y                   |
      | map_zip_with | typeof lambda body | map(1, 2), map(1, 3), (k, x, y) -> typeof((SELECT 7))               |
      | map_zip_with | typeof plain body  | map(1, 2), map(1, 3), typeof((SELECT 7))                            |

  Scenario Outline: <function> preserves the legacy subquery opt-out in the <position>
    Given config spark.sql.analyzer.allowSubqueryExpressionsInLambdasOrHigherOrderFunctions = true
    When query
      """
      SELECT <function>(<arguments>) AS result
      """
    Then query result
      | result   |
      | <result> |

    Examples:
      | function     | position        | arguments                                                | result   |
      | zip_with     | lambda body     | array(1), array(2), (x, y) -> (SELECT 7)                  | [7]      |
      | zip_with     | collection      | (SELECT array(1)), array(2), (x, y) -> x + y              | [3]      |
      | map_zip_with | lambda body     | map(1, 2), map(1, 3), (k, x, y) -> (SELECT 7)             | {1 -> 7} |
      | map_zip_with | collection      | (SELECT map(1, 2)), map(1, 3), (k, x, y) -> x + y         | {1 -> 5} |

  Scenario Outline: <function> leaves enclosing and sibling subqueries valid
    When query
      """
      SELECT (SELECT <function>(<arguments>)) AS nested,
             <function>(<arguments>) AS zipped,
             (SELECT 7) AS sibling
      """
    Then query result
      | nested   | zipped   | sibling |
      | <result> | <result> | 7       |

    Examples:
      | function     | arguments                                     | result   |
      | zip_with     | array(1), array(2), (x, y) -> x + y             | [3]      |
      | map_zip_with | map(1, 2), map(1, 3), (k, x, y) -> x + y        | {1 -> 5} |

  Scenario Outline: <function> accepts collection columns from a derived table
    When query
      """
      SELECT <function>(a, b, <lambda>) AS result
      FROM (SELECT <left> AS a, <right> AS b)
      """
    Then query result
      | result   |
      | <result> |

    Examples:
      | function     | left      | right     | lambda             | result   |
      | zip_with     | array(1)  | array(2)  | (x, y) -> x + y    | [3]      |
      | map_zip_with | map(1, 2) | map(1, 3) | (k, x, y) -> x + y | {1 -> 5} |
