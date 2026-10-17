Feature: Date comparisons beside subqueries preserve Spark optimization order

  Scenario Outline: A <kind> prevents early local date comparison evaluation
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT d = TIMESTAMP '2011-12-31 00:00:00' AS eq,
             <subquery> AS other
      FROM VALUES(DATE '2011-12-30') AS t(d)
      """
    Then query result
      | eq    | other      |
      | false | <expected> |

    Examples:
      | kind              | subquery                          | expected |
      | scalar literal    | (SELECT 1)                        | 1        |
      | scalar aggregate  | (SELECT max(id) FROM range(1))    | 0        |
      | existence test    | EXISTS(SELECT 1 FROM range(1))    | true     |

  Scenario: A correlated filter preserves early local date comparison evaluation
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT (
        SELECT d = TIMESTAMP '2011-12-31 00:00:00'
        FROM VALUES(DATE '2011-12-30') AS t(d) WHERE id = 0
      ) AS eq
      FROM range(1)
      """
    Then query result
      | eq   |
      | true |
