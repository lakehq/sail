Feature: Date comparisons beside subqueries preserve converted instants

  # A neighboring subquery changes when Spark applies its skipped-day cast rewrite.
  # It does not change the DATE's converted instant: Sail keeps equality TRUE for
  # Apia 2011-12-30 at 2011-12-31 midnight, while the Spark table records FALSE.
  # Only that incorrect optimizer result differs; the subquery result stays unchanged.

  Scenario Outline: Date comparison retains its converted instant beside a <kind>
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT d = TIMESTAMP '2011-12-31 00:00:00' AS eq,
             <subquery> AS other
      FROM VALUES(DATE '2011-12-30') AS t(d)
      """
    Then query result for Spark
      | eq    | other      |
      | false | <expected> |
    And query result for Sail
      | eq   | other      |
      | true | <expected> |

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
