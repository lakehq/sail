Feature: Date comparisons preserve grouping-set provenance

  Scenario Outline: Date comparisons distinguish <grouping> from a foldable grouping key
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT count(*) AS row_count, count_if(eq) AS matches,
             count_if(eq IS NULL) AS nulls
      FROM (
        SELECT d = TIMESTAMP '2011-12-31 00:00:00' AS eq
        FROM (
          SELECT d, count(*) AS n
          FROM (SELECT DATE '2011-12-30' AS d FROM range(1))
          GROUP BY <grouping>
        )
      )
      """
    Then query result
      | row_count | matches   | nulls   |
      | <rows>    | <matches> | <nulls> |

    Examples:
      | grouping                  | rows | matches | nulls |
      | GROUPING SETS ((d))        | 1    | 0       | 0     |
      | GROUPING SETS ((d), ())    | 2    | 0       | 1     |
      | d                         | 1    | 1       | 0     |

  Scenario: Grouping sets preserve an independent date literal
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT d = TIMESTAMP '2011-12-31 00:00:00' AS eq
      FROM (
        SELECT DATE '2011-12-30' AS d, count(*) AS n
        FROM range(1) GROUP BY GROUPING SETS ((id), ())
      )
      """
    Then query result
      | eq   |
      | true |
      | true |
