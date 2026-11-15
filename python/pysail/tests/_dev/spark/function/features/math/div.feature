Feature: div output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to div yields the schema Spark declares
      When query
        """
        SELECT 3 div 2 AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

  Rule: Integral division widens operands before computing the quotient

    Scenario Outline: INT minimum divided by negative one fits BIGINT with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT -2147483648 DIV -1 AS result
        """
      Then query result
        | result     |
        | 2147483648 |
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: DIV coerces every CASE branch before widening with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT (CASE WHEN false THEN 0 ELSE 1.5 END) DIV 0.5 AS dividend_case,
               3 DIV (CASE WHEN false THEN 1 ELSE 0.5 END) AS divisor_case
        """
      Then query result
        | dividend_case | divisor_case |
        | 3             | 6            |
      When query
        """
        SELECT id,
               (CASE WHEN id = 0 THEN 0 ELSE 1.5 END) DIV 0.5 AS dividend_case,
               3 DIV (CASE WHEN id = 0 THEN 1 ELSE 0.5 END) AS divisor_case
        FROM range(2) ORDER BY id
        """
      Then query result ordered
        | id | dividend_case | divisor_case |
        | 0  | 0             | 3            |
        | 1  | 3             | 6            |

      Examples:
        | ansi  |
        | true  |
        | false |


    Scenario Outline: DIV follows projected CASE types through <boundary> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        <query>
        """
      Then query result
        | dividend_case | divisor_case |
        | 3             | 6            |

      Examples:
        | boundary | ansi  | query |
        | derived  | true  | SELECT v DIV 0.5 AS dividend_case, 3 DIV d AS divisor_case FROM (SELECT CASE WHEN false THEN 0 ELSE 1.5 END AS v, CASE WHEN false THEN 1 ELSE 0.5 END AS d) t |
        | derived  | false | SELECT v DIV 0.5 AS dividend_case, 3 DIV d AS divisor_case FROM (SELECT CASE WHEN false THEN 0 ELSE 1.5 END AS v, CASE WHEN false THEN 1 ELSE 0.5 END AS d) t |
        | CTE      | true  | WITH t AS (SELECT CASE WHEN false THEN 0 ELSE 1.5 END AS v, CASE WHEN false THEN 1 ELSE 0.5 END AS d) SELECT v DIV 0.5 AS dividend_case, 3 DIV d AS divisor_case FROM t |
        | CTE      | false | WITH t AS (SELECT CASE WHEN false THEN 0 ELSE 1.5 END AS v, CASE WHEN false THEN 1 ELSE 0.5 END AS d) SELECT v DIV 0.5 AS dividend_case, 3 DIV d AS divisor_case FROM t |
        | scalar   | true  | SELECT (SELECT CASE WHEN false THEN 0 ELSE 1.5 END) DIV 0.5 AS dividend_case, 3 DIV (SELECT CASE WHEN false THEN 1 ELSE 0.5 END) AS divisor_case |
        | scalar   | false | SELECT (SELECT CASE WHEN false THEN 0 ELSE 1.5 END) DIV 0.5 AS dividend_case, 3 DIV (SELECT CASE WHEN false THEN 1 ELSE 0.5 END) AS divisor_case |
        | UNION    | true  | SELECT v DIV 0.5 AS dividend_case, 3 DIV d AS divisor_case FROM (SELECT CASE WHEN false THEN 0 ELSE 1.5 END AS v, CASE WHEN false THEN 1 ELSE 0.5 END AS d UNION SELECT 1.5, 0.5) t |
        | UNION    | false | SELECT v DIV 0.5 AS dividend_case, 3 DIV d AS divisor_case FROM (SELECT CASE WHEN false THEN 0 ELSE 1.5 END AS v, CASE WHEN false THEN 1 ELSE 0.5 END AS d UNION SELECT 1.5, 0.5) t |

    Scenario Outline: DIV preserves grouped and shared aggregate expressions with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT (id + 0) DIV 2 AS q FROM range(4) GROUP BY id + 0 ORDER BY q
        """
      Then query result ordered
        | q |
        | 0 |
        | 0 |
        | 1 |
        | 1 |
      When query
        """
        SELECT sum(id + 0) AS s, sum(id + 0) DIV 2 AS q
        FROM range(4) HAVING sum(id + 0) > 0
        """
      Then query result
        | s | q |
        | 6 | 3 |
      When query
        """
        SELECT grouping(id + 0) DIV 1 AS g, grouping_id(id + 0) DIV 1 AS gid
        FROM range(2) GROUP BY ROLLUP(id + 0) ORDER BY g
        """
      Then query result ordered
        | g | gid |
        | 0 | 0   |
        | 0 | 0   |
        | 1 | 1   |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: DIV preserves shared ordered windows with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT sum(id) OVER (ORDER BY id + 0) AS s,
               sum(id) OVER (ORDER BY id + 0) DIV 2 AS q
        FROM range(4) ORDER BY id
        """
      Then query result ordered
        | s | q |
        | 0 | 0 |
        | 1 | 0 |
        | 3 | 1 |
        | 6 | 3 |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Projected <data_type> minimum DIV negative one widens with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT v DIV -1 AS result
        FROM VALUES (CAST(<minimum> AS <data_type>)) AS t(v)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | data_type | minimum     | result     | ansi  |
        | TINYINT   | -128        | 128        | true  |
        | TINYINT   | -128        | 128        | false |
        | SMALLINT  | -32768      | 32768      | true  |
        | SMALLINT  | -32768      | 32768      | false |
        | INT       | -2147483648 | 2147483648 | true  |
        | INT       | -2147483648 | 2147483648 | false |
