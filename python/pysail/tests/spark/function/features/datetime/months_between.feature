Feature: months_between output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to months_between yields the schema Spark declares
      When query
        """
        SELECT months_between('1997-02-28 10:30:00', '1996-10-30') AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to months_between yields the schema Spark declares
      When query
        """
        SELECT months_between(CAST(id AS STRING), '1996-10-30') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to months_between stays nullable
      When query
        """
        SELECT months_between(c, '1996-10-30') AS result FROM VALUES ('1997-02-28 10:30:00'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

  Rule: Month fractions use the earliest valid start of each local day

    Scenario Outline: elapsed seconds since local day start: <case>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT months_between(TIMESTAMP '<left>', TIMESTAMP '<right>', false) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                          | zone                | left                | right               | result              |
        | wide year                     | UTC                 | 1000-03-10 12:00:00 | 1000-02-09 12:00:00 | 1.032258064516129   |
        | spring gap                    | America/Los_Angeles | 2024-03-10 12:00:00 | 2024-02-09 12:00:00 | 1.0309139784946237  |
        | repeated midnight             | America/Havana      | 2015-11-01 12:00:00 | 2015-10-02 12:00:00 | 0.9690860215053764  |
        | repeated midnight reversed    | America/Havana      | 2015-10-02 12:00:00 | 2015-11-01 12:00:00 | -0.9690860215053764 |
        | midnight inside a gap         | America/Toronto     | 1919-03-31 12:00:00 | 1919-02-27 12:00:00 | 1.1283602150537635  |

    Scenario: timestamp columns retain month fractions across a repeated midnight
      Given config spark.sql.session.timeZone = America/Havana
      When query
        """
        SELECT months_between(left_ts, right_ts, false) AS result
        FROM VALUES
          (1, TIMESTAMP '2015-11-01 12:00:00', TIMESTAMP '2015-10-02 12:00:00'),
          (2, TIMESTAMP '2015-10-02 12:00:00', TIMESTAMP '2015-11-01 12:00:00'),
          (3, CAST(NULL AS TIMESTAMP), TIMESTAMP '2015-10-02 12:00:00') AS t(id, left_ts, right_ts)
        ORDER BY id
        """
      Then query result ordered
        | result              |
        | 0.9690860215053764  |
        | -0.9690860215053764 |
        | NULL                |

    Scenario: nullable rounding flags propagate NULL without changing valid flags
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT months_between(TIMESTAMP '2024-03-10 12:00:00', TIMESTAMP '2024-02-09 12:00:00', flag) AS result
        FROM VALUES (1, false), (2, true), (3, CAST(NULL AS BOOLEAN)) AS t(id, flag)
        ORDER BY id
        """
      Then query result ordered
        | result            |
        | 1.032258064516129 |
        | 1.03225806        |
        | NULL              |
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: non-null timestamp operands produce a non-null month fraction
      When query
        """
        SELECT months_between(TIMESTAMP '2024-03-10 12:00:00', TIMESTAMP '2024-02-09 12:00:00', false) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = false)
        """
