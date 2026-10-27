Feature: make_dt_interval output schema

  @function(nullability) @spark-4
  Rule: Output schema

    Scenario: a non-null literal input to make_dt_interval yields the schema Spark declares
      When query
        """
        SELECT make_dt_interval(1, 12, 30, 01.001001) AS result
        """
      Then query schema
        """
        root
         |-- result: interval day to second (nullable = false)
        """

    Scenario: a non-null column input to make_dt_interval yields the schema Spark declares
      When query
        """
        SELECT make_dt_interval(CAST(id AS INT), 12, 30, 01.001001) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: interval day to second (nullable = false)
        """

    Scenario: a nullable column input to make_dt_interval stays nullable
      When query
        """
        SELECT make_dt_interval(c, 12, 30, 01.001001) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: interval day to second (nullable = true)
        """

  Rule: Result values (migrated from test_make_dt_interval.txt doctests)

    Scenario Outline: Doctest (derived column name): <case>
      When query
        """
        SELECT (make_dt_interval(<args>))
        """
      Then query result
        | <name>   |
        | <result> |

      Examples:
        | case                                  | args          | name                            | result                                |
        | make_dt_interval doctest #1 (result)  | null, 0, 0, 0 | make_dt_interval(NULL, 0, 0, 0) | NULL                                  |
        | make_dt_interval doctest #2 (result)  | 0, null, 0, 0 | make_dt_interval(0, NULL, 0, 0) | NULL                                  |
        | make_dt_interval doctest #3 (result)  | 0, 0, null, 0 | make_dt_interval(0, 0, NULL, 0) | NULL                                  |
        | make_dt_interval doctest #4 (result)  | 0, 0, 0, null | make_dt_interval(0, 0, 0, NULL) | NULL                                  |
        | make_dt_interval doctest #10 (result) | 0, 0, 0, 0    | make_dt_interval(0, 0, 0, 0)    | INTERVAL '0 00:00:00' DAY TO SECOND   |
        | make_dt_interval doctest #13 (result) | 0, 0, 0, 0.1  | make_dt_interval(0, 0, 0, 0.1)  | INTERVAL '0 00:00:00.1' DAY TO SECOND |

    Scenario Outline: Doctest (aliased): <case>
      When query
        """
        SELECT (make_dt_interval(<args>)) AS make_dt_interval
        """
      Then query result
        | make_dt_interval |
        | <result>         |

      Examples:
        | case                                 | args       | result                              |
        | make_dt_interval doctest #5 (result) |            | INTERVAL '0 00:00:00' DAY TO SECOND |
        | make_dt_interval doctest #6 (result) | 1          | INTERVAL '1 00:00:00' DAY TO SECOND |
        | make_dt_interval doctest #7 (result) | 1, 1       | INTERVAL '1 01:00:00' DAY TO SECOND |
        | make_dt_interval doctest #8 (result) | 1, 1, 1    | INTERVAL '1 01:01:00' DAY TO SECOND |
        | make_dt_interval doctest #9 (result) | 1, 1, 1, 1 | INTERVAL '1 01:01:01' DAY TO SECOND |

    Scenario Outline: Doctest (bare alias): <case>
      When query
        """
        SELECT (make_dt_interval(<args>)) <alias>
        """
      Then query result
        | <alias>  |
        | <result> |

      Examples:
        | case                                  | args         | alias | result                              |
        | make_dt_interval doctest #11 (result) | -1, 24, 0, 0 | df    | INTERVAL '0 00:00:00' DAY TO SECOND |
        | make_dt_interval doctest #12 (result) | 1, -24, 0, 0 | dt    | INTERVAL '0 00:00:00' DAY TO SECOND |

    Scenario: make_dt_interval doctest #14 (result)
      When query
        """
        SELECT day, hour, `min`, sec, make_dt_interval(day) AS r FROM VALUES (CAST(1 AS BIGINT), CAST(12 AS BIGINT), CAST(30 AS BIGINT), CAST(1.001001 AS DOUBLE)) AS t(day, hour, `min`, sec)
        """
      Then query result
        | day | hour | min | sec      | r                                   |
        | 1   | 12   | 30  | 1.001001 | INTERVAL '1 00:00:00' DAY TO SECOND |

    Scenario: make_dt_interval doctest #15 (result)
      When query
        """
        SELECT day, hour, `min`, sec, make_dt_interval(day, hour) AS r FROM VALUES (CAST(1 AS BIGINT), CAST(12 AS BIGINT), CAST(30 AS BIGINT), CAST(1.001001 AS DOUBLE)) AS t(day, hour, `min`, sec)
        """
      Then query result
        | day | hour | min | sec      | r                                   |
        | 1   | 12   | 30  | 1.001001 | INTERVAL '1 12:00:00' DAY TO SECOND |

    Scenario: make_dt_interval doctest #16 (result)
      When query
        """
        SELECT day, hour, `min`, sec, make_dt_interval(day, hour, `min`) AS r FROM VALUES (CAST(1 AS BIGINT), CAST(12 AS BIGINT), CAST(30 AS BIGINT), CAST(1.001001 AS DOUBLE)) AS t(day, hour, `min`, sec)
        """
      Then query result
        | day | hour | min | sec      | r                                   |
        | 1   | 12   | 30  | 1.001001 | INTERVAL '1 12:30:00' DAY TO SECOND |

    Scenario: make_dt_interval doctest #17 (result)
      When query
        """
        SELECT day, hour, `min`, sec, make_dt_interval(day, hour, `min`, sec) AS r FROM VALUES (CAST(1 AS BIGINT), CAST(12 AS BIGINT), CAST(30 AS BIGINT), CAST(1.001001 AS DOUBLE)) AS t(day, hour, `min`, sec)
        """
      Then query result
        | day | hour | min | sec      | r                                          |
        | 1   | 12   | 30  | 1.001001 | INTERVAL '1 12:30:01.001001' DAY TO SECOND |
