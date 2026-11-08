Feature: typeof output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to typeof yields the schema Spark declares
      When query
        """
        SELECT typeof(1) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a non-null column input to typeof yields the schema Spark declares
      When query
        """
        SELECT typeof(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to typeof stays nullable
      When query
        """
        SELECT typeof(c) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: typeof of an aggregate function yields a non-nullable string
      When query
        """
        SELECT typeof(count(*)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

  Rule: Aggregate functions in the argument make the query an aggregation

    Scenario: typeof of listagg returns one row
      When query
        """
        SELECT typeof(listagg(b)) FROM VALUES ('a'), ('b'), ('c'), ('d') AS t(b)
        """
      Then query result
        | typeof(listagg(b, NULL)) |
        | string                   |

    Scenario: typeof of count over empty input returns one row
      When query
        """
        SELECT typeof(count(*)) FROM range(0)
        """
      Then query result
        | typeof(count(1)) |
        | bigint           |

    Scenario: typeof of several aggregate functions keeps the column names
      When query
        """
        SELECT typeof(count(*)), typeof(sum(id)), typeof(max(id)) FROM range(3)
        """
      Then query result
        | typeof(count(1)) | typeof(sum(id)) | typeof(max(id)) |
        | bigint           | bigint          | bigint          |

    Scenario Outline: typeof of an aggregate expression returns one row: <expr>
      When query
        """
        SELECT typeof(<expr>) AS t FROM range(4)
        """
      Then query result
        | t      |
        | <type> |

      Examples:
        | expr                                                 | type                      |
        | sum(id)                                              | bigint                    |
        | avg(id)                                              | double                    |
        | count(DISTINCT id)                                   | bigint                    |
        | collect_set(id)                                      | array<bigint>             |
        | array_agg(id)                                        | array<bigint>             |
        | first(id)                                            | bigint                    |
        | percentile_approx(id, 0.5)                           | bigint                    |
        | sum(CAST(id AS DECIMAL(10,2)))                       | decimal(20,2)             |
        | avg(CAST(id AS DECIMAL(10,2)))                       | decimal(14,6)             |
        | max(named_struct('a', id, 'b', CAST(id AS STRING)))  | struct<a:bigint,b:string> |
        | max(NULL)                                            | void                      |
        | count(*) FILTER (WHERE id > 1)                       | bigint                    |
        | percentile_cont(0.5) WITHIN GROUP (ORDER BY id)      | double                    |
        | count(*) + 1                                         | bigint                    |
        | sum(id) / 2                                          | double                    |
        | typeof(count(*))                                     | string                    |

    Scenario: typeof of an aggregate function nested in another function returns one row
      When query
        """
        SELECT concat(typeof(count(*)), 'x') FROM range(4)
        """
      Then query result
        | concat(typeof(count(1)), x) |
        | bigintx                     |

    Scenario: typeof of an aggregate function in a CASE condition returns one row
      When query
        """
        SELECT CASE WHEN typeof(max(id)) = 'bigint' THEN 'y' ELSE 'n' END AS r FROM range(3)
        """
      Then query result
        | r |
        | y |

    Scenario: typeof of an aggregate function next to the aggregate function
      When query
        """
        SELECT typeof(count(*)) AS t, count(*) AS c FROM range(4)
        """
      Then query result
        | t      | c |
        | bigint | 4 |

    Scenario: typeof of an aggregate function returns one row per group
      When query
        """
        SELECT k, typeof(sum(v)) AS t FROM VALUES (1, 10), (1, 20), (2, 30) AS t(k, v) GROUP BY k
        """
      Then query result
        | k | t      |
        | 1 | bigint |
        | 2 | bigint |

    Scenario: typeof of an aggregate function with grouping sets
      When query
        """
        SELECT id % 2 AS k, typeof(count(*)) AS t FROM range(4) GROUP BY GROUPING SETS ((id % 2), ())
        """
      Then query result
        | k    | t      |
        | 0    | bigint |
        | 1    | bigint |
        | NULL | bigint |

    Scenario: typeof of a window function over an aggregate function returns one row per group
      When query
        """
        SELECT k, typeof(sum(sum(v)) OVER ()) AS t FROM VALUES (1, 10), (1, 20), (2, 30) AS t(k, v) GROUP BY k
        """
      Then query result
        | k | t      |
        | 1 | bigint |
        | 2 | bigint |

    # TODO: A window function over a global aggregate (no GROUP BY) is not turned into an
    #   aggregation, because the window is extracted (`rewrite_window_projection` in
    #   `resolver/query/project.rs`) before the projection is checked for aggregate functions.
    #   This also makes `SELECT sum(sum(id)) OVER () FROM range(4)` fail.
    @sail-bug
    Scenario: typeof of a window function over a global aggregate function returns one row
      When query
        """
        SELECT typeof(sum(sum(id)) OVER ()) AS t FROM range(4)
        """
      Then query result
        | t      |
        | bigint |

    Scenario: typeof of an aggregate function in HAVING
      When query
        """
        SELECT count(*) AS c FROM range(4) HAVING typeof(sum(id)) = 'bigint'
        """
      Then query result
        | c |
        | 4 |

    Scenario: typeof of an aggregate function in ORDER BY
      When query
        """
        SELECT k FROM VALUES (2, 30), (1, 10), (1, 20) AS t(k, v) GROUP BY k ORDER BY typeof(sum(v)), k
        """
      Then query result collected ordered
        | k |
        | 1 |
        | 2 |

    Scenario: typeof of an aggregate function in a scalar subquery
      When query
        """
        SELECT (SELECT typeof(count(*)) FROM range(5)) AS r
        """
      Then query result
        | r      |
        | bigint |

    Scenario: typeof of an aggregate function in a correlated scalar subquery
      When query
        """
        SELECT id, (SELECT typeof(count(*)) FROM range(5) r WHERE r.id < t.id) AS r FROM range(3) t
        """
      Then query result
        | id | r      |
        | 0  | bigint |
        | 1  | bigint |
        | 2  | bigint |

    Scenario: typeof of an aggregate function in each branch of a union
      When query
        """
        SELECT typeof(count(*)) AS t FROM range(2) UNION ALL SELECT typeof(sum(id)) AS t FROM range(3)
        """
      Then query result
        | t      |
        | bigint |
        | bigint |

  Rule: The argument is not evaluated

    Scenario: typeof of an aggregate function that would raise an error
      When query
        """
        SELECT typeof(max(raise_error('boom'))) AS t FROM range(3)
        """
      Then query result
        | t    |
        | void |

    Scenario: typeof of an aggregate function that would raise an error over empty input
      When query
        """
        SELECT typeof(max(raise_error('boom'))) AS t FROM range(0)
        """
      Then query result
        | t    |
        | void |

    Scenario: typeof of an aggregate function that would raise an error in each group
      When query
        """
        SELECT id % 2 AS k, typeof(max(raise_error('boom'))) AS t FROM range(4) GROUP BY id % 2
        """
      Then query result
        | k | t    |
        | 0 | void |
        | 1 | void |

    Scenario: typeof of an aggregate function that would divide by zero in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof(sum(id / (id - id))) AS t FROM range(3)
        """
      Then query result
        | t      |
        | double |

    Scenario: typeof of an aggregate function that would divide by zero in ANSI mode in each group
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id % 2 AS k, typeof(sum(id / (id - id))) AS t FROM range(4) GROUP BY id % 2
        """
      Then query result
        | k | t      |
        | 0 | double |
        | 1 | double |

    Scenario: typeof of a window function that would divide by zero in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof(sum(id / (id - id)) OVER ()) AS t FROM range(3)
        """
      Then query result
        | t      |
        | double |
        | double |
        | double |

    Scenario: typeof of an expression that would raise an error
      When query
        """
        SELECT typeof(raise_error('boom')) AS t FROM range(2)
        """
      Then query result
        | t    |
        | void |
        | void |

  Rule: Window functions in the argument keep one row per input row

    Scenario Outline: typeof of a window function: <expr>
      When query
        """
        SELECT typeof(<expr>) AS t FROM range(3)
        """
      Then query result
        | t      |
        | <type> |
        | <type> |
        | <type> |

      Examples:
        | expr                                | type   |
        | row_number() OVER (ORDER BY id)     | int    |
        | sum(id) OVER ()                     | bigint |
        | lag(id) OVER (ORDER BY id)          | bigint |
        | rank() OVER (ORDER BY id)           | int    |
