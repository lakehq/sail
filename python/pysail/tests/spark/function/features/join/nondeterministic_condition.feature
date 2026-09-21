Feature: Non-deterministic expressions in join conditions

  Rule: A join condition with a non-deterministic function is rejected

    @sail-bug
    Scenario Outline: join condition rejects <function>
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW nd_x AS SELECT id, array(1, 2) AS arr FROM range(6)
        """
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW nd_y AS SELECT id, array(1, 2) AS arr FROM range(6)
        """
      When query
        """
        SELECT nd_x.id FROM nd_x JOIN nd_y ON nd_x.id = nd_y.id AND <predicate>
        """
      Then query error INVALID_NON_DETERMINISTIC_EXPRESSIONS

      Examples:
        | function                          | predicate                                  |
        | rand                              | rand() < 2                                 |
        | rand with seed                    | rand(42) < 2                               |
        | random                            | random() < 2                               |
        | random with seed                  | random(7) < 2                              |
        | randn                             | randn() < 100                              |
        | randn with seed                   | randn(7) < 100                             |
        | uniform                           | uniform(0, 10) < 20                        |
        | uniform with seed                 | uniform(0, 10, 3) < 20                     |
        | randstr                           | length(randstr(5)) = 5                     |
        | randstr with seed                 | length(randstr(5, 1)) = 5                  |
        | uuid                              | length(uuid()) = 36                        |
        | shuffle of a literal array        | size(shuffle(array(1, 2, 3))) = 3          |
        | shuffle of a column               | size(shuffle(nd_x.arr)) = 2                |
        | monotonically_increasing_id       | monotonically_increasing_id() >= 0         |
        | spark_partition_id                | spark_partition_id() >= 0                  |

    @sail-bug
    Scenario Outline: join condition rejects unimplemented non-deterministic <function>
      When query
        """
        SELECT x.id FROM range(6) x JOIN range(6) y ON x.id = y.id AND <predicate>
        """
      Then query error INVALID_NON_DETERMINISTIC_EXPRESSIONS

      Examples:
        | function                | predicate                                                |
        | input_file_name         | input_file_name() IS NOT NULL                            |
        | input_file_block_start  | input_file_block_start() IS NOT NULL                     |
        | input_file_block_length | input_file_block_length() IS NOT NULL                    |
        | reflect                 | reflect('java.util.UUID', 'randomUUID') IS NOT NULL      |
        | java_method             | java_method('java.util.UUID', 'randomUUID') IS NOT NULL  |
        | try_reflect             | try_reflect('java.util.UUID', 'randomUUID') IS NOT NULL  |

  Rule: Every join type rejects a non-deterministic condition

    @sail-bug
    Scenario Outline: <join> rejects rand in the condition
      When query
        """
        SELECT x.id FROM range(6) x <join> range(6) y ON x.id = y.id AND rand() < 2
        """
      Then query error INVALID_NON_DETERMINISTIC_EXPRESSIONS

      Examples:
        | join            |
        | JOIN            |
        | INNER JOIN      |
        | LEFT JOIN       |
        | LEFT OUTER JOIN |
        | RIGHT JOIN      |
        | FULL JOIN       |
        | FULL OUTER JOIN |
        | LEFT SEMI JOIN  |
        | SEMI JOIN       |
        | LEFT ANTI JOIN  |
        | ANTI JOIN       |

    Scenario Outline: <join> with a deterministic condition is accepted
      When query
        """
        SELECT count(*) AS c FROM range(6) x <join> range(6) y ON x.id = y.id
        """
      Then query result
        | c   |
        | <c> |

      Examples:
        | join            | c |
        | JOIN            | 6 |
        | LEFT JOIN       | 6 |
        | RIGHT JOIN      | 6 |
        | FULL OUTER JOIN | 6 |
        | LEFT SEMI JOIN  | 6 |
        | LEFT ANTI JOIN  | 0 |

    @sail-bug
    Scenario: CROSS JOIN with a non-deterministic ON condition is rejected
      When query
        """
        SELECT x.id FROM range(6) x CROSS JOIN range(6) y ON x.id = y.id AND rand() < 2
        """
      Then query error INVALID_NON_DETERMINISTIC_EXPRESSIONS

    @sail-bug
    Scenario: CROSS JOIN with a deterministic ON condition behaves as an inner join
      When query
        """
        SELECT count(*) AS c FROM range(6) x CROSS JOIN range(6) y ON x.id = y.id
        """
      Then query result
        | c |
        | 6 |

    @sail-bug
    Scenario: CROSS JOIN with USING behaves as an inner join
      When query
        """
        SELECT * FROM range(3) x CROSS JOIN range(3) y USING (id)
        """
      Then query result
        | id |
        | 0  |
        | 1  |
        | 2  |

    Scenario: CROSS JOIN without a condition is a cartesian product
      When query
        """
        SELECT count(*) AS c FROM range(6) x CROSS JOIN range(6) y
        """
      Then query result
        | c  |
        | 36 |

    @sail-bug
    Scenario: NATURAL CROSS JOIN is rejected
      When query
        """
        SELECT count(*) AS c FROM range(6) x NATURAL CROSS JOIN range(6) y
        """
      Then query error INCOMPATIBLE_JOIN_TYPES

  Rule: A non-deterministic call hidden inside the condition is still rejected

    @sail-bug
    Scenario Outline: join condition rejects rand nested in <shape>
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW nd_x AS SELECT id, array(1, 2) AS arr FROM range(6)
        """
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW nd_y AS SELECT id, array(1, 2) AS arr FROM range(6)
        """
      When query
        """
        SELECT nd_x.id FROM nd_x JOIN nd_y ON <condition>
        """
      Then query error INVALID_NON_DETERMINISTIC_EXPRESSIONS

      Examples:
        | shape                         | condition                                                                        |
        | the only predicate            | rand() < 2                                                                       |
        | the equi-join key             | nd_x.id = nd_y.id + rand() * 0                                                   |
        | a cast in the equi-join key   | nd_x.id = nd_y.id + CAST(rand() AS BIGINT)                                       |
        | a null-safe equality          | nd_x.id <=> nd_y.id + CAST(rand() * 0 AS BIGINT)                                 |
        | an OR branch                  | nd_x.id = nd_y.id OR rand() < -1                                                 |
        | a NOT                         | nd_x.id = nd_y.id AND NOT (rand() > 2)                                           |
        | a CASE condition              | nd_x.id = nd_y.id AND CASE WHEN rand() < 2 THEN true ELSE false END              |
        | a CASE branch never taken     | nd_x.id = CASE WHEN nd_x.id >= 0 THEN nd_y.id ELSE CAST(rand() AS BIGINT) END    |
        | an IF branch never taken      | nd_x.id = nd_y.id AND if(nd_x.id >= 0, true, rand() < 2)                         |
        | coalesce                      | nd_x.id = nd_y.id AND coalesce(rand(), 0) < 2                                    |
        | nvl                           | nd_x.id = nd_y.id AND nvl(NULL, rand()) < 2                                      |
        | a multiplication by zero      | nd_x.id = nd_y.id AND rand() * 0 = 0                                             |
        | IS NOT NULL                   | nd_x.id = nd_y.id AND rand() IS NOT NULL                                         |
        | BETWEEN                       | nd_x.id = nd_y.id AND rand() BETWEEN -1 AND 2                                    |
        | an IN list                    | nd_x.id = nd_y.id AND floor(rand()) IN (0, 1)                                    |
        | a struct field                | nd_x.id = nd_y.id AND named_struct('r', rand()).r < 2                            |
        | an array element              | nd_x.id = nd_y.id AND array(rand())[0] < 2                                       |
        | a string concatenation        | nd_x.id = nd_y.id AND concat(CAST(nd_x.id AS STRING), uuid()) IS NOT NULL        |
        | an exists lambda              | nd_x.id = nd_y.id AND exists(nd_x.arr, e -> e < rand() + 5)                      |
        | a transform lambda            | nd_x.id = nd_y.id AND size(transform(nd_x.arr, e -> e + rand())) = 2             |
        | a filter lambda               | nd_x.id = nd_y.id AND size(filter(nd_x.arr, e -> rand() < 2)) = 2                |

    # TODO: Sail does not look into subquery plans of a join condition yet. Spark treats a
    # subquery as non-deterministic when its plan is (`SubqueryExpression.deterministic`).
    @sail-bug
    Scenario Outline: join condition rejects a non-deterministic <shape>
      When query
        """
        SELECT x.id FROM range(6) x JOIN range(6) y ON x.id = y.id AND <condition>
        """
      Then query error INVALID_NON_DETERMINISTIC_EXPRESSIONS

      Examples:
        | shape                                   | condition                                                         |
        | scalar subquery                         | (SELECT rand()) < 2                                               |
        | EXISTS subquery                         | EXISTS (SELECT 1 FROM range(1) WHERE rand() < 2)                  |
        | IN subquery                             | x.id IN (SELECT id FROM range(10) WHERE rand() < 2)               |
        | IN subquery of spark_partition_id       | x.id IN (SELECT spark_partition_id() FROM range(3))               |
        | IN subquery of monotonically_increasing_id | x.id IN (SELECT monotonically_increasing_id() FROM range(3))   |

    Scenario Outline: join condition with a deterministic <shape> is accepted
      When query
        """
        SELECT count(*) AS c FROM range(6) x JOIN range(6) y ON <condition>
        """
      Then query result
        | c |
        | 6 |

      Examples:
        | shape            | condition                                                  |
        | scalar subquery  | x.id = y.id AND (SELECT 1) = 1                             |
        | IN subquery      | x.id = y.id AND x.id IN (SELECT id FROM range(10))         |
        | EXISTS subquery  | x.id = y.id AND EXISTS (SELECT 1 FROM range(1))            |

  Rule: A deterministic function in a join condition is accepted

    Scenario Outline: join condition accepts <function>
      When query
        """
        SELECT count(*) AS c FROM range(6) x JOIN range(6) y ON x.id = y.id AND <predicate>
        """
      Then query result
        | c |
        | 6 |

      Examples:
        | function                | predicate                                                   |
        | a column predicate      | x.id >= 0                                                   |
        | current_timestamp       | current_timestamp() IS NOT NULL                             |
        | now                     | now() IS NOT NULL                                           |
        | current_date            | current_date() IS NOT NULL                                  |
        | localtimestamp          | localtimestamp() IS NOT NULL                                |
        | unix_timestamp          | unix_timestamp() > 0                                        |
        | current_user            | current_user() IS NOT NULL                                  |
        | current_database        | current_database() IS NOT NULL                              |
        | version                 | version() IS NOT NULL                                       |
        | hash                    | hash(x.id) = hash(y.id)                                     |
        | aes_encrypt of literals | aes_encrypt('spark', '1234567890123456') IS NOT NULL        |

    # `try_aes_encrypt` does not exist in Spark 4.2; it is exempt like `aes_encrypt`.
  Rule: Non-determinism outside the join condition is accepted

    Scenario Outline: rand <place> is accepted
      When query
        """
        <query>
        """
      Then query result
        | c |
        | 6 |

      Examples:
        | place                                   | query                                                                                                                  |
        | in a WHERE above the join               | SELECT count(*) AS c FROM range(6) x JOIN range(6) y ON x.id = y.id WHERE rand() < 2                                   |
        | in the WHERE of a comma join            | SELECT count(*) AS c FROM range(6) x, range(6) y WHERE x.id = y.id AND rand() < 2                                      |
        | in the projection over the join         | SELECT count(r) AS c FROM (SELECT rand() AS r FROM range(6) x JOIN range(6) y ON x.id = y.id)                          |
        | in a derived table referenced by column | SELECT count(*) AS c FROM range(6) x JOIN (SELECT id, rand() AS r FROM range(6)) y ON x.id = y.id AND y.r < 2          |
        | in a CTE referenced by column           | WITH y AS (SELECT id, rand() AS r FROM range(6)) SELECT count(*) AS c FROM range(6) x JOIN y ON x.id = y.id AND y.r < 2 |
        | in ORDER BY                             | SELECT count(*) AS c FROM (SELECT x.id FROM range(6) x JOIN range(6) y ON x.id = y.id ORDER BY rand())                 |
        | in SORT BY                              | SELECT count(*) AS c FROM (SELECT id FROM range(6) SORT BY rand())                                                     |
        | in GROUP BY                             | SELECT count(*) AS c FROM range(6) GROUP BY rand() < 2                                                                 |
        | in HAVING                               | SELECT count(*) AS c FROM (SELECT id FROM range(6) GROUP BY id HAVING rand() < 2)                                      |
        | in a generator                          | SELECT least(count(*), 6) AS c FROM (SELECT explode(sequence(0, CAST(rand() * 2 AS INT))) FROM range(6))               |
        | in a USING join input                   | SELECT count(*) AS c FROM (SELECT id, rand() AS r FROM range(6)) x JOIN range(6) y USING (id)                          |
        | in a NATURAL join input                 | SELECT count(*) AS c FROM (SELECT id, rand() AS r FROM range(6)) x NATURAL JOIN range(6) y                             |

  Rule: SQL functions carry the determinism of their body

    @sail-bug
    Scenario: join condition rejects a SQL function whose body calls rand
      Given statement
        """
        CREATE OR REPLACE TEMPORARY FUNCTION nd_sql_fn() RETURNS DOUBLE RETURN rand()
        """
      When query
        """
        SELECT x.id FROM range(6) x JOIN range(6) y ON x.id = y.id AND nd_sql_fn() < 2
        """
      Then query error INVALID_NON_DETERMINISTIC_EXPRESSIONS

    @sail-bug
    Scenario: join condition accepts a deterministic SQL function
      Given statement
        """
        CREATE OR REPLACE TEMPORARY FUNCTION d_sql_fn() RETURNS DOUBLE RETURN 1.0
        """
      When query
        """
        SELECT count(*) AS c FROM range(6) x JOIN range(6) y ON x.id = y.id AND d_sql_fn() < 2
        """
      Then query result
        | c |
        | 6 |
