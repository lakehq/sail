Feature: approx_count_distinct

  # Spark's `HyperLogLogPlusPlus` accepts any input type. A NULL struct is skipped, while a
  # struct whose fields are NULL is a value. All expected values were captured on Spark JVM 4.1.1.
  Rule: Struct input

    Scenario: approx_count_distinct of a single-field struct per group
      When query
        """
        SELECT grp, approx_count_distinct(struct(pid)) AS count
        FROM VALUES ('a', 1), ('a', 2), ('a', 1), ('b', 3) AS t(grp, pid)
        GROUP BY grp ORDER BY grp
        """
      Then query result ordered
        | grp | count |
        | a   | 2     |
        | b   | 1     |

    Scenario: approx_count_distinct of a multi-field struct per group
      When query
        """
        SELECT grp, approx_count_distinct(struct(grp, pid)) AS count
        FROM VALUES ('a', 1), ('a', 2), ('a', 1), ('b', 3) AS t(grp, pid)
        GROUP BY grp ORDER BY grp
        """
      Then query result ordered
        | grp | count |
        | a   | 2     |
        | b   | 1     |

    Scenario Outline: approx_count_distinct of a struct: <case>
      When query
        """
        SELECT approx_count_distinct(<arg>) AS c FROM VALUES <values> AS t(<columns>)
        """
      Then query result
        | c        |
        | <result> |

      Examples:
        | case                                           | arg                                             | columns  | values                                                                                                                                                                                                                                                                                | result |
        | distinct pairs without grouping                | struct(grp, pid)                                | grp, pid | ('a', 1), ('b', 1), ('a', 2), ('a', 1)                                                                                                                                                                                                                                                | 3      |
        | NULL fields count but a NULL struct is skipped | s                                               | s        | (named_struct('a', 1, 'b', 'x')), (named_struct('a', CAST(NULL AS INT), 'b', 'x')), (named_struct('a', 1, 'b', CAST(NULL AS STRING))), (CAST(NULL AS STRUCT<a: INT, b: STRING>)), (named_struct('a', 1, 'b', 'x')), (named_struct('a', CAST(NULL AS INT), 'b', CAST(NULL AS STRING))) | 4      |
        | nested struct                                  | named_struct('o', named_struct('i', i), 'k', k) | i, k     | (1, 'a'), (1, 'a'), (2, 'a'), (1, 'b'), (CAST(NULL AS INT), 'a')                                                                                                                                                                                                                      | 4      |
        | only NULL structs                              | s                                               | s        | (CAST(NULL AS STRUCT<a: INT>))                                                                                                                                                                                                                                                        | 0      |
        | negative zero equals zero in a struct field    | named_struct('d', d)                            | d        | (CAST(0.0 AS DOUBLE)), (CAST(-0.0 AS DOUBLE))                                                                                                                                                                                                                                         | 1      |

    Scenario: approx_count_distinct of a struct over no rows
      When query
        """
        SELECT approx_count_distinct(named_struct('a', id)) AS c FROM range(0)
        """
      Then query result
        | c |
        | 0 |

    Scenario: approx_count_distinct of a struct is a non-nullable bigint
      When query
        """
        SELECT approx_count_distinct(named_struct('a', id)) AS c FROM range(3)
        """
      Then query schema
        """
        root
         |-- c: long (nullable = false)
        """

    Scenario: approx_count_distinct of a struct as a window function
      When query
        """
        SELECT grp, pid, approx_count_distinct(struct(pid)) OVER (PARTITION BY grp) AS c
        FROM VALUES ('a', 1), ('a', 2), ('a', 1), ('b', 3) AS t(grp, pid)
        ORDER BY grp, pid
        """
      Then query result ordered
        | grp | pid | c |
        | a   | 1   | 2 |
        | a   | 1   | 2 |
        | a   | 2   | 2 |
        | b   | 3   | 1 |
