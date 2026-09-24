Feature: Set operations (INTERSECT, EXCEPT)

  Rule: INTERSECT DISTINCT

    Scenario: intersect distinct two tables
      When query
        """
        SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS a(id)
        INTERSECT
        SELECT * FROM (VALUES (3), (4), (5), (6), (7)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 3  |
        | 4  |
        | 5  |

    Scenario: intersect distinct removes duplicates
      When query
        """
        SELECT * FROM (VALUES (1), (1), (2), (2), (3)) AS a(id)
        INTERSECT DISTINCT
        SELECT * FROM (VALUES (1), (2), (2), (4)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |

    Scenario: intersect distinct three tables
      When query
        """
        SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS a(id)
        INTERSECT
        SELECT * FROM (VALUES (2), (3), (4), (5), (6)) AS b(id)
        INTERSECT
        SELECT * FROM (VALUES (3), (4), (5), (6), (7)) AS c(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 3  |
        | 4  |
        | 5  |

  # Note: INTERSECT ALL tests excluded — DataFusion's LogicalPlanBuilder::intersect(is_all=true)
  # produces extra duplicates compared to Spark. Pre-existing upstream bug.
  # https://github.com/apache/datafusion/issues/12955

  Rule: EXCEPT DISTINCT

    Scenario: except distinct two tables
      When query
        """
        SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS a(id)
        EXCEPT
        SELECT * FROM (VALUES (3), (4), (5), (6), (7)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |

    Scenario: except distinct removes duplicates
      When query
        """
        SELECT * FROM (VALUES (1), (1), (2), (2), (3)) AS a(id)
        EXCEPT DISTINCT
        SELECT * FROM (VALUES (2), (4)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 3  |

    Scenario: except distinct three tables
      When query
        """
        SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS a(id)
        EXCEPT
        SELECT * FROM (VALUES (4), (5), (6)) AS b(id)
        EXCEPT
        SELECT * FROM (VALUES (1), (7)) AS c(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 2  |
        | 3  |

  Rule: EXCEPT ALL

    Scenario: except all preserves duplicates
      When query
        """
        SELECT * FROM (VALUES (1), (1), (2), (2), (3)) AS a(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (1), (2)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
        | 3  |

    Scenario: except all three tables
      When query
        """
        SELECT * FROM (VALUES (1), (1), (1), (2), (2), (3), (3)) AS a(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (1), (2), (3)) AS b(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (1), (3)) AS c(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |

    Scenario: except all subtracts matching count
      When query
        """
        SELECT * FROM (VALUES (1), (1), (1)) AS a(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (1), (1)) AS b(id)
        """
      Then query result
        | id |
        | 1  |

  Rule: Wide table set operations

    Scenario: intersect distinct with multiple columns
      When query
        """
        SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS a(id, name)
        INTERSECT
        SELECT * FROM (VALUES (2, 'b'), (3, 'c'), (4, 'd')) AS b(id, name)
        ORDER BY id
        """
      Then query result ordered
        | id | name |
        | 2  | b    |
        | 3  | c    |

    Scenario: except all with multiple columns
      When query
        """
        SELECT * FROM (VALUES (1, 'a'), (1, 'a'), (2, 'b'), (3, 'c')) AS a(id, name)
        EXCEPT ALL
        SELECT * FROM (VALUES (1, 'a'), (3, 'c')) AS b(id, name)
        ORDER BY id
        """
      Then query result ordered
        | id | name |
        | 1  | a    |
        | 2  | b    |

  Rule: Null handling

    Scenario: intersect with nulls
      When query
        """
        SELECT * FROM (VALUES (1), (NULL), (3)) AS a(id)
        INTERSECT
        SELECT * FROM (VALUES (NULL), (3), (4)) AS b(id)
        ORDER BY id ASC NULLS LAST
        """
      Then query result ordered
        | id   |
        | 3    |
        | NULL |

    Scenario: except all with nulls
      When query
        """
        SELECT * FROM (VALUES (1), (NULL), (NULL), (3)) AS a(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (NULL), (3)) AS b(id)
        ORDER BY id ASC NULLS LAST
        """
      Then query result ordered
        | id   |
        | 1    |
        | NULL |

  Rule: Empty results

    Scenario: intersect with no common rows
      When query
        """
        SELECT * FROM (VALUES (1), (2)) AS a(id)
        INTERSECT
        SELECT * FROM (VALUES (3), (4)) AS b(id)
        """
      Then query result
        | id |

    Scenario: except all removing everything
      When query
        """
        SELECT * FROM (VALUES (1), (2)) AS a(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (1), (2), (3)) AS b(id)
        """
      Then query result
        | id |

  Rule: a set operation widens its numeric columns to the common type

    # `WidenSetOperationTypes` widens each positional pair to their common type
    # (`TypeCoercion.scala`), so an INT branch beside a BIGINT one is a BIGINT. Sail built the plan
    # from the LEFT input's schema, so the column declared INT while carrying a BIGINT value: the
    # rows came back right and the schema lied, which broke `toArrow` and `CREATE TABLE AS SELECT`.
    Scenario Outline: <case> is <type>
      When query
        """
        SELECT typeof(v) AS t FROM (<query>) LIMIT 1
        """
      Then query result
        | t      |
        | <type> |

      Examples:
        | case                     | query                                                              | type   |
        | an int beside a bigint   | SELECT -2147483648 AS v UNION ALL SELECT 3000000000L AS v          | bigint |
        | a bigint beside an int   | SELECT 3000000000L AS v UNION ALL SELECT -2147483648 AS v          | bigint |
        | an int beside a double   | SELECT 1 AS v UNION ALL SELECT CAST(1.5 AS DOUBLE) AS v            | double |
        | an int beside a decimal  | SELECT 1 AS v UNION ALL SELECT CAST(1.5 AS DECIMAL(10,2)) AS v     | decimal(12,2) |
        | capped decimals preserve integral digits | SELECT CAST(1 AS DECIMAL(38,0)) AS v UNION ALL SELECT CAST(1.5 AS DECIMAL(38,10)) AS v | decimal(38,0) |
        | a distinct union         | SELECT -2147483648 AS v UNION SELECT 3000000000L AS v              | bigint |

    Scenario: every row of a widened union survives
      When query
        """
        SELECT v FROM (SELECT -2147483648 AS v UNION ALL SELECT 3000000000L AS v) ORDER BY v
        """
      Then query result ordered
        | v           |
        | -2147483648 |
        | 3000000000  |

    # `WidenSetOperationTypes` covers `Except` (`TypeCoercionBase.scala:194`) and `Intersect`
    # (`:208`), not only `Union` (`:222`).
    Scenario Outline: <case> widens too
      When query
        """
        SELECT typeof(v) AS t FROM (<query>) LIMIT 1
        """
      Then query result
        | t      |
        | <type> |

      Examples:
        | case                    | query                                                                  | type          |
        | an except with a decimal | SELECT 1 AS v EXCEPT SELECT CAST(0.5 AS DECIMAL(10,1)) AS v            | decimal(11,1) |
        | an intersect with a bigint | SELECT 1 AS v INTERSECT SELECT 1L AS v                                | bigint        |
        | an intersect with a decimal | SELECT CAST(1.0 AS DECIMAL(10,1)) AS v INTERSECT SELECT 1 AS v      | decimal(11,1) |

    Scenario: an except keeps the widened value
      When query
        """
        SELECT v FROM (SELECT 1 AS v EXCEPT SELECT CAST(0.5 AS DECIMAL(10,1)) AS v)
        """
      Then query result
        | v   |
        | 1.0 |

    Scenario: a union of the same type is not rewritten
      When query
        """
        SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2 AS v) ORDER BY v
        """
      Then query result ordered
        | v |
        | 1 |
        | 2 |

  Rule: set operations whose columns have no common type are refused

    # TODO: `WidenSetOperationTypes` finds no wider type for an INT beside a DATE or an ARRAY, so
    #  Spark refuses with `INCOMPATIBLE_COLUMN_TYPE` (`TypeCoercionBase.scala:190-222`). Sail keeps
    #  the left input's type; the numeric widening this PR added does not reach these pairs.
    Scenario Outline: a UNION of <case> is refused
      When query
        """
        SELECT <query>
        """
      Then query error (?i)INCOMPATIBLE_COLUMN_TYPE|can only be performed

      Examples:
        | case              | query                                            |
        | an INT and a DATE  | 1 AS v UNION ALL SELECT DATE'2024-01-01' AS v    |
        | an INT and an ARRAY | 1 AS v UNION ALL SELECT array(1) AS v          |

    Scenario: a fourth incompatible column identifies its ordinal
      When query
        """
        SELECT 1, 2, 3, DATE'2020-01-01'
        UNION ALL
        SELECT 1, 2, 3, 4
        """
      Then query error (?i)4th column

  Rule: only ANSI widens an integral beside a FLOAT in a set operation

    # `WidenSetOperationTypes` uses the same `findWiderTypeForTwo` as the branches of a CASE, so the
    # FLOAT survives with ANSI off (`TypeCoercion.scala:89-92`) and becomes a DOUBLE with it on
    # (`AnsiTypeCoercion.scala:117-121`).
    Scenario Outline: <case> is <type> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT DISTINCT typeof(c) AS t FROM (<query>)
        """
      Then query result
        | t      |
        | <type> |

      Examples:
        | case                      | ansi  | query                                                        | type   |
        | a union of int and float  | false | SELECT CAST(1 AS INT) AS c UNION ALL SELECT CAST(0.1 AS FLOAT) | float  |
        | a union of int and float  | true  | SELECT CAST(1 AS INT) AS c UNION ALL SELECT CAST(0.1 AS FLOAT) | double |
        | an except of int and float | false | SELECT CAST(1 AS INT) AS c EXCEPT SELECT CAST(0.1 AS FLOAT)   | float  |
        | a union of int and bigint | false | SELECT CAST(1 AS INT) AS c UNION ALL SELECT 3000000000L       | bigint |
