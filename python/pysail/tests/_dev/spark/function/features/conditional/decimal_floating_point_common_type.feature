Feature: DECIMAL with FLOAT or DOUBLE values has the DOUBLE common type

  Like Spark, the common type of DECIMAL and FLOAT or DOUBLE values is DOUBLE, since floating point
  values such as NaN, infinity, or 1e20 may not fit in a DECIMAL.

  Background:
    Given config spark.sql.ansi.enabled = true

  Rule: Conditional functions

    Scenario Outline: <function> of DECIMAL and DOUBLE values is DOUBLE
      When query
        """
        SELECT id, <expression> AS result, typeof(<expression>) AS result_type
        FROM VALUES
          (0, CAST(1.25 AS DECIMAL(10,2)), CAST('NaN' AS DOUBLE)),
          (1, CAST(NULL AS DECIMAL(10,2)), CAST('NaN' AS DOUBLE)),
          (2, CAST(NULL AS DECIMAL(10,2)), CAST('Infinity' AS DOUBLE)),
          (3, CAST(NULL AS DECIMAL(10,2)), CAST(1e20 AS DOUBLE)),
          (4, CAST(NULL AS DECIMAL(10,2)), CAST(2.5 AS DOUBLE))
        AS t(id, de, d)
        ORDER BY id
        """
      Then query result ordered
        | id | result   | result_type |
        | 0  | 1.25     | double      |
        | 1  | NaN      | double      |
        | 2  | Infinity | double      |
        | 3  | 1.0E20   | double      |
        | 4  | 2.5      | double      |

      Examples:
        | function | expression                            |
        | coalesce | coalesce(de, d)                       |
        | nvl      | nvl(de, d)                            |
        | ifnull   | ifnull(de, d)                         |
        | nvl2     | nvl2(de, de, d)                       |
        | if       | if(de IS NOT NULL, de, d)             |
        | CASE     | CASE WHEN de IS NULL THEN d ELSE de END |

    Scenario Outline: <function> of DECIMAL and FLOAT values is DOUBLE
      When query
        """
        SELECT id, <expression> AS result, typeof(<expression>) AS result_type
        FROM VALUES
          (0, CAST(1.25 AS DECIMAL(38,18)), CAST(1.5 AS FLOAT)),
          (1, CAST(NULL AS DECIMAL(38,18)), CAST('NaN' AS FLOAT)),
          (2, CAST(NULL AS DECIMAL(38,18)), CAST(1.5 AS FLOAT))
        AS t(id, de, f)
        ORDER BY id
        """
      Then query result ordered
        | id | result | result_type |
        | 0  | 1.25   | double      |
        | 1  | NaN    | double      |
        | 2  | 1.5    | double      |

      Examples:
        | function | expression                              |
        | coalesce | coalesce(de, f)                         |
        | nvl      | nvl(de, f)                              |
        | CASE     | CASE WHEN de IS NULL THEN f ELSE de END |

    Scenario: coalesce of a DECIMAL column and a DOUBLE literal is DOUBLE
      When query
        """
        SELECT id, coalesce(de, 1.5D) AS result, typeof(coalesce(de, 1.5D)) AS result_type
        FROM VALUES
          (0, CAST(1.25 AS DECIMAL(10,2))),
          (1, CAST(NULL AS DECIMAL(10,2))),
          (2, CAST(-99999999.99 AS DECIMAL(10,2)))
        AS t(id, de)
        ORDER BY id
        """
      Then query result ordered
        | id | result         | result_type |
        | 0  | 1.25           | double      |
        | 1  | 1.5            | double      |
        | 2  | -9.999999999E7 | double      |

    Scenario: greatest and least of DECIMAL and DOUBLE values are DOUBLE
      When query
        """
        SELECT
          id,
          greatest(de, d) AS g,
          least(de, d) AS l,
          typeof(greatest(de, d)) AS g_type
        FROM VALUES
          (0, CAST(1.25 AS DECIMAL(10,2)), CAST('NaN' AS DOUBLE)),
          (1, CAST(3 AS DECIMAL(10,2)), CAST(1e20 AS DOUBLE))
        AS t(id, de, d)
        ORDER BY id
        """
      Then query result ordered
        | id | g      | l    | g_type |
        | 0  | NaN    | 1.25 | double |
        | 1  | 1.0E20 | 3.0  | double |

  Rule: Comparisons cast the DECIMAL value to DOUBLE

    Scenario: DECIMAL and DOUBLE comparisons
      When query
        """
        SELECT
          id,
          de = d AS eq,
          de < d AS lt,
          d <= de AS le,
          de <=> d AS eq_null_safe,
          de IN (d) AS in_list,
          d BETWEEN de AND 1e30 AS between,
          CASE de WHEN d THEN 'y' ELSE 'n' END AS simple_case
        FROM VALUES
          (0, CAST(1.25 AS DECIMAL(10,2)), CAST('NaN' AS DOUBLE)),
          (1, CAST(3 AS DECIMAL(10,2)), CAST(1e20 AS DOUBLE)),
          (2, CAST(1.25 AS DECIMAL(10,2)), CAST(1.25 AS DOUBLE))
        AS t(id, de, d)
        ORDER BY id
        """
      Then query result ordered
        | id | eq    | lt    | le    | eq_null_safe | in_list | between | simple_case |
        | 0  | false | true  | false | false        | false   | false   | n           |
        | 1  | false | true  | false | false        | false   | true    | n           |
        | 2  | true  | false | true  | true         | true    | true    | y           |

    Scenario: DECIMAL and FLOAT comparisons
      When query
        """
        SELECT id, de = f AS eq, f > de AS gt, de IN (f, 2D) AS in_list
        FROM VALUES
          (0, CAST(1.25 AS DECIMAL(10,2)), CAST(1.5 AS FLOAT)),
          (1, CAST(1.25 AS DECIMAL(10,2)), CAST('NaN' AS FLOAT)),
          (2, CAST(1.25 AS DECIMAL(10,2)), CAST(1.25 AS FLOAT))
        AS t(id, de, f)
        ORDER BY id
        """
      Then query result ordered
        | id | eq    | gt    | in_list |
        | 0  | false | true  | false   |
        | 1  | false | true  | false   |
        | 2  | true  | false | true    |

    Scenario: DOUBLE values compared with DECIMAL literals
      When query
        """
        SELECT
          id,
          d = 1.25 AS eq,
          d > 1.5 AS gt,
          d IN (1.25, 2.5) AS in_list,
          d = CAST(1e20 AS DECIMAL(38,0)) AS eq_wide
        FROM VALUES
          (0, CAST('NaN' AS DOUBLE)),
          (1, CAST('Infinity' AS DOUBLE)),
          (2, CAST(1e20 AS DOUBLE)),
          (3, CAST(1.25 AS DOUBLE))
        AS t(id, d)
        ORDER BY id
        """
      Then query result ordered
        | id | eq    | gt    | in_list | eq_wide |
        | 0  | false | true  | false   | false   |
        | 1  | false | true  | false   | false   |
        | 2  | false | true  | false   | true    |
        | 3  | true  | false | true    | false   |

  Rule: Set operations

    Scenario: UNION ALL of DECIMAL and DOUBLE values is DOUBLE
      When query
        """
        SELECT de AS c FROM VALUES (CAST(1.25 AS DECIMAL(10,2))) AS t(de)
        UNION ALL
        SELECT d FROM VALUES (CAST('NaN' AS DOUBLE)) AS u(d)
        """
      Then query column types
        | column | type   |
        | c      | double |
      And query result
        | c    |
        | 1.25 |
        | NaN  |

    Scenario: UNION of FLOAT and DECIMAL values is DOUBLE
      When query
        """
        SELECT f AS c FROM VALUES (CAST(1.5 AS FLOAT)) AS u(f)
        UNION
        SELECT de FROM VALUES (CAST(1.25 AS DECIMAL(38,10))) AS t(de)
        """
      Then query column types
        | column | type   |
        | c      | double |
      And query result
        | c    |
        | 1.25 |
        | 1.5  |
