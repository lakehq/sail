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

  Rule: UNION column types

    Scenario Outline: UNION retains a DOUBLE first input when combined with DECIMAL: <operator>, ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(v) AS result_type
        FROM (
          SELECT CAST(1.25 AS DOUBLE) AS v
          <operator>
          SELECT CAST(2.5 AS DECIMAL(2,1)) AS v
        ) AS q
        """
      Then query result
        | result_type |
        | double      |
        | double      |

      Examples:
        | operator  | ansi  |
        | UNION ALL | false |
        | UNION     | false |
        | UNION ALL | true  |
        | UNION     | true  |

    Scenario: UNION keeps interval values from inputs with different qualifiers
      When query
        """
        SELECT k, CAST(v AS INT) AS months
        FROM (
          SELECT 1 AS k, INTERVAL '14' MONTH AS v
          UNION ALL
          SELECT 2 AS k, INTERVAL '1' YEAR AS v
        ) AS q
        ORDER BY k
        """
      Then query result ordered
        | k | months |
        | 1 | 14     |
        | 2 | 12     |

    Scenario: UNION of DATE and TIMESTAMP matches DATE values in a non-UTC session time zone
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT d
        FROM VALUES (DATE '2024-06-15'), (DATE '2024-06-16') AS t(d)
        WHERE d IN (SELECT DATE '2024-06-15' UNION ALL SELECT TIMESTAMP '2000-01-01 00:00:00')
        ORDER BY d
        """
      Then query result ordered
        | d          |
        | 2024-06-15 |

    @sail-bug
    Scenario: UNION widens interval qualifiers like Spark
      When query
        """
        SELECT v FROM (SELECT INTERVAL '1' DAY AS v UNION ALL SELECT INTERVAL '1 02' DAY TO HOUR AS v)
        """
      Then query schema
        """
        root
         |-- v: interval day to hour (nullable = false)
        """

    @sail-bug
    Scenario: UNION widens DATE with TIMESTAMP to TIMESTAMP
      When query
        """
        SELECT typeof(v) AS result_type
        FROM (SELECT DATE '2024-06-15' AS v UNION ALL SELECT TIMESTAMP '2000-01-01 00:00:00' AS v)
        """
      Then query result
        | result_type |
        | timestamp   |
        | timestamp   |

    Scenario Outline: ANSI STRING-first <number_type> UNION conditional consumers retain fractional values
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id, CAST(<expression> AS DOUBLE) AS value
        FROM (
          SELECT 0 AS id, '2.5' AS v
          <operator>
          SELECT 1 AS id, CAST(2.5 AS <number_type>) AS v
        ) AS q
        ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 0  | 0.0   |
        | 1  | 2.5   |

      Examples:
        | number_type  | operator  | expression                         |
        | DECIMAL(2,1) | UNION ALL | IF(id = 0, 0, v)                   |
        | FLOAT        | UNION     | CASE WHEN id = 0 THEN 0 ELSE v END |
        | DOUBLE       | UNION ALL | NVL2(NULLIF(id, 1), 0, v)          |

    Scenario: ANSI fractional UNION conditional skips an invalid unselected string
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id, CAST(IF(id = 0, 0, v) AS DOUBLE) AS value
        FROM (
          SELECT 0 AS id, 'bad' AS v
          UNION ALL
          SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v
        ) AS q
        ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 0  | 0.0   |
        | 1  | 2.5   |

    Scenario: ANSI fractional UNION conditional retains a projected COALESCE value
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id, CAST(IF(id = 0, 0, x) AS DOUBLE) AS value
        FROM (
          SELECT id, coalesce(v, NULL) AS x
          FROM (
            SELECT 0 AS id, '2.5' AS v
            UNION ALL
            SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v
          ) AS q
        ) AS p
        ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 0  | 0.0   |
        | 1  | 2.5   |

    Scenario Outline: ANSI fractional UNION conditional preserves <kind> leaves
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id, CAST(<leaf> AS DOUBLE) AS value
        FROM (
          SELECT id, <expression> AS result
          FROM (
            SELECT 0 AS id, <string_value> AS v
            UNION ALL
            SELECT 1 AS id, <numeric_value> AS v
          ) AS q
        ) AS p
        ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 0  | 0.0   |
        | 1  | 2.5   |

      Examples:
        | kind         | string_value                   | numeric_value                                         | expression                                                    | leaf        |
        | ARRAY        | array('2.5')                   | array(CAST(2.5 AS DECIMAL(2,1)))                        | IF(id = 0, array(0), v)                                        | result[0]   |
        | STRUCT       | named_struct('a', '2.5')        | named_struct('a', CAST(2.5 AS DECIMAL(2,1)))             | CASE WHEN id = 0 THEN named_struct('a', 0) ELSE v END           | result.a    |
        | MAP          | map('a', '2.5')                 | map('a', CAST(2.5 AS DECIMAL(2,1)))                      | IF(id = 0, map('a', 0), v)                                     | result['a'] |
        | ARRAY STRUCT | array(named_struct('a', '2.5')) | array(named_struct('a', CAST(2.5 AS DECIMAL(2,1))))      | CASE WHEN id = 0 THEN array(named_struct('a', 0)) ELSE v END    | result[0].a |

    Scenario Outline: An enclosing <number_type> conditional preserves fractional UNION values
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id, CAST(IF(id = 9, CAST(0 AS <number_type>), x) AS DOUBLE) AS value
        FROM (
          SELECT id, IF(id = 0, 0, v) AS x
          FROM (
            SELECT 0 AS id, '2.55' AS v
            UNION ALL
            SELECT 1 AS id, CAST(2.55 AS DECIMAL(3,2)) AS v
          ) AS q
        ) AS p
        ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 0  | 0.0   |
        | 1  | 2.55  |

      Examples:
        | number_type  |
        | BIGINT       |
        | DECIMAL(2,1) |

    Scenario: ANSI chained UNION conditional retains exact DECIMAL precision
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id, CAST(IF(id = 9, 0, v) AS DECIMAL(22,1)) AS value
        FROM (
          SELECT 0 AS id, '9007199254740993' AS v
          UNION ALL
          SELECT 1 AS id, 1 AS v
          UNION ALL
          SELECT 2 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v
        ) AS q
        ORDER BY id
        """
      Then query result ordered
        | id | value              |
        | 0  | 9007199254740993.0 |
        | 1  | 1.0                |
        | 2  | 2.5                |

    Scenario: ANSI fractional UNION conditional INSERT retains scalar nested and exact values
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.storeAssignmentPolicy = ANSI
      And variable location for temporary directory fractional_union_assignment
      And final statement
        """
        DROP TABLE IF EXISTS fractional_union_assignment
        """
      And statement template
        """
        CREATE TABLE fractional_union_assignment (
          id INT, scalar_value DOUBLE, nested_value ARRAY<DOUBLE>, exact_value DECIMAL(22,1)
        ) USING PARQUET LOCATION {{ location.sql }}
        """
      And statement
        """
        INSERT INTO fractional_union_assignment
        SELECT id, IF(id = 0, 0, v), IF(id = 0, array(0), a), IF(id = 9, 0, v)
        FROM (
          SELECT 0 AS id, '9007199254740993' AS v, array('2.5') AS a
          UNION ALL
          SELECT 1 AS id, 1 AS v, array(1) AS a
          UNION ALL
          SELECT 2 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v, array(CAST(2.5 AS DECIMAL(2,1))) AS a
        ) AS q
        """
      When query
        """
        SELECT id, scalar_value, nested_value[0] AS nested_value, exact_value
        FROM fractional_union_assignment ORDER BY id
        """
      Then query result ordered
        | id | scalar_value | nested_value | exact_value        |
        | 0  | 0.0          | 0.0          | 9007199254740993.0 |
        | 1  | 1.0          | 1.0          | 1.0                |
        | 2  | 2.5          | 2.5          | 2.5                |

    Scenario Outline: ANSI fractional UNION conditional retains values through a <kind> view
      Given config spark.sql.ansi.enabled = true
      And final statement
        """
        DROP VIEW IF EXISTS <view_name>
        """
      And statement
        """
        <create_view> AS
        SELECT id, IF(id = 0, 0, v) AS v
        FROM (
          SELECT 0 AS id, '2.5' AS v
          UNION ALL
          SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v
        ) AS q
        """
      When query
        """
        SELECT id, CAST(IF(id = 9, 0L, v) AS DOUBLE) AS value
        FROM <view_name> ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 0  | 0.0   |
        | 1  | 2.5   |

      Examples:
        | kind       | create_view                                              | view_name                            |
        | temporary  | CREATE OR REPLACE TEMP VIEW fractional_union_view         | fractional_union_view                |
        | global     | CREATE OR REPLACE GLOBAL TEMP VIEW fractional_union_view  | global_temp.fractional_union_view    |
        | persistent | CREATE OR REPLACE VIEW fractional_union_view              | fractional_union_view                |

    Scenario: ANSI fractional UNION scalar subquery preserves an enclosing numeric conditional
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(IF(false, 0L, x) AS DOUBLE) AS value
        FROM (
          SELECT IF(false, 0, (
            SELECT v
            FROM (
              SELECT 0 AS id, '2.5' AS v
              UNION ALL
              SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v
            ) AS q
            WHERE id = 1
          )) AS x
        ) AS p
        """
      Then query result
        | value |
        | 2.5   |

    @sail-bug
    Scenario: An unrelated numeric conditional widens precisely beside an unresolved fractional STRING UNION
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id, CAST(IF(id = 0, n, CAST(0 AS FLOAT)) AS BIGINT) AS value
        FROM (
          SELECT 0 AS id, '2.5' AS v, 16777217L AS n
          UNION ALL
          SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v, 0L AS n
        ) AS q
        ORDER BY id
        """
      Then query result ordered
        | id | value    |
        | 0  | 16777217 |
        | 1  | 0        |

    Scenario: ANSI fractional UNION conditional retains values after inferred Parquet materialization
      Given config spark.sql.ansi.enabled = true
      And variable location for temporary directory fractional_union_materialized
      And final statement
        """
        DROP TABLE IF EXISTS fractional_union_materialized
        """
      And statement template
        """
        CREATE TABLE fractional_union_materialized
        USING PARQUET LOCATION {{ location.sql }} AS
        SELECT 0 AS id, '2.5' AS v
        UNION ALL
        SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v
        """
      When query
        """
        SELECT id, CAST(IF(id = 0, 0, v) AS DOUBLE) AS value
        FROM fractional_union_materialized ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 0  | 0.0   |
        | 1  | 2.5   |

    Scenario Outline: A <kind> view preserves fractional conditionals over materialized UNION values
      Given config spark.sql.ansi.enabled = true
      And variable location for temporary directory fractional_union_view_source
      And final statement
        """
        DROP TABLE IF EXISTS fractional_union_view_source
        """
      And final statement
        """
        DROP VIEW IF EXISTS <view_name>
        """
      And statement template
        """
        CREATE TABLE fractional_union_view_source
        USING PARQUET LOCATION {{ location.sql }} AS
        SELECT 0 AS id, '2.5' AS v
        UNION ALL
        SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v
        """
      And statement
        """
        CREATE OR REPLACE <view_kind> VIEW fractional_union_materialized_view AS
        SELECT id, IF(id = 0, 0, v) AS x FROM fractional_union_view_source
        """
      When query
        """
        SELECT id, CAST(IF(id = 9, 0L, x) AS DOUBLE) AS value
        FROM <view_name> ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 0  | 0.0   |
        | 1  | 2.5   |

      Examples:
        | kind      | view_kind   | view_name                                     |
        | temporary | TEMP        | fractional_union_materialized_view            |
        | global    | GLOBAL TEMP | global_temp.fractional_union_materialized_view |

    @sail-bug
    Scenario: ANSI numeric conditional promotes a dynamic STRING column to BIGINT
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id, typeof(IF(id = 0, 0, v)) AS result_type
        FROM VALUES (0, '1'), (1, '2') AS t(id, v)
        ORDER BY id
        """
      Then query result ordered
        | id | result_type |
        | 0  | bigint      |
        | 1  | bigint      |

  Rule: UNION nested field metadata

    Scenario Outline: UNION preserves nested interval qualifiers: <operator>
      When query
        """
        SELECT k, CAST(s.v AS INT) AS years
        FROM (
          SELECT 1 AS k, struct(INTERVAL '1' YEAR AS v) AS s
          <operator>
          SELECT 2 AS k, struct(CAST(NULL AS INTERVAL YEAR) AS v) AS s
        ) AS q
        ORDER BY k
        """
      Then query result ordered
        | k | years |
        | 1 | 1     |
        | 2 | NULL  |

      Examples:
        | operator  |
        | UNION ALL |
        | UNION     |

    Scenario: UNION widens numeric fields beside nested interval metadata
      When query
        """
        SELECT typeof(s.n) AS number_type
        FROM (
          SELECT struct(INTERVAL '1' YEAR AS v, CAST(1 AS INT) AS n) AS s
          UNION ALL
          SELECT struct(CAST(NULL AS INTERVAL YEAR) AS v, CAST(2 AS BIGINT) AS n) AS s
        ) AS q
        """
      Then query result
        | number_type |
        | bigint      |
        | bigint      |

  Rule: UNION timestamp consumers

    Scenario Outline: UNION preserves timestamp units for UTC conversions: <operator>, <kind>, ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      And config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT
          from_utc_timestamp(v, 'America/Los_Angeles') AS from_utc,
          to_utc_timestamp(v, 'America/Los_Angeles') AS to_utc
        FROM (
          SELECT v
          FROM VALUES
            (CAST('2024-06-15 12:00:00' AS <kind>)),
            (CAST(NULL AS <kind>))
            AS t(v)
          <operator>
          SELECT '2024-06-16 12:00:00' AS v
        ) AS q
        ORDER BY from_utc
        """
      Then query result ordered
        | from_utc            | to_utc              |
        | NULL                | NULL                |
        | 2024-06-15 05:00:00 | 2024-06-15 19:00:00 |
        | 2024-06-16 05:00:00 | 2024-06-16 19:00:00 |
      And query schema
        """
        root
         |-- from_utc: timestamp (nullable = true)
         |-- to_utc: timestamp (nullable = true)
        """

      Examples:
        | operator  | kind          | ansi  |
        | UNION ALL | TIMESTAMP     | true  |
        | UNION ALL | TIMESTAMP     | false |
        | UNION     | TIMESTAMP     | true  |
        | UNION     | TIMESTAMP     | false |
        | UNION ALL | TIMESTAMP_NTZ | true  |
        | UNION ALL | TIMESTAMP_NTZ | false |
        | UNION     | TIMESTAMP_NTZ | true  |
        | UNION     | TIMESTAMP_NTZ | false |

    @sail-bug
    Scenario: An ANSI TIMESTAMP and STRING UNION returns Spark timestamps
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(NULL AS TIMESTAMP) AS v
        UNION ALL
        SELECT CAST(NULL AS STRING) AS v
        """
      Then query result collected
        | v    |
        | NULL |
        | NULL |
      And query schema
        """
        root
         |-- v: timestamp (nullable = true)
        """

  Rule: UNION preserves existing consumers while widening its declared types

    Scenario Outline: Floating-point UNION inputs retain DOUBLE division results: <operator>, ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT
          scalar_value / 2 AS scalar_result,
          typeof(scalar_value / 2) AS scalar_type,
          struct_value.n / 2 AS struct_result,
          typeof(struct_value.n / 2) AS struct_type,
          array_value[0] / 2 AS array_result,
          typeof(array_value[0] / 2) AS array_type
        FROM (
          SELECT 0 AS id, CAST(1.25 AS FLOAT) AS scalar_value,
            struct(CAST(1.25 AS DOUBLE) AS n) AS struct_value,
            array(CAST(1.25 AS FLOAT)) AS array_value
          <operator>
          SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS scalar_value,
            struct(CAST(2.5 AS DECIMAL(2,1)) AS n) AS struct_value,
            array(CAST(2.5 AS DECIMAL(2,1))) AS array_value
        ) AS q
        ORDER BY id
        """
      Then query result ordered
        | scalar_result | scalar_type | struct_result | struct_type | array_result | array_type |
        | 0.625         | double      | 0.625         | double      | 0.625        | double     |
        | 1.25          | double      | 1.25          | double      | 1.25         | double     |

      Examples:
        | operator  | ansi  |
        | UNION ALL | false |
        | UNION     | false |
        | UNION ALL | true  |
        | UNION     | true  |

    Scenario Outline: UNION preserves floating-point map value division: ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT v['n'] / 2 AS result, typeof(v['n'] / 2) AS result_type
        FROM (
          SELECT 0 AS id, map('n', CAST(1.25 AS DOUBLE)) AS v
          UNION ALL
          SELECT 1 AS id, map('n', CAST(2.5 AS DECIMAL(2,1))) AS v
        ) AS q
        ORDER BY id
        """
      Then query result ordered
        | result | result_type |
        | 0.625  | double      |
        | 1.25   | double      |

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario Outline: UNION preserves UTC conversion of TIMESTAMP-first nested inputs: <operator>, <kind>, ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      And config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT
          from_utc_timestamp(scalar_value, 'America/Los_Angeles') AS scalar_from_utc,
          to_utc_timestamp(scalar_value, 'America/Los_Angeles') AS scalar_to_utc,
          from_utc_timestamp(struct_value.n, 'America/Los_Angeles') AS struct_from_utc,
          to_utc_timestamp(array_value[0], 'America/Los_Angeles') AS array_to_utc
        FROM (
          SELECT 0 AS id, CAST('2024-06-15 12:00:00' AS <kind>) AS scalar_value,
            struct(CAST('2024-06-15 12:00:00' AS <kind>) AS n) AS struct_value,
            array(CAST('2024-06-15 12:00:00' AS <kind>)) AS array_value
          <operator>
          SELECT 1 AS id, '2024-06-16 12:00:00' AS scalar_value,
            struct('2024-06-16 12:00:00' AS n) AS struct_value,
            array('2024-06-16 12:00:00') AS array_value
        ) AS q
        ORDER BY id
        """
      Then query result ordered
        | scalar_from_utc     | scalar_to_utc       | struct_from_utc     | array_to_utc        |
        | 2024-06-15 05:00:00 | 2024-06-15 19:00:00 | 2024-06-15 05:00:00 | 2024-06-15 19:00:00 |
        | 2024-06-16 05:00:00 | 2024-06-16 19:00:00 | 2024-06-16 05:00:00 | 2024-06-16 19:00:00 |
      And query schema
        """
        root
         |-- scalar_from_utc: timestamp (nullable = true)
         |-- scalar_to_utc: timestamp (nullable = true)
         |-- struct_from_utc: timestamp (nullable = true)
         |-- array_to_utc: timestamp (nullable = true)
        """

      Examples:
        | operator  | kind          | ansi  |
        | UNION ALL | TIMESTAMP     | true  |
        | UNION ALL | TIMESTAMP     | false |
        | UNION     | TIMESTAMP     | true  |
        | UNION     | TIMESTAMP     | false |
        | UNION ALL | TIMESTAMP_NTZ | true  |
        | UNION ALL | TIMESTAMP_NTZ | false |
        | UNION     | TIMESTAMP_NTZ | true  |
        | UNION     | TIMESTAMP_NTZ | false |

    Scenario Outline: UNION preserves UTC conversion of map values: <kind>, ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      And config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT
          from_utc_timestamp(v['n'], 'America/Los_Angeles') AS from_utc,
          to_utc_timestamp(v['n'], 'America/Los_Angeles') AS to_utc
        FROM (
          SELECT 0 AS id, map('n', CAST('2024-06-15 12:00:00' AS <kind>)) AS v
          UNION ALL
          SELECT 1 AS id, map('n', '2024-06-16 12:00:00') AS v
        ) AS q
        ORDER BY id
        """
      Then query result ordered
        | from_utc            | to_utc              |
        | 2024-06-15 05:00:00 | 2024-06-15 19:00:00 |
        | 2024-06-16 05:00:00 | 2024-06-16 19:00:00 |
      And query schema
        """
        root
         |-- from_utc: timestamp (nullable = true)
         |-- to_utc: timestamp (nullable = true)
        """

      Examples:
        | kind          | ansi  |
        | TIMESTAMP     | true  |
        | TIMESTAMP     | false |
        | TIMESTAMP_NTZ | true  |
        | TIMESTAMP_NTZ | false |

    Scenario Outline: Persistent views retain non-ANSI UNION conditional values: <expression>, <operator>
      Given config spark.sql.ansi.enabled = false
      And statement
        """
        CREATE OR REPLACE VIEW union_conditional_legacy_view AS
        SELECT id, <expression> AS result
        FROM (
          SELECT 0 AS id, 1 AS v
          <operator>
          SELECT 1 AS id, 'x' AS v
        ) AS q
        """
      And final statement
        """
        DROP VIEW IF EXISTS union_conditional_legacy_view
        """
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id, result
        FROM union_conditional_legacy_view
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 0  | 2      |
        | 1  | x      |

      Examples:
        | expression                                                 | operator  |
        | IF(id = 0, CAST(2 AS BIGINT), v)                             | UNION ALL |
        | IF(id = 0, CAST(2 AS BIGINT), v)                             | UNION     |
        | CASE WHEN id = 0 THEN CAST(2 AS BIGINT) ELSE v END            | UNION ALL |
        | CASE WHEN id = 0 THEN CAST(2 AS BIGINT) ELSE v END            | UNION     |
        | nvl2(nullif(id, 1), CAST(2 AS BIGINT), v)                     | UNION ALL |
        | nvl2(nullif(id, 1), CAST(2 AS BIGINT), v)                     | UNION     |

    @sail-bug
    Scenario: UNION widens raw FLOAT and DECIMAL output to DOUBLE
      When query
        """
        SELECT CAST(1.25 AS FLOAT) AS v
        UNION ALL
        SELECT CAST(2.5 AS DECIMAL(2,1)) AS v
        """
      Then query schema
        """
        root
         |-- v: double (nullable = false)
        """
      And query result
        | v    |
        | 1.25 |
        | 2.5  |

    Scenario Outline: UNION preserves widened sibling precision in <container>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT id, CAST(result AS BIGINT) AS value, typeof(result) AS result_type
        FROM (
          SELECT id, if(id = 0, CAST(2 AS FLOAT), <access>) AS result
          FROM (
            SELECT 0 AS id, <first> AS v
            UNION ALL
            SELECT 1 AS id, <second> AS v
          ) AS inputs
        ) AS results
        ORDER BY id
        """
      Then query result ordered
        | id | value    | result_type |
        | 0  | 2        | double      |
        | 1  | 16777217 | double      |

      Examples:
        | container           | first                                                      | second                                                                               | access      |
        | a struct            | named_struct('a', CAST(1 AS FLOAT), 'b', 1)                 | named_struct('a', CAST(1 AS DECIMAL(5,2)), 'b', CAST(16777217 AS DOUBLE))                 | v.b         |
        | an array of structs | array(named_struct('a', CAST(1 AS FLOAT), 'b', 1))          | array(named_struct('a', CAST(1 AS DECIMAL(5,2)), 'b', CAST(16777217 AS DOUBLE)))          | v[0].b      |
        | a map of arrays     | map('k', array(named_struct('a', CAST(1 AS FLOAT), 'b', 1))) | map('k', array(named_struct('a', CAST(1 AS DECIMAL(5,2)), 'b', CAST(16777217 AS DOUBLE)))) | v['k'][0].b |

    @sail-bug
    Scenario: STRING-first UNION inputs support UTC conversion
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT from_utc_timestamp(v, 'America/Los_Angeles') AS result
        FROM (
          SELECT '2024-06-15 12:00:00' AS v
          UNION ALL
          SELECT TIMESTAMP '2024-06-16 12:00:00' AS v
        ) AS q
        ORDER BY result
        """
      Then query result ordered
        | result              |
        | 2024-06-15 05:00:00 |
        | 2024-06-16 05:00:00 |
      And query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    @sail-bug
    Scenario: ANSI UNION widens nested numeric and STRING leaves to BIGINT
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT v.n AS value, typeof(v) AS result_type
        FROM (
          SELECT 0 AS id, named_struct('n', 1) AS v
          UNION ALL
          SELECT 1 AS id, named_struct('n', '7') AS v
        ) AS inputs
        ORDER BY id
        """
      Then query result ordered
        | value | result_type      |
        | 1     | struct<n:bigint> |
        | 7     | struct<n:bigint> |

    Scenario Outline: UNION widens numeric siblings without losing interval metadata: <container>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          id,
          CAST(<access>.v AS INT) AS years,
          CAST(IF(id = 0, CAST(2 AS FLOAT), <access>.n) AS BIGINT) AS if_value,
          CAST(CASE WHEN id = 0 THEN CAST(2 AS FLOAT) ELSE <access>.n END AS BIGINT) AS case_value,
          CAST(NVL2(NULLIF(id, 1), CAST(2 AS FLOAT), <access>.n) AS BIGINT) AS nvl2_value
        FROM (
          SELECT 0 AS id, <first> AS v
          <operator>
          SELECT 1 AS id, <second> AS v
        ) AS q
        ORDER BY id
        """
      Then query result ordered
        | id | years | if_value | case_value | nvl2_value |
        | 0  | 1     | 2        | 2          | 2          |
        | 1  | 2     | 16777217 | 16777217   | 16777217   |

      Examples:
        | container | operator  | first                                                                 | second                                                                          | access  |
        | struct    | UNION ALL | struct(INTERVAL '1' YEAR AS v, CAST(1 AS INT) AS n)                 | struct(INTERVAL '2' YEAR AS v, CAST(16777217 AS DOUBLE) AS n)                 | v       |
        | struct    | UNION     | struct(INTERVAL '1' YEAR AS v, CAST(1 AS INT) AS n)                 | struct(INTERVAL '2' YEAR AS v, CAST(16777217 AS DOUBLE) AS n)                 | v       |
        | array     | UNION ALL | array(struct(INTERVAL '1' YEAR AS v, CAST(1 AS INT) AS n))          | array(struct(INTERVAL '2' YEAR AS v, CAST(16777217 AS DOUBLE) AS n))          | v[0]    |
        | map       | UNION ALL | map('k', struct(INTERVAL '1' YEAR AS v, CAST(1 AS INT) AS n))       | map('k', struct(INTERVAL '2' YEAR AS v, CAST(16777217 AS DOUBLE) AS n))       | v['k']  |

  Rule: Timestamp conversions over UNION results

    Scenario Outline: <function> preserves nonnullable timestamps from <operator> in <timezone>
      Given config spark.sql.ansi.enabled = <ansi>
      And config spark.sql.session.timeZone = <timezone>
      When query
        """
        SELECT id, <function>(v) AS result
        FROM (
          SELECT 0 AS id, TIMESTAMP_NTZ '2024-06-15 12:30:00' AS v
          <operator>
          SELECT 1 AS id, TIMESTAMP '2024-06-16 12:30:00' AS v
        ) AS timestamps
        ORDER BY id
        """
      Then query result ordered
        | id | result              |
        | 0  | 2024-06-15 12:30:00 |
        | 1  | 2024-06-16 12:30:00 |
      And query schema
        """
        root
         |-- id: integer (nullable = false)
         |-- result: timestamp (nullable = false)
        """

      Examples:
        | function         | operator  | timezone            | ansi  |
        | to_timestamp     | UNION ALL | UTC                 | false |
        | to_timestamp     | UNION     | America/Los_Angeles | true  |
        | to_timestamp_ltz | UNION ALL | America/Los_Angeles | false |
        | to_timestamp_ltz | UNION     | UTC                 | true  |

    Scenario: Timestamp conversion preserves nullable UNION input
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT id, to_timestamp(v) AS result
        FROM (
          SELECT 0 AS id, CAST(NULL AS TIMESTAMP_NTZ) AS v
          UNION ALL
          SELECT 1 AS id, TIMESTAMP '2024-06-16 12:30:00' AS v
        ) AS timestamps
        ORDER BY id
        """
      Then query result ordered
        | id | result              |
        | 0  | NULL                |
        | 1  | 2024-06-16 12:30:00 |
      And query schema
        """
        root
         |-- id: integer (nullable = false)
         |-- result: timestamp (nullable = true)
        """

    @sail-bug
    Scenario: Mixed NTZ and LTZ UNION inputs expose their common timestamp type
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT id, typeof(v) AS result_type
        FROM (
          SELECT 0 AS id, TIMESTAMP_NTZ '2024-06-15 12:30:00' AS v
          UNION ALL
          SELECT 1 AS id, TIMESTAMP '2024-06-16 12:30:00' AS v
        ) AS timestamps
        ORDER BY id
        """
      Then query result ordered
        | id | result_type |
        | 0  | timestamp   |
        | 1  | timestamp   |

    @sail-bug
    Scenario: Casting a mixed NTZ and LTZ UNION preserves instants in the session timezone
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT id, CAST(v AS TIMESTAMP) AS result
        FROM (
          SELECT 0 AS id, TIMESTAMP_NTZ '2024-06-15 12:30:00' AS v
          UNION ALL
          SELECT 1 AS id, TIMESTAMP '2024-06-16 12:30:00' AS v
        ) AS timestamps
        ORDER BY id
        """
      Then query result ordered
        | id | result              |
        | 0  | 2024-06-15 12:30:00 |
        | 1  | 2024-06-16 12:30:00 |

    @sail-bug
    Scenario: Timezone conversion consumers accept mixed NTZ and LTZ UNION input
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT id, convert_timezone('UTC', 'America/Los_Angeles', v) AS result
        FROM (
          SELECT 0 AS id, TIMESTAMP_NTZ '2024-06-15 12:30:00' AS v
          UNION ALL
          SELECT 1 AS id, TIMESTAMP '2024-06-16 12:30:00' AS v
        ) AS timestamps
        ORDER BY id
        """
      Then query result ordered
        | id | result              |
        | 0  | 2024-06-15 05:30:00 |
        | 1  | 2024-06-16 05:30:00 |
