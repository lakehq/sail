Feature: Year-month and day-time intervals multiplied or divided by a numeric value

  The result is exact, whether or not ANSI mode is enabled: an integral product that overflows
  raises an error, an integral quotient and a floating point or decimal result are rounded half
  away from zero to whole months or microseconds, and a division by zero raises an error.

  Rule: Numeric multipliers

    Scenario Outline: Year-month interval times <case> (ANSI <ansi>)
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT id, CAST(ym * <num> AS STRING) AS r1, CAST(<num> * ym AS STRING) AS r2
        FROM VALUES
          (0, INTERVAL '1-3' YEAR TO MONTH),
          (1, INTERVAL '-1-3' YEAR TO MONTH),
          (2, CAST(NULL AS INTERVAL YEAR TO MONTH)),
          (3, INTERVAL '0-1' YEAR TO MONTH)
        AS t(id, ym)
        ORDER BY id
        """
      Then query result ordered
        | id | r1     | r2     |
        | 0  | <r0>   | <r0>   |
        | 1  | <r1>   | <r1>   |
        | 2  | NULL   | NULL   |
        | 3  | <r3>   | <r3>   |

      Examples:
        | case               | ansi  | num                    | r0                            | r1                             | r3                            |
        | int                | true  | 2                      | INTERVAL '2-6' YEAR TO MONTH  | INTERVAL '-2-6' YEAR TO MONTH  | INTERVAL '0-2' YEAR TO MONTH  |
        | int                | false | 2                      | INTERVAL '2-6' YEAR TO MONTH  | INTERVAL '-2-6' YEAR TO MONTH  | INTERVAL '0-2' YEAR TO MONTH  |
        | tinyint            | true  | CAST(2 AS TINYINT)     | INTERVAL '2-6' YEAR TO MONTH  | INTERVAL '-2-6' YEAR TO MONTH  | INTERVAL '0-2' YEAR TO MONTH  |
        | smallint           | true  | CAST(2 AS SMALLINT)    | INTERVAL '2-6' YEAR TO MONTH  | INTERVAL '-2-6' YEAR TO MONTH  | INTERVAL '0-2' YEAR TO MONTH  |
        | bigint             | true  | 2L                     | INTERVAL '2-6' YEAR TO MONTH  | INTERVAL '-2-6' YEAR TO MONTH  | INTERVAL '0-2' YEAR TO MONTH  |
        | double             | true  | 1.5D                   | INTERVAL '1-11' YEAR TO MONTH | INTERVAL '-1-11' YEAR TO MONTH | INTERVAL '0-2' YEAR TO MONTH  |
        | double             | false | 1.5D                   | INTERVAL '1-11' YEAR TO MONTH | INTERVAL '-1-11' YEAR TO MONTH | INTERVAL '0-2' YEAR TO MONTH  |
        | float              | true  | CAST(1.5 AS FLOAT)     | INTERVAL '1-11' YEAR TO MONTH | INTERVAL '-1-11' YEAR TO MONTH | INTERVAL '0-2' YEAR TO MONTH  |
        | decimal            | true  | 1.5                    | INTERVAL '1-11' YEAR TO MONTH | INTERVAL '-1-11' YEAR TO MONTH | INTERVAL '0-2' YEAR TO MONTH  |
        | double half        | true  | 0.5D                   | INTERVAL '0-8' YEAR TO MONTH  | INTERVAL '-0-8' YEAR TO MONTH  | INTERVAL '0-1' YEAR TO MONTH  |
        | negative double    | true  | -0.5D                  | INTERVAL '-0-8' YEAR TO MONTH | INTERVAL '0-8' YEAR TO MONTH   | INTERVAL '-0-1' YEAR TO MONTH |
        | double tenth       | true  | 0.1D                   | INTERVAL '0-2' YEAR TO MONTH  | INTERVAL '-0-2' YEAR TO MONTH  | INTERVAL '0-0' YEAR TO MONTH  |
        | float tenth        | true  | CAST(0.1 AS FLOAT)     | INTERVAL '0-2' YEAR TO MONTH  | INTERVAL '-0-2' YEAR TO MONTH  | INTERVAL '0-0' YEAR TO MONTH  |
        | decimal tenth      | true  | 0.1                    | INTERVAL '0-2' YEAR TO MONTH  | INTERVAL '-0-2' YEAR TO MONTH  | INTERVAL '0-0' YEAR TO MONTH  |
        | small decimal      | true  | 0.03333333333333333333 | INTERVAL '0-0' YEAR TO MONTH  | INTERVAL '0-0' YEAR TO MONTH   | INTERVAL '0-0' YEAR TO MONTH  |
        | wide decimal       | true  | CAST(2.5 AS DECIMAL(38,37)) | INTERVAL '3-2' YEAR TO MONTH | INTERVAL '-3-2' YEAR TO MONTH | INTERVAL '0-3' YEAR TO MONTH |
        | NULL               | true  | CAST(NULL AS INT)      | NULL                          | NULL                           | NULL                          |

    Scenario Outline: Year-month interval times a value rounds half away from zero: <case>
      When query
        """
        SELECT CAST(<expr> AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                    | expr                                        | result                                |
        | positive double half    | INTERVAL '0-3' YEAR TO MONTH * 2.5D         | INTERVAL '0-8' YEAR TO MONTH          |
        | negative double half    | INTERVAL '0-3' YEAR TO MONTH * -2.5D        | INTERVAL '-0-8' YEAR TO MONTH         |
        | positive decimal half   | INTERVAL '0-1' YEAR TO MONTH * 2.5          | INTERVAL '0-3' YEAR TO MONTH          |
        | negative decimal half   | INTERVAL '0-1' YEAR TO MONTH * -2.5         | INTERVAL '-0-3' YEAR TO MONTH         |
        | largest double          | INTERVAL '0-1' YEAR TO MONTH * 2147483647.49D | INTERVAL '178956970-7' YEAR TO MONTH |
        | smallest double         | INTERVAL '0-1' YEAR TO MONTH * -2147483648.49D | INTERVAL '-178956970-8' YEAR TO MONTH |
        | largest decimal         | INTERVAL '0-1' YEAR TO MONTH * 2147483647.49 | INTERVAL '178956970-7' YEAR TO MONTH |
        | largest int             | INTERVAL '0-1' YEAR TO MONTH * 2147483647   | INTERVAL '178956970-7' YEAR TO MONTH  |
        | smallest bigint         | INTERVAL '0-1' YEAR TO MONTH * -2147483648L | INTERVAL '-178956970-8' YEAR TO MONTH |
        | year interval           | INTERVAL '2' YEAR * 2                       | INTERVAL '4-0' YEAR TO MONTH          |
        | month interval          | INTERVAL '5' MONTH * 3                      | INTERVAL '1-3' YEAR TO MONTH          |
        | decimal column          | CAST(1.25 AS DECIMAL(10,2)) * INTERVAL '0-7' YEAR TO MONTH | INTERVAL '0-9' YEAR TO MONTH |

    Scenario: The product is a year-to-month interval
      When query
        """
        SELECT
          typeof(INTERVAL '1' YEAR * 2) AS t0,
          typeof(2.5D * INTERVAL '5' MONTH) AS t1,
          typeof(INTERVAL '1-3' YEAR TO MONTH * 1.5) AS t2
        """
      Then query result
        | t0                     | t1                     | t2                     |
        | interval year to month | interval year to month | interval year to month |

    Scenario: Year-month interval products in expressions and aggregates
      When query
        """
        SELECT
          CAST(sum(ym * 2) AS STRING) AS s,
          CAST(max(2 * ym) AS STRING) AS m,
          CAST(min(coalesce(ym * 2, INTERVAL '0-1' YEAR TO MONTH)) AS STRING) AS c
        FROM VALUES
          (INTERVAL '1-3' YEAR TO MONTH),
          (INTERVAL '-1-3' YEAR TO MONTH),
          (CAST(NULL AS INTERVAL YEAR TO MONTH)),
          (INTERVAL '0-1' YEAR TO MONTH)
        AS t(ym)
        """
      Then query result
        | s                            | m                            | c                             |
        | INTERVAL '0-2' YEAR TO MONTH | INTERVAL '2-6' YEAR TO MONTH | INTERVAL '-2-6' YEAR TO MONTH |

  Rule: Overflow raises an error whether or not ANSI mode is enabled

    Scenario Outline: Year-month interval product overflow: <case> (ANSI <ansi>)
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(<expr> AS STRING) AS result
        """
      Then query error <error>

      Examples:
        | case                    | ansi  | expr                                                 | error                     |
        | int                     | true  | INTERVAL '0-2' YEAR TO MONTH * 2147483647            | integer overflow          |
        | int                     | false | INTERVAL '0-2' YEAR TO MONTH * 2147483647            | integer overflow          |
        | negated smallest        | true  | INTERVAL '-178956970-8' YEAR TO MONTH * -1           | integer overflow          |
        | bigint                  | true  | INTERVAL '0-1' YEAR TO MONTH * 2147483648L           | integer overflow          |
        | bigint product          | true  | INTERVAL '0-2' YEAR TO MONTH * 9223372036854775807   | long overflow             |
        | double                  | true  | INTERVAL '0-1' YEAR TO MONTH * 2147483647.5D         | out of range              |
        | double                  | false | INTERVAL '0-1' YEAR TO MONTH * 2147483647.5D         | out of range              |
        | negative double         | true  | INTERVAL '0-1' YEAR TO MONTH * -2147483648.5D        | out of range              |
        | large double            | true  | INTERVAL '0-1' YEAR TO MONTH * 1e100D                | out of range              |
        | NaN                     | true  | INTERVAL '0-1' YEAR TO MONTH * CAST('NaN' AS DOUBLE) | infinite or NaN           |
        | infinity                | false | INTERVAL '0-1' YEAR TO MONTH * CAST('Infinity' AS DOUBLE) | infinite or NaN      |
        | decimal                 | true  | INTERVAL '0-1' YEAR TO MONTH * 2147483647.5          | Overflow                  |
        | negative decimal        | false | INTERVAL '0-1' YEAR TO MONTH * -2147483648.5         | Overflow                  |

  Rule: Year-month interval divided by a numeric value

    Scenario Outline: Year-month interval divided by <case> (ANSI <ansi>)
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT id, CAST(ym / <num> AS STRING) AS result
        FROM VALUES
          (0, INTERVAL '1-3' YEAR TO MONTH),
          (1, INTERVAL '-1-3' YEAR TO MONTH),
          (2, CAST(NULL AS INTERVAL YEAR TO MONTH)),
          (3, INTERVAL '0-1' YEAR TO MONTH)
        AS t(id, ym)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 0  | <r0>   |
        | 1  | <r1>   |
        | 2  | NULL   |
        | 3  | <r3>   |

      Examples:
        | case     | ansi  | num                | r0                            | r1                             | r3                            |
        | int      | true  | 2                  | INTERVAL '0-8' YEAR TO MONTH  | INTERVAL '-0-8' YEAR TO MONTH  | INTERVAL '0-1' YEAR TO MONTH  |
        | int      | false | 2                  | INTERVAL '0-8' YEAR TO MONTH  | INTERVAL '-0-8' YEAR TO MONTH  | INTERVAL '0-1' YEAR TO MONTH  |
        | negative | true  | -2                 | INTERVAL '-0-8' YEAR TO MONTH | INTERVAL '0-8' YEAR TO MONTH   | INTERVAL '-0-1' YEAR TO MONTH |
        | bigint   | true  | 4L                 | INTERVAL '0-4' YEAR TO MONTH  | INTERVAL '-0-4' YEAR TO MONTH  | INTERVAL '0-0' YEAR TO MONTH  |
        | tinyint  | true  | CAST(3 AS TINYINT) | INTERVAL '0-5' YEAR TO MONTH  | INTERVAL '-0-5' YEAR TO MONTH  | INTERVAL '0-0' YEAR TO MONTH  |
        | double   | true  | 0.4D               | INTERVAL '3-2' YEAR TO MONTH  | INTERVAL '-3-2' YEAR TO MONTH  | INTERVAL '0-3' YEAR TO MONTH  |
        | float    | true  | CAST(2 AS FLOAT)   | INTERVAL '0-8' YEAR TO MONTH  | INTERVAL '-0-8' YEAR TO MONTH  | INTERVAL '0-1' YEAR TO MONTH  |
        | decimal  | true  | 0.3                | INTERVAL '4-2' YEAR TO MONTH  | INTERVAL '-4-2' YEAR TO MONTH  | INTERVAL '0-3' YEAR TO MONTH  |
        | decimal  | false | 2.6                | INTERVAL '0-6' YEAR TO MONTH  | INTERVAL '-0-6' YEAR TO MONTH  | INTERVAL '0-0' YEAR TO MONTH  |
        | infinity | true  | CAST('Infinity' AS DOUBLE) | INTERVAL '0-0' YEAR TO MONTH | INTERVAL '0-0' YEAR TO MONTH | INTERVAL '0-0' YEAR TO MONTH |
        | string   | true  | '2'                | INTERVAL '0-8' YEAR TO MONTH  | INTERVAL '-0-8' YEAR TO MONTH  | INTERVAL '0-1' YEAR TO MONTH  |
        | NULL     | true  | CAST(NULL AS INT)  | NULL                          | NULL                           | NULL                          |

    Scenario: The quotient is a year-to-month interval
      When query
        """
        SELECT typeof(INTERVAL '1' YEAR / 2) AS t0, CAST(INTERVAL '2' YEAR / 3 AS STRING) AS r0
        """
      Then query result
        | t0                     | r0                           |
        | interval year to month | INTERVAL '0-8' YEAR TO MONTH |

    Scenario: A NULL interval divided by zero is NULL
      When query
        """
        SELECT id, CAST(ym / z AS STRING) AS result
        FROM VALUES
          (0, INTERVAL '1-3' YEAR TO MONTH, 2),
          (1, CAST(NULL AS INTERVAL YEAR TO MONTH), 0)
        AS t(id, ym, z)
        ORDER BY id
        """
      Then query result ordered
        | id | result                       |
        | 0  | INTERVAL '0-8' YEAR TO MONTH |
        | 1  | NULL                         |

  Rule: Day-time interval multiplied or divided by a numeric value

    Scenario Outline: Day-time interval <case> (ANSI <ansi>)
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(<expr> AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                     | ansi  | expr                                     | result                                    |
        | times double             | true  | INTERVAL '0.000001' SECOND * 1.5D        | INTERVAL '0 00:00:00.000002' DAY TO SECOND |
        | times double             | false | INTERVAL '0.000001' SECOND * 2.5D        | INTERVAL '0 00:00:00.000003' DAY TO SECOND |
        | times negative double    | true  | INTERVAL '0.000001' SECOND * -0.5D       | INTERVAL '-0 00:00:00.000001' DAY TO SECOND |
        | double times             | true  | 2.5D * INTERVAL '0.000001' SECOND        | INTERVAL '0 00:00:00.000003' DAY TO SECOND |
        | times decimal            | true  | INTERVAL '0.000001' SECOND * 0.5         | INTERVAL '0 00:00:00.000001' DAY TO SECOND |
        | times int                | true  | INTERVAL '1 02:00:00' DAY TO SECOND * 2  | INTERVAL '2 04:00:00' DAY TO SECOND       |
        | times hour double        | true  | INTERVAL '1' HOUR * 1.5D                 | INTERVAL '0 01:30:00' DAY TO SECOND       |
        | times string             | true  | INTERVAL '1' DAY * '1.5'                 | INTERVAL '1 12:00:00' DAY TO SECOND       |
        | divided by int           | true  | INTERVAL '1' DAY / 7                     | INTERVAL '0 03:25:42.857143' DAY TO SECOND |
        | divided by int           | false | INTERVAL '0.000003' SECOND / 2           | INTERVAL '0 00:00:00.000002' DAY TO SECOND |
        | divided by negative int  | true  | INTERVAL '0.000003' SECOND / -2          | INTERVAL '-0 00:00:00.000002' DAY TO SECOND |
        | divided by bigint        | true  | INTERVAL '10' MINUTE / 3L                | INTERVAL '0 00:03:20' DAY TO SECOND       |
        | divided by double        | true  | INTERVAL '1' DAY / 7.0D                  | INTERVAL '0 03:25:42.857143' DAY TO SECOND |
        | divided by small double  | true  | INTERVAL '0.000001' SECOND / 0.4D        | INTERVAL '0 00:00:00.000003' DAY TO SECOND |
        | divided by decimal       | true  | INTERVAL '0.000005' SECOND / 1.5         | INTERVAL '0 00:00:00.000003' DAY TO SECOND |
        | divided by decimal       | false | INTERVAL '1' DAY / 7.0                   | INTERVAL '0 03:25:42.857143' DAY TO SECOND |

    Scenario: The result is a day-to-second interval
      When query
        """
        SELECT typeof(INTERVAL '1' DAY * 1.5D) AS t0, typeof(INTERVAL '1' HOUR / 2) AS t1
        """
      Then query result
        | t0                     | t1                     |
        | interval day to second | interval day to second |

  Rule: Division by zero and overflow raise errors whether or not ANSI mode is enabled

    Scenario Outline: Interval division error: <case> (ANSI <ansi>)
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(<expr> AS STRING) AS result
        """
      Then query error <error>

      Examples:
        | case                    | ansi  | expr                                                                         | error                        |
        | ym by int zero          | true  | INTERVAL '1-3' YEAR TO MONTH / 0                                             | INTERVAL_DIVIDED_BY_ZERO     |
        | ym by int zero          | false | INTERVAL '1-3' YEAR TO MONTH / 0                                             | INTERVAL_DIVIDED_BY_ZERO     |
        | ym by decimal zero      | false | INTERVAL '1-3' YEAR TO MONTH / 0.0                                           | INTERVAL_DIVIDED_BY_ZERO     |
        | ym by negative zero     | true  | INTERVAL '1-3' YEAR TO MONTH / -0.0D                                         | INTERVAL_DIVIDED_BY_ZERO     |
        | dt by int zero          | false | INTERVAL '1' DAY / 0                                                         | INTERVAL_DIVIDED_BY_ZERO     |
        | dt by double zero       | true  | INTERVAL '1' DAY / 0.0D                                                      | INTERVAL_DIVIDED_BY_ZERO     |
        | dt by double zero       | false | INTERVAL '1' DAY / 0.0D                                                      | INTERVAL_DIVIDED_BY_ZERO     |
        | ym min by minus one     | true  | (INTERVAL '-178956970-7' YEAR TO MONTH - INTERVAL '0-1' YEAR TO MONTH) / -1   | INTERVAL_ARITHMETIC_OVERFLOW |
        | ym by NaN               | true  | INTERVAL '0-1' YEAR TO MONTH / CAST('NaN' AS DOUBLE)                         | infinite or NaN              |
        | ym by tiny double       | false | INTERVAL '0-1' YEAR TO MONTH / 1e-10D                                        | out of range                 |
        | dt times int            | true  | INTERVAL '106751991 04:00:54.775807' DAY TO SECOND * 2                       | long overflow                |
        | dt times int            | false | INTERVAL '106751991 04:00:54.775807' DAY TO SECOND * 2                       | long overflow                |
        | dt times double         | true  | INTERVAL '106751991 04:00:54.775807' DAY TO SECOND * 1.0D                    | out of range                 |

    Scenario: A column zero divisor raises an error for the rows that evaluate it
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(ym / z AS STRING) AS result
        FROM VALUES (INTERVAL '1-3' YEAR TO MONTH, 0) AS t(ym, z)
        """
      Then query error INTERVAL_DIVIDED_BY_ZERO
