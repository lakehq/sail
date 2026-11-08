Feature: DECIMAL values compared with DOUBLE constants

  Like Spark, a DECIMAL value is compared with a DOUBLE value as a DOUBLE value. The comparison
  with a DOUBLE constant gives the same result as casting every DECIMAL value to DOUBLE, including
  constants that are not exact DECIMAL values, NaN, infinite, or out of the range of the DECIMAL
  values.

  Rule: Comparisons with DOUBLE constants

    Scenario Outline: DECIMAL values <op> <constant>
      When query
        """
        SELECT
          id,
          d <op> <constant> AS r1,
          <constant> <op> d AS r2
        FROM VALUES
          (0, CAST(0.00 AS DECIMAL(6,2))),
          (1, CAST(-1.00 AS DECIMAL(6,2))),
          (2, CAST(NULL AS DECIMAL(6,2))),
          (3, CAST(0.10 AS DECIMAL(6,2))),
          (4, CAST(1000.50 AS DECIMAL(6,2))),
          (5, CAST(1000.51 AS DECIMAL(6,2))),
          (6, CAST(9999.99 AS DECIMAL(6,2))),
          (7, CAST(-9999.99 AS DECIMAL(6,2)))
        AS t(id, d)
        ORDER BY id
        """
      Then query result ordered
        | id | r1   | r2   |
        | 0  | <r0> | <s0> |
        | 1  | <r1> | <s1> |
        | 2  | NULL | NULL |
        | 3  | <r3> | <s3> |
        | 4  | <r4> | <s4> |
        | 5  | <r5> | <s5> |
        | 6  | <r6> | <s6> |
        | 7  | <r7> | <s7> |

      Examples:
        | op | constant                   | r0    | r1    | r3    | r4    | r5    | r6    | r7    | s0    | s1    | s3    | s4    | s5    | s6    | s7    |
        | <  | 1000.505D                  | true  | true  | true  | true  | false | false | true  | false | false | false | false | true  | true  | false |
        | <= | 1000.5D                    | true  | true  | true  | true  | false | false | true  | false | false | false | true  | true  | true  | false |
        | >  | 0.1D                       | false | false | false | true  | true  | true  | false | true  | true  | false | false | false | false | true  |
        | >= | 0.1D                       | false | false | true  | true  | true  | true  | false | true  | true  | true  | false | false | false | true  |
        | =  | 0.1D                       | false | false | true  | false | false | false | false | false | false | true  | false | false | false | false |
        | <> | 1000.51D                   | true  | true  | true  | true  | false | true  | true  | true  | true  | true  | true  | false | true  | true  |
        | =  | 0.0D                       | true  | false | false | false | false | false | false | true  | false | false | false | false | false | false |
        | <  | 1e30D                      | true  | true  | true  | true  | true  | true  | true  | false | false | false | false | false | false | false |
        | >  | -1e30D                     | true  | true  | true  | true  | true  | true  | true  | false | false | false | false | false | false | false |
        | <  | CAST('NaN' AS DOUBLE)      | true  | true  | true  | true  | true  | true  | true  | false | false | false | false | false | false | false |
        | =  | CAST('NaN' AS DOUBLE)      | false | false | false | false | false | false | false | false | false | false | false | false | false | false |
        | >= | CAST('-Infinity' AS DOUBLE) | true  | true  | true  | true  | true  | true  | true  | false | false | false | false | false | false | false |
        | <  | CAST('Infinity' AS DOUBLE)  | true  | true  | true  | true  | true  | true  | true  | false | false | false | false | false | false | false |
        | >  | -0.0D                      | false | false | true  | true  | true  | true  | false | false | true  | false | false | false | false | true  |
        | =  | -0.0D                      | true  | false | false | false | false | false | false | true  | false | false | false | false | false | false |
        | <= | -0.0D                      | true  | true  | false | false | false | false | true  | true  | false | true  | true  | true  | true  | false |

  Rule: Constants out of the range of the DECIMAL values

    Scenario: DOUBLE constants out of the range of DECIMAL(31,15) values
      When query
        """
        SELECT
          id,
          d > 1e30D AS gt,
          d < -1e30D AS lt,
          d = 1e30D AS eq,
          d <> 1e30D AS ne
        FROM VALUES
          (0, CAST(0 AS DECIMAL(31,15))),
          (1, CAST('9999999999999999.999999999999999' AS DECIMAL(31,15))),
          (2, CAST('-9999999999999999.999999999999999' AS DECIMAL(31,15))),
          (3, CAST(NULL AS DECIMAL(31,15)))
        AS t(id, d)
        ORDER BY id
        """
      Then query result ordered
        | id | gt    | lt    | eq    | ne   |
        | 0  | false | false | false | true |
        | 1  | false | false | false | true |
        | 2  | false | false | false | true |
        | 3  | NULL  | NULL  | NULL  | NULL |

    Scenario: DOUBLE constants in the range of DECIMAL(38,0) values
      When query
        """
        SELECT
          id,
          d > 1e30D AS gt,
          d < -1e30D AS lt,
          d = 1e30D AS eq,
          d <> 1e30D AS ne
        FROM VALUES
          (0, CAST(0 AS DECIMAL(38,0))),
          (1, CAST('1000000000000000000000000000000' AS DECIMAL(38,0))),
          (2, CAST('1000000000000000000000000000001' AS DECIMAL(38,0))),
          (3, CAST('-1000000000000000000000000000000' AS DECIMAL(38,0))),
          (4, CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))),
          (5, CAST(NULL AS DECIMAL(38,0)))
        AS t(id, d)
        ORDER BY id
        """
      Then query result ordered
        | id | gt    | lt    | eq    | ne    |
        | 0  | false | false | false | true  |
        | 1  | false | false | true  | false |
        | 2  | false | false | true  | false |
        | 3  | false | false | false | true  |
        | 4  | true  | false | false | true  |
        | 5  | NULL  | NULL  | NULL  | NULL  |
