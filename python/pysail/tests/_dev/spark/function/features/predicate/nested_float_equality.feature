Feature: Equality of nested values with floating-point fields

  # Spark compares floats with `-0.0` equal to `0.0` (SQLOrderingUtil), including floats nested
  # in arrays and structs, for `=`, `<=>`, `!=` and IN.
  Rule: -0.0 and 0.0 are equal in nested values

    Scenario: comparison operators on structs and arrays with -0.0 and 0.0
      When query
        """
        SELECT
          named_struct('a', x) = named_struct('a', y) AS eq,
          named_struct('a', x) <=> named_struct('a', y) AS null_safe_eq,
          named_struct('a', x) != named_struct('a', y) AS ne,
          array(x) = array(y) AS array_eq,
          array(x) IN (array(y)) AS array_in,
          array(x) NOT IN (array(y), array(2.0D)) AS array_not_in
        FROM VALUES (-0.0D, 0.0D) AS t(x, y)
        """
      Then query result
        | eq   | null_safe_eq | ne    | array_eq | array_in | array_not_in |
        | true | true         | false | true     | true     | false        |

    Scenario: deeply nested values with -0.0 and 0.0
      When query
        """
        SELECT
          array(array(x)) = array(array(y)) AS a,
          array(named_struct('f', x)) = array(named_struct('f', y)) AS b,
          named_struct('f', array(x)) <=> named_struct('f', array(y)) AS c
        FROM VALUES (-0.0D, 0.0D) AS t(x, y)
        """
      Then query result
        | a    | b    | c    |
        | true | true | true |

    Scenario: arrays of FLOAT values with -0.0 and 0.0
      When query
        """
        SELECT array(x) = array(y) AS a FROM VALUES (-0.0F, 0.0F), (0.0F, -0.0F), (1.0F, 2.0F) AS t(x, y)
        """
      Then query result ordered
        | a     |
        | true  |
        | true  |
        | false |

    Scenario: filters on nested values with -0.0 and 0.0
      When query
        """
        SELECT
          (SELECT count(*) FROM VALUES (-0.0D), (0.0D), (1.0D) AS t(x) WHERE array(x) = array(0.0D)) AS eq_count,
          (SELECT count(*) FROM VALUES (-0.0D), (1.0D) AS t(x) WHERE array(x) != array(0.0D)) AS ne_count,
          (SELECT count(*) FROM VALUES (-0.0D), (1.0D), (0.0D) AS t(x) WHERE named_struct('a', x) IN (named_struct('a', 0.0D))) AS in_count
        """
      Then query result
        | eq_count | ne_count | in_count |
        | 2        | 1        | 2        |

    Scenario: structs with NaN fields are equal
      When query
        """
        SELECT named_struct('a', x, 'b', 1) = named_struct('a', y, 'b', 1) AS a
        FROM VALUES (CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE)) AS t(x, y)
        """
      Then query result
        | a    |
        | true |

  # IN lists compare floating-point values with -0.0 equal to 0.0 as well.
  Rule: -0.0 and 0.0 are equal in IN lists

    Scenario: IN lists of columns with -0.0 and 0.0
      When query
        """
        SELECT id, CAST(d AS STRING) AS ds, de IN (d, f) AS a, de NOT IN (d, f) AS b, d IN (de) AS c
        FROM VALUES
          (1, CAST('0' AS DECIMAL(1,0)), CAST('-0.0' AS DOUBLE), CAST('-0.0' AS FLOAT)),
          (2, CAST('0' AS DECIMAL(1,0)), CAST('0.0' AS DOUBLE), CAST('0.0' AS FLOAT)),
          (3, CAST('1' AS DECIMAL(1,0)), CAST('2.5' AS DOUBLE), CAST('NaN' AS FLOAT))
        AS t(id, de, d, f)
        ORDER BY id
        """
      Then query result ordered
        | id | ds   | a     | b     | c     |
        | 1  | -0.0 | true  | false | true  |
        | 2  | 0.0  | true  | false | true  |
        | 3  | 2.5  | false | true  | false |

    Scenario: IN lists of literals with -0.0 and 0.0
      When query
        """
        SELECT id,
          d IN (0.0D, 7.0D) AS a,
          d IN (-0.0D) AS b,
          d NOT IN (0.0D, 7.0D) AS c,
          f IN (CAST(0 AS FLOAT)) AS e,
          0.0D IN (d, 1.0D) AS g
        FROM VALUES
          (1, CAST('-0.0' AS DOUBLE), CAST('-0.0' AS FLOAT)),
          (2, CAST('0.0' AS DOUBLE), CAST('0.0' AS FLOAT)),
          (3, CAST('2.5' AS DOUBLE), CAST('NaN' AS FLOAT))
        AS t(id, d, f)
        ORDER BY id
        """
      Then query result ordered
        | id | a     | b     | c     | e     | g     |
        | 1  | true  | true  | false | true  | true  |
        | 2  | true  | true  | false | true  | true  |
        | 3  | false | false | true  | false | false |

    Scenario: long IN lists of literals with -0.0 and 0.0
      When query
        """
        SELECT id,
          d IN (0.0D, 1.0D, 2.0D, 3.0D, 4.0D, 5.0D, 6.0D, 7.0D, 8.0D, 9.0D, 10.0D, 11.0D, 12.0D, 13.0D, 14.0D, 15.0D, 16.0D, 17.0D, 18.0D, 19.0D, 20.0D) AS a,
          d IN (-0.0D, 1.0D, 2.0D, 3.0D, 4.0D, 5.0D, 6.0D, 7.0D, 8.0D, 9.0D, 10.0D, 11.0D, 12.0D, 13.0D, 14.0D, 15.0D, 16.0D, 17.0D, 18.0D, 19.0D, 20.0D) AS b,
          d NOT IN (0.0D, 1.0D, 2.0D, 3.0D, 4.0D, 5.0D, 6.0D, 7.0D, 8.0D, 9.0D, 10.0D, 11.0D, 12.0D, 13.0D, 14.0D, 15.0D, 16.0D, 17.0D, 18.0D, 19.0D, 20.0D) AS c
        FROM VALUES (1, CAST('-0.0' AS DOUBLE)), (2, CAST('0.0' AS DOUBLE)), (3, CAST('2.5' AS DOUBLE)) AS t(id, d)
        ORDER BY id
        """
      Then query result ordered
        | id | a     | b     | c     |
        | 1  | true  | true  | false |
        | 2  | true  | true  | false |
        | 3  | false | false | true  |

    Scenario: IN lists of decimal and floating-point values with -0.0
      When query
        """
        SELECT id, de IN (d, f) AS c0
        FROM VALUES
          (5, CAST('0' AS DECIMAL(10,2)), CAST('-0.0' AS DOUBLE), CAST('-0.0' AS FLOAT)),
          (6, CAST('0' AS DECIMAL(10,2)), CAST('1' AS DOUBLE), CAST('0.0' AS FLOAT)),
          (7, CAST('0' AS DECIMAL(10,2)), CAST('-0.0' AS DOUBLE), CAST('1' AS FLOAT))
        AS t(id, de, d, f)
        ORDER BY id
        """
      Then query result ordered
        | id | c0   |
        | 5  | true |
        | 6  | true |
        | 7  | true |

    Scenario: IN lists of literals compared with a literal -0.0
      When query
        """
        SELECT CAST(0 AS DECIMAL(1,0)) IN (-0.0D, CAST('-0.0' AS FLOAT)) AS a, 0.0D IN (-0.0D) AS b
        """
      Then query result
        | a    | b    |
        | true | true |
