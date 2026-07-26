@array_min
Feature: array_min and array_max functions

  Rule: Basic usage

    Scenario Outline: Basic: <case>
      When query
        """
        SELECT array_min(<arr>) AS min_val, array_max(<arr>) AS max_val
        """
      Then query result
        | min_val | max_val |
        | <min>   | <max>   |

      Examples:
        | case                                           | arr                                | min   | max    |
        | array_min and array_max with integers          | array(3, 1, 2)                     | 1     | 3      |
        | array_min and array_max with strings           | array('banana', 'apple', 'cherry') | apple | cherry |
        | array_min and array_max with doubles           | array(3.14, 1.5, 2.7)              | 1.50  | 3.14   |
        | array_min and array_max with single element    | array(42)                          | 42    | 42     |
        | array_min and array_max with repeated elements | array(5, 5, 5)                     | 5     | 5      |

  Rule: Empty and NULL inputs

    Scenario Outline: Empty and NULL: <case>
      When query
        """
        SELECT array_min(<arr>) AS min_val, array_max(<arr>) AS max_val
        """
      Then query result
        | min_val | max_val |
        | <min>   | <max>   |

      Examples:
        | case                                                | arr                                         | min  | max  |
        | array_min and array_max with empty array            | array()                                     | NULL | NULL |
        | array_min and array_max with NULL input             | NULL                                        | NULL | NULL |
        | array_min and array_max with all NULLs              | array(CAST(NULL AS INT), CAST(NULL AS INT)) | NULL | NULL |
        | array_min and array_max with some NULLs mixed       | array(3, NULL, 1, NULL, 2)                  | 1    | 3    |
        | array_min and array_max with NULL at first position | array(NULL, 2, 3)                           | 2    | 3    |
        | array_min and array_max with NULL at last position  | array(1, 2, NULL)                           | 1    | 2    |

  Rule: Negative numbers

    Scenario Outline: Negative: <case>
      When query
        """
        SELECT array_min(<arr>) AS min_val, array_max(<arr>) AS max_val
        """
      Then query result
        | min_val | max_val |
        | <min>   | <max>   |

      Examples:
        | case                                                          | arr                | min | max |
        | array_min and array_max with negative numbers                 | array(-5, -1, -10) | -10 | -1  |
        | array_min and array_max with mixed positive negative and zero | array(-3, 0, 3)    | -3  | 3   |

  Rule: Float special values

    Scenario Outline: Float special: <case>
      When query
        """
        SELECT array_min(<arr>) AS min_val, array_max(<arr>) AS max_val
        """
      Then query result
        | min_val | max_val |
        | <min>   | <max>   |

      Examples:
        | case                                                    | arr                                                            | min       | max      |
        | array_min and array_max with NaN                        | array(1.0, CAST('NaN' AS DOUBLE), 2.0)                         | 1.0       | NaN      |
        | array_min and array_max with NaN only                   | array(CAST('NaN' AS DOUBLE))                                   | NaN       | NaN      |
        | array_min and array_max with NaN and NULL               | array(CAST('NaN' AS DOUBLE), NULL, 1.0)                        | 1.0       | NaN      |
        | array_min and array_max with NaN vs Infinity            | array(CAST('NaN' AS DOUBLE), CAST('Infinity' AS DOUBLE))       | Infinity  | NaN      |
        | array_min and array_max with NaN vs negative Infinity   | array(CAST('-Infinity' AS DOUBLE), CAST('NaN' AS DOUBLE))      | -Infinity | NaN      |
        | array_min and array_max with positive Infinity          | array(1.0, CAST('Infinity' AS DOUBLE))                         | 1.0       | Infinity |
        | array_min and array_max with negative Infinity          | array(1.0, CAST('-Infinity' AS DOUBLE))                        | -Infinity | 1.0      |
        | array_min and array_max with both Infinities            | array(CAST('-Infinity' AS DOUBLE), CAST('Infinity' AS DOUBLE)) | -Infinity | Infinity |
        | array_min and array_max with float NaN                  | array(CAST('NaN' AS FLOAT), CAST(1.0 AS FLOAT))                | 1.0       | NaN      |
        | array_min and array_max with positive and negative zero | array(CAST(0.0 AS DOUBLE), CAST(-0.0 AS DOUBLE))               | 0.0       | 0.0      |

    @sail-only
    Scenario: array_min and array_max with extreme double values (display format differs from Spark)
      When query
        """
        SELECT array_min(array(1.7976931348623157E308, -1.7976931348623157E308, 0.0)) AS min_val, array_max(array(1.7976931348623157E308, -1.7976931348623157E308, 0.0)) AS max_val
        """
      Then query result
        | min_val                 | max_val                |
        | -1.7976931348623157e308 | 1.7976931348623157e308 |

  Rule: Boolean arrays

    Scenario: array_min and array_max with booleans
      When query
        """
        SELECT array_min(array(true, false)) AS min_val, array_max(array(true, false)) AS max_val
        """
      Then query result
        | min_val | max_val |
        | false   | true    |

  Rule: String edge cases

    Scenario: array_min and array_max with empty string in array
      When query
        """
        SELECT array_min(array('', 'a', 'b')) AS min_val, array_max(array('', 'a', 'b')) AS max_val
        """
      Then query result
        | min_val | max_val |
        |         | b       |

    Scenario Outline: String: <case>
      When query
        """
        SELECT array_min(<arr>) AS min_val, array_max(<arr>) AS max_val
        """
      Then query result
        | min_val | max_val |
        | <min>   | <max>   |

      Examples:
        | case                                                        | arr                        | min | max |
        | array_min and array_max with case sensitive strings         | array('Z', 'a', 'A')       | A   | a   |
        | array_min and array_max with mixed case and numeric strings | array('abc', 'ABc', '123') | 123 | abc |

  Rule: Date and timestamp arrays

    Scenario Outline: Date/timestamp: <case>
      When query
        """
        SELECT array_min(<arr>) AS min_val, array_max(<arr>) AS max_val
        """
      Then query result
        | min_val | max_val |
        | <min>   | <max>   |

      Examples:
        | case                                        | arr                                                                                                      | min                 | max                 |
        | array_min and array_max with dates          | array(DATE '2023-01-01', DATE '2023-12-31', DATE '2023-06-15')                                           | 2023-01-01          | 2023-12-31          |
        | array_min and array_max with timestamps     | array(TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-12-31 23:59:59', TIMESTAMP '2023-06-15 12:00:00') | 2023-01-01 00:00:00 | 2023-12-31 23:59:59 |
        | array_min and array_max with dates and NULL | array(DATE '2023-01-01', NULL, DATE '2023-12-31')                                                        | 2023-01-01          | 2023-12-31          |

  Rule: Decimal arrays

    Scenario: array_min and array_max with decimals
      When query
        """
        SELECT array_min(array(CAST(1.11 AS DECIMAL(10,2)), CAST(2.22 AS DECIMAL(10,2)), CAST(0.99 AS DECIMAL(10,2)))) AS min_val, array_max(array(CAST(1.11 AS DECIMAL(10,2)), CAST(2.22 AS DECIMAL(10,2)), CAST(0.99 AS DECIMAL(10,2)))) AS max_val
        """
      Then query result
        | min_val | max_val |
        | 0.99    | 2.22    |

    Scenario: array_min with equal decimal values of different precision
      When query
        """
        SELECT array_min(array(CAST(0.1 AS DECIMAL(38,18)), CAST(0.10 AS DECIMAL(38,18)), CAST(0.100 AS DECIMAL(38,18)))) AS min_val
        """
      Then query result
        | min_val              |
        | 0.100000000000000000 |

  Rule: BIGINT boundary values

    Scenario Outline: Integer extremes: <case>
      When query
        """
        SELECT array_min(<arr>) AS min_val, array_max(<arr>) AS max_val
        """
      Then query result
        | min_val | max_val |
        | <min>   | <max>   |

      Examples:
        | case                                           | arr                                                                                                 | min                  | max                 |
        | array_min and array_max with BIGINT extremes   | array(CAST(9223372036854775807 AS BIGINT), CAST(-9223372036854775808 AS BIGINT), CAST(0 AS BIGINT)) | -9223372036854775808 | 9223372036854775807 |
        | array_min and array_max with SMALLINT extremes | array(CAST(1 AS SMALLINT), CAST(-32768 AS SMALLINT), CAST(32767 AS SMALLINT))                       | -32768               | 32767               |
        | array_min and array_max with TINYINT extremes  | array(CAST(1 AS TINYINT), CAST(-128 AS TINYINT), CAST(127 AS TINYINT))                              | -128                 | 127                 |

  Rule: Large arrays

    Scenario: array_min and array_max with large sequence
      When query
        """
        SELECT array_min(sequence(1, 1000)) AS min_val, array_max(sequence(1, 1000)) AS max_val
        """
      Then query result
        | min_val | max_val |
        | 1       | 1000    |

  Rule: Nested arrays

    Scenario Outline: <fn> with nested arrays
      When query
        """
        SELECT <fn>(array(array(1,2), array(3,4))) AS <alias>
        """
      Then query result
        | <alias>  |
        | <result> |

      Examples:
        | fn        | alias   | result |
        | array_min | min_val | [1, 2] |
        | array_max | max_val | [3, 4] |

  Rule: Multi-row results

    Scenario: array_min and array_max across multiple rows
      When query
        """
        SELECT id, array_min(arr) AS min_val, array_max(arr) AS max_val
        FROM VALUES (1, array(10, 20, 30)), (2, array(5, 15, 25)), (3, array(100)) AS t(id, arr)
        ORDER BY id
        """
      Then query result ordered
        | id | min_val | max_val |
        | 1  | 10      | 30      |
        | 2  | 5       | 25      |
        | 3  | 100     | 100     |

  @spark_null
  Rule: Output schema

    Scenario: array_min of a non-null array literal
      When query
        """
        SELECT array_min(array(3, 1, 2)) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: array_min of a non-null array column
      When query
        """
        SELECT array_min(array(id, id)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: array_min of a nullable array column stays nullable
      When query
        """
        SELECT array_min(c) AS result FROM VALUES (array(1, 2)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: array_max of a non-null array literal
      When query
        """
        SELECT array_max(array(3, 1, 2)) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: array_max of a non-null array column
      When query
        """
        SELECT array_max(array(id, id)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: array_max of a nullable array column stays nullable
      When query
        """
        SELECT array_max(c) AS result FROM VALUES (array(1, 2)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
