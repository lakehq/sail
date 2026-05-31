@array_min_max
Feature: array_min and array_max functions

  Rule: Basic usage

    Scenario: array_min and array_max with integers
      When query
        """
        SELECT array_min(array(3, 1, 2)) AS min_val, array_max(array(3, 1, 2)) AS max_val
        """
      Then query result
        | min_val | max_val |
        | 1       | 3       |

    Scenario: array_min and array_max with strings
      When query
        """
        SELECT array_min(array('banana', 'apple', 'cherry')) AS min_val, array_max(array('banana', 'apple', 'cherry')) AS max_val
        """
      Then query result
        | min_val | max_val |
        | apple   | cherry  |

    Scenario: array_min and array_max with doubles
      When query
        """
        SELECT array_min(array(3.14, 1.5, 2.7)) AS min_val, array_max(array(3.14, 1.5, 2.7)) AS max_val
        """
      Then query result
        | min_val | max_val |
        | 1.50    | 3.14    |

    Scenario: array_min and array_max with single element
      When query
        """
        SELECT array_min(array(42)) AS min_val, array_max(array(42)) AS max_val
        """
      Then query result
        | min_val | max_val |
        | 42      | 42      |

    Scenario: array_min and array_max with repeated elements
      When query
        """
        SELECT array_min(array(5, 5, 5)) AS min_val, array_max(array(5, 5, 5)) AS max_val
        """
      Then query result
        | min_val | max_val |
        | 5       | 5       |

  Rule: Empty and NULL inputs

    Scenario: array_min and array_max with empty array
      When query
        """
        SELECT array_min(array()) AS min_val, array_max(array()) AS max_val
        """
      Then query result
        | min_val | max_val |
        | NULL    | NULL    |

    Scenario: array_min and array_max with NULL input
      When query
        """
        SELECT array_min(NULL) AS min_val, array_max(NULL) AS max_val
        """
      Then query result
        | min_val | max_val |
        | NULL    | NULL    |

    Scenario: array_min and array_max with all NULLs
      When query
        """
        SELECT array_min(array(CAST(NULL AS INT), CAST(NULL AS INT))) AS min_val, array_max(array(CAST(NULL AS INT), CAST(NULL AS INT))) AS max_val
        """
      Then query result
        | min_val | max_val |
        | NULL    | NULL    |

    Scenario: array_min and array_max with some NULLs mixed
      When query
        """
        SELECT array_min(array(3, NULL, 1, NULL, 2)) AS min_val, array_max(array(3, NULL, 1, NULL, 2)) AS max_val
        """
      Then query result
        | min_val | max_val |
        | 1       | 3       |

    Scenario: array_min and array_max with NULL at first position
      When query
        """
        SELECT array_min(array(NULL, 2, 3)) AS min_val, array_max(array(NULL, 2, 3)) AS max_val
        """
      Then query result
        | min_val | max_val |
        | 2       | 3       |

    Scenario: array_min and array_max with NULL at last position
      When query
        """
        SELECT array_min(array(1, 2, NULL)) AS min_val, array_max(array(1, 2, NULL)) AS max_val
        """
      Then query result
        | min_val | max_val |
        | 1       | 2       |

  Rule: Negative numbers

    Scenario: array_min and array_max with negative numbers
      When query
        """
        SELECT array_min(array(-5, -1, -10)) AS min_val, array_max(array(-5, -1, -10)) AS max_val
        """
      Then query result
        | min_val | max_val |
        | -10     | -1      |

    Scenario: array_min and array_max with mixed positive negative and zero
      When query
        """
        SELECT array_min(array(-3, 0, 3)) AS min_val, array_max(array(-3, 0, 3)) AS max_val
        """
      Then query result
        | min_val | max_val |
        | -3      | 3       |

  Rule: Float special values

    Scenario: array_min and array_max with NaN
      When query
        """
        SELECT array_min(array(1.0, CAST('NaN' AS DOUBLE), 2.0)) AS min_val, array_max(array(1.0, CAST('NaN' AS DOUBLE), 2.0)) AS max_val
        """
      Then query result
        | min_val | max_val |
        | 1.0     | NaN     |

    Scenario: array_min and array_max with NaN only
      When query
        """
        SELECT array_min(array(CAST('NaN' AS DOUBLE))) AS min_val, array_max(array(CAST('NaN' AS DOUBLE))) AS max_val
        """
      Then query result
        | min_val | max_val |
        | NaN     | NaN     |

    Scenario: array_min and array_max with NaN and NULL
      When query
        """
        SELECT array_min(array(CAST('NaN' AS DOUBLE), NULL, 1.0)) AS min_val, array_max(array(CAST('NaN' AS DOUBLE), NULL, 1.0)) AS max_val
        """
      Then query result
        | min_val | max_val |
        | 1.0     | NaN     |

    Scenario: array_min and array_max with NaN vs Infinity
      When query
        """
        SELECT array_min(array(CAST('NaN' AS DOUBLE), CAST('Infinity' AS DOUBLE))) AS min_val, array_max(array(CAST('NaN' AS DOUBLE), CAST('Infinity' AS DOUBLE))) AS max_val
        """
      Then query result
        | min_val  | max_val |
        | Infinity | NaN     |

    Scenario: array_min and array_max with NaN vs negative Infinity
      When query
        """
        SELECT array_min(array(CAST('-Infinity' AS DOUBLE), CAST('NaN' AS DOUBLE))) AS min_val, array_max(array(CAST('-Infinity' AS DOUBLE), CAST('NaN' AS DOUBLE))) AS max_val
        """
      Then query result
        | min_val   | max_val |
        | -Infinity | NaN     |

    Scenario: array_min and array_max with positive Infinity
      When query
        """
        SELECT array_min(array(1.0, CAST('Infinity' AS DOUBLE))) AS min_val, array_max(array(1.0, CAST('Infinity' AS DOUBLE))) AS max_val
        """
      Then query result
        | min_val | max_val  |
        | 1.0     | Infinity |

    Scenario: array_min and array_max with negative Infinity
      When query
        """
        SELECT array_min(array(1.0, CAST('-Infinity' AS DOUBLE))) AS min_val, array_max(array(1.0, CAST('-Infinity' AS DOUBLE))) AS max_val
        """
      Then query result
        | min_val   | max_val |
        | -Infinity | 1.0     |

    Scenario: array_min and array_max with both Infinities
      When query
        """
        SELECT array_min(array(CAST('-Infinity' AS DOUBLE), CAST('Infinity' AS DOUBLE))) AS min_val, array_max(array(CAST('-Infinity' AS DOUBLE), CAST('Infinity' AS DOUBLE))) AS max_val
        """
      Then query result
        | min_val   | max_val  |
        | -Infinity | Infinity |

    Scenario: array_min and array_max with float NaN
      When query
        """
        SELECT array_min(array(CAST('NaN' AS FLOAT), CAST(1.0 AS FLOAT))) AS min_val, array_max(array(CAST('NaN' AS FLOAT), CAST(1.0 AS FLOAT))) AS max_val
        """
      Then query result
        | min_val | max_val |
        | 1.0     | NaN     |

    Scenario: array_min and array_max with positive and negative zero
      When query
        """
        SELECT array_min(array(CAST(0.0 AS DOUBLE), CAST(-0.0 AS DOUBLE))) AS min_val, array_max(array(CAST(0.0 AS DOUBLE), CAST(-0.0 AS DOUBLE))) AS max_val
        """
      Then query result
        | min_val | max_val |
        | 0.0     | 0.0     |

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

    Scenario: array_min and array_max with case sensitive strings
      When query
        """
        SELECT array_min(array('Z', 'a', 'A')) AS min_val, array_max(array('Z', 'a', 'A')) AS max_val
        """
      Then query result
        | min_val | max_val |
        | A       | a       |

    Scenario: array_min and array_max with mixed case and numeric strings
      When query
        """
        SELECT array_min(array('abc', 'ABc', '123')) AS min_val, array_max(array('abc', 'ABc', '123')) AS max_val
        """
      Then query result
        | min_val | max_val |
        | 123     | abc     |

  Rule: Date and timestamp arrays

    Scenario: array_min and array_max with dates
      When query
        """
        SELECT array_min(array(DATE '2023-01-01', DATE '2023-12-31', DATE '2023-06-15')) AS min_val, array_max(array(DATE '2023-01-01', DATE '2023-12-31', DATE '2023-06-15')) AS max_val
        """
      Then query result
        | min_val    | max_val    |
        | 2023-01-01 | 2023-12-31 |

    Scenario: array_min and array_max with timestamps
      When query
        """
        SELECT array_min(array(TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-12-31 23:59:59', TIMESTAMP '2023-06-15 12:00:00')) AS min_val, array_max(array(TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-12-31 23:59:59', TIMESTAMP '2023-06-15 12:00:00')) AS max_val
        """
      Then query result
        | min_val             | max_val             |
        | 2023-01-01 00:00:00 | 2023-12-31 23:59:59 |

    Scenario: array_min and array_max with dates and NULL
      When query
        """
        SELECT array_min(array(DATE '2023-01-01', NULL, DATE '2023-12-31')) AS min_val, array_max(array(DATE '2023-01-01', NULL, DATE '2023-12-31')) AS max_val
        """
      Then query result
        | min_val    | max_val    |
        | 2023-01-01 | 2023-12-31 |

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

    Scenario: array_min and array_max with BIGINT extremes
      When query
        """
        SELECT array_min(array(CAST(9223372036854775807 AS BIGINT), CAST(-9223372036854775808 AS BIGINT), CAST(0 AS BIGINT))) AS min_val, array_max(array(CAST(9223372036854775807 AS BIGINT), CAST(-9223372036854775808 AS BIGINT), CAST(0 AS BIGINT))) AS max_val
        """
      Then query result
        | min_val              | max_val             |
        | -9223372036854775808 | 9223372036854775807 |

    Scenario: array_min and array_max with SMALLINT extremes
      When query
        """
        SELECT array_min(array(CAST(1 AS SMALLINT), CAST(-32768 AS SMALLINT), CAST(32767 AS SMALLINT))) AS min_val, array_max(array(CAST(1 AS SMALLINT), CAST(-32768 AS SMALLINT), CAST(32767 AS SMALLINT))) AS max_val
        """
      Then query result
        | min_val | max_val |
        | -32768  | 32767   |

    Scenario: array_min and array_max with TINYINT extremes
      When query
        """
        SELECT array_min(array(CAST(1 AS TINYINT), CAST(-128 AS TINYINT), CAST(127 AS TINYINT))) AS min_val, array_max(array(CAST(1 AS TINYINT), CAST(-128 AS TINYINT), CAST(127 AS TINYINT))) AS max_val
        """
      Then query result
        | min_val | max_val |
        | -128    | 127     |

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

    Scenario: array_min with nested arrays
      When query
        """
        SELECT array_min(array(array(1,2), array(3,4))) AS min_val
        """
      Then query result
        | min_val |
        | [1, 2]  |

    Scenario: array_max with nested arrays
      When query
        """
        SELECT array_max(array(array(1,2), array(3,4))) AS max_val
        """
      Then query result
        | max_val |
        | [3, 4]  |

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
