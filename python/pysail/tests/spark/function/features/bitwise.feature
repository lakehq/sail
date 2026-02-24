Feature: Bitwise functions

  Rule: bit_count

    Scenario: bit_count with positive integers
      When query
        """
        SELECT bit_count(0) AS r0, bit_count(1) AS r1, bit_count(7) AS r2, bit_count(255) AS r3
        """
      Then query result
        | r0 | r1 | r2 | r3 |
        | 0  | 1  | 3  | 8  |

    Scenario: bit_count with negative integers
      When query
        """
        SELECT bit_count(-1) AS r1, bit_count(-2) AS r2, bit_count(-128) AS r3
        """
      Then query result
        | r1 | r2 | r3 |
        | 64 | 63 | 57 |

    Scenario: bit_count with bigint
      When query
        """
        SELECT bit_count(CAST(0 AS BIGINT)) AS r0, bit_count(CAST(9223372036854775807 AS BIGINT)) AS r1
        """
      Then query result
        | r0 | r1 |
        | 0  | 63 |

    Scenario: bit_count with NULL
      When query
        """
        SELECT bit_count(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: bit_count with smallint and tinyint
      When query
        """
        SELECT bit_count(CAST(127 AS TINYINT)) AS r1, bit_count(CAST(-1 AS SMALLINT)) AS r2
        """
      Then query result
        | r1 | r2 |
        | 7  | 64 |

  Rule: Bitwise NOT (~)

    Scenario: tilde operator basic
      When query
        """
        SELECT ~0 AS r0, ~1 AS r1, ~(-1) AS r2
        """
      Then query result
        | r0 | r1 | r2 |
        | -1 | -2 | 0  |

    Scenario: tilde operator with bigint extremes
      When query
        """
        SELECT ~CAST(9223372036854775807 AS BIGINT) AS r1, ~CAST(-9223372036854775808 AS BIGINT) AS r2
        """
      Then query result
        | r1                    | r2                   |
        | -9223372036854775808  | 9223372036854775807  |

    Scenario: tilde operator with NULL
      When query
        """
        SELECT ~CAST(NULL AS INT) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: bit_get / getbit

    Scenario: bit_get basic
      When query
        """
        SELECT bit_get(11, 0) AS b0, bit_get(11, 1) AS b1, bit_get(11, 2) AS b2, bit_get(11, 3) AS b3, bit_get(11, 4) AS b4
        """
      Then query result
        | b0 | b1 | b2 | b3 | b4 |
        | 1  | 1  | 0  | 1  | 0  |

    Scenario: getbit is alias for bit_get
      When query
        """
        SELECT getbit(11, 0) AS b0, getbit(11, 3) AS b3
        """
      Then query result
        | b0 | b3 |
        | 1  | 1  |

    Scenario: bit_get with bigint high bits
      When query
        """
        SELECT bit_get(CAST(9223372036854775807 AS BIGINT), 62) AS r1, bit_get(CAST(9223372036854775807 AS BIGINT), 63) AS r2
        """
      Then query result
        | r1 | r2 |
        | 1  | 0  |

    Scenario: bit_get with negative int
      When query
        """
        SELECT bit_get(-1, 0) AS r0, bit_get(-1, 31) AS r31
        """
      Then query result
        | r0 | r31 |
        | 1  | 1   |

    Scenario: bit_get with negative bigint
      When query
        """
        SELECT bit_get(CAST(-1 AS BIGINT), 0) AS r0, bit_get(CAST(-1 AS BIGINT), 63) AS r63
        """
      Then query result
        | r0 | r63 |
        | 1  | 1   |

    Scenario: bit_get with NULL
      When query
        """
        SELECT bit_get(NULL, 0) AS r1, bit_get(11, NULL) AS r2
        """
      Then query result
        | r1   | r2   |
        | NULL | NULL |

    Scenario: bit_get with invalid negative position
      When query
        """
        SELECT bit_get(11, -1) AS result
        """
      # Spark: INVALID_PARAMETER_VALUE.BIT_POSITION_RANGE, Sail: Compute error (different message, same semantics)
      Then query error bit_get

    Scenario: bit_get position beyond int bit width
      When query
        """
        SELECT bit_get(11, 32) AS result
        """
      # Spark: INVALID_PARAMETER_VALUE.BIT_POSITION_RANGE, Sail: Compute error (different message, same semantics)
      Then query error bit_get

    Scenario: bit_get position beyond bigint bit width
      When query
        """
        SELECT bit_get(CAST(11 AS BIGINT), 64) AS result
        """
      # Spark: INVALID_PARAMETER_VALUE.BIT_POSITION_RANGE, Sail: Compute error (different message, same semantics)
      Then query error bit_get

  Rule: Bitwise AND (&)

    Scenario: bitwise AND basic
      When query
        """
        SELECT 12 & 10 AS result
        """
      Then query result
        | result |
        | 8      |

    Scenario: bitwise AND with zero
      When query
        """
        SELECT 255 & 0 AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: bitwise AND with negative
      When query
        """
        SELECT -1 & 15 AS result
        """
      Then query result
        | result |
        | 15     |

    Scenario: bitwise AND with NULL
      When query
        """
        SELECT 12 & CAST(NULL AS INT) AS r1, CAST(NULL AS INT) & 10 AS r2
        """
      Then query result
        | r1   | r2   |
        | NULL | NULL |

    Scenario: bitwise AND with bigint
      When query
        """
        SELECT CAST(9223372036854775807 AS BIGINT) & CAST(1 AS BIGINT) AS result
        """
      Then query result
        | result |
        | 1      |

  Rule: Bitwise OR (|)

    Scenario: bitwise OR basic
      When query
        """
        SELECT 12 | 10 AS result
        """
      Then query result
        | result |
        | 14     |

    Scenario: bitwise OR with zero
      When query
        """
        SELECT 255 | 0 AS result
        """
      Then query result
        | result |
        | 255    |

    Scenario: bitwise OR with NULL
      When query
        """
        SELECT 12 | CAST(NULL AS INT) AS r1, CAST(NULL AS INT) | 10 AS r2
        """
      Then query result
        | r1   | r2   |
        | NULL | NULL |

  Rule: Bitwise XOR (^)

    Scenario: bitwise XOR basic
      When query
        """
        SELECT 12 ^ 10 AS result
        """
      Then query result
        | result |
        | 6      |

    Scenario: bitwise XOR with same value
      When query
        """
        SELECT 42 ^ 42 AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: bitwise XOR with NULL
      When query
        """
        SELECT 12 ^ CAST(NULL AS INT) AS r1, CAST(NULL AS INT) ^ 10 AS r2
        """
      Then query result
        | r1   | r2   |
        | NULL | NULL |

  Rule: shiftleft

    Scenario: shiftleft basic
      When query
        """
        SELECT shiftleft(1, 0) AS r0, shiftleft(1, 1) AS r1, shiftleft(1, 10) AS r10
        """
      Then query result
        | r0 | r1 | r10  |
        | 1  | 2  | 1024 |

    Scenario: shiftleft overflow int
      When query
        """
        SELECT shiftleft(1, 31) AS result
        """
      Then query result
        | result      |
        | -2147483648 |

    Scenario: shiftleft wraps on 32 for int (Java modulo behavior)
      When query
        """
        SELECT shiftleft(1, 32) AS r32, shiftleft(1, 33) AS r33
        """
      Then query result
        | r32 | r33 |
        | 1   | 2   |

    Scenario: shiftleft with negative value
      When query
        """
        SELECT shiftleft(-1, 1) AS result
        """
      Then query result
        | result |
        | -2     |

    Scenario: shiftleft with NULL
      When query
        """
        SELECT shiftleft(NULL, 1) AS r1, shiftleft(1, NULL) AS r2
        """
      Then query result
        | r1   | r2   |
        | NULL | NULL |

    Scenario: shiftleft bigint overflow
      When query
        """
        SELECT shiftleft(CAST(1 AS BIGINT), 63) AS result
        """
      Then query result
        | result               |
        | -9223372036854775808 |

    Scenario: shiftleft bigint wraps on 64 (Java modulo behavior)
      When query
        """
        SELECT shiftleft(CAST(1 AS BIGINT), 64) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: shiftleft negative shift
      When query
        """
        SELECT shiftleft(16, -1) AS result
        """
      Then query result
        | result |
        | 0      |

  Rule: shiftright

    Scenario: shiftright basic
      When query
        """
        SELECT shiftright(16, 1) AS r1, shiftright(16, 4) AS r4, shiftright(16, 5) AS r5
        """
      Then query result
        | r1 | r4 | r5 |
        | 8  | 1  | 0  |

    Scenario: shiftright with negative (arithmetic shift)
      When query
        """
        SELECT shiftright(-1, 1) AS result
        """
      Then query result
        | result |
        | -1     |

    Scenario: shiftright negative value
      When query
        """
        SELECT shiftright(-16, 2) AS result
        """
      Then query result
        | result |
        | -4     |

    Scenario: shiftright with NULL
      When query
        """
        SELECT shiftright(NULL, 1) AS r1, shiftright(16, NULL) AS r2
        """
      Then query result
        | r1   | r2   |
        | NULL | NULL |

    Scenario: shiftright wraps on 32 for int (Java modulo behavior)
      When query
        """
        SELECT shiftright(1, 32) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: shiftright negative shift
      When query
        """
        SELECT shiftright(16, -1) AS result
        """
      Then query result
        | result |
        | 0      |

  Rule: shiftrightunsigned

    Scenario: shiftrightunsigned basic
      When query
        """
        SELECT shiftrightunsigned(16, 1) AS r1, shiftrightunsigned(16, 4) AS r4
        """
      Then query result
        | r1 | r4 |
        | 8  | 1  |

    Scenario: shiftrightunsigned with negative int (logical shift)
      When query
        """
        SELECT shiftrightunsigned(-1, 1) AS result
        """
      Then query result
        | result     |
        | 2147483647 |

    Scenario: shiftrightunsigned negative value
      When query
        """
        SELECT shiftrightunsigned(-16, 2) AS result
        """
      Then query result
        | result     |
        | 1073741820 |

    Scenario: shiftrightunsigned with bigint negative
      When query
        """
        SELECT shiftrightunsigned(CAST(-1 AS BIGINT), 1) AS result
        """
      Then query result
        | result              |
        | 9223372036854775807 |

    Scenario: shiftrightunsigned with NULL
      When query
        """
        SELECT shiftrightunsigned(NULL, 1) AS r1, shiftrightunsigned(16, NULL) AS r2
        """
      Then query result
        | r1   | r2   |
        | NULL | NULL |

    Scenario: shiftrightunsigned wraps on 32 for int (Java modulo behavior)
      When query
        """
        SELECT shiftrightunsigned(1, 32) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: shiftrightunsigned zero
      When query
        """
        SELECT shiftrightunsigned(0, 5) AS result
        """
      Then query result
        | result |
        | 0      |

  Rule: Mixed types and edge cases

    Scenario: bitwise operations with boolean cast
      When query
        """
        SELECT CAST(true AS INT) & 1 AS r1, CAST(false AS INT) | 1 AS r2
        """
      Then query result
        | r1 | r2 |
        | 1  | 1  |

    Scenario: bitwise chain operations
      When query
        """
        SELECT (15 & 12) | 3 AS result
        """
      Then query result
        | result |
        | 15     |

    Scenario: bit_count of bitwise result
      When query
        """
        SELECT bit_count(255 & 170) AS result
        """
      Then query result
        | result |
        | 4      |

    Scenario: all zeros
      When query
        """
        SELECT 0 & 0 AS and_r, 0 | 0 AS or_r, 0 ^ 0 AS xor_r, ~0 AS not_r, bit_count(0) AS cnt_r
        """
      Then query result
        | and_r | or_r | xor_r | not_r | cnt_r |
        | 0     | 0    | 0     | -1    | 0     |

    Scenario: all ones (int)
      When query
        """
        SELECT -1 & -1 AS and_r, -1 | -1 AS or_r, -1 ^ -1 AS xor_r, bit_count(-1) AS cnt_r
        """
      Then query result
        | and_r | or_r | xor_r | cnt_r |
        | -1    | -1   | 0     | 64    |

    Scenario: int min max values
      When query
        """
        SELECT 2147483647 & -2147483648 AS and_r, 2147483647 | -2147483648 AS or_r, 2147483647 ^ -2147483648 AS xor_r
        """
      Then query result
        | and_r | or_r | xor_r |
        | 0     | -1   | -1    |

  Rule: ANSI mode off (same behavior as ANSI on for bitwise)

    Scenario: tilde with bigint extremes ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT ~CAST(9223372036854775807 AS BIGINT) AS r1, ~CAST(-9223372036854775808 AS BIGINT) AS r2
        """
      Then query result
        | r1                    | r2                   |
        | -9223372036854775808  | 9223372036854775807  |

    Scenario: bit_get invalid position errors even with ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT bit_get(11, -1) AS result
        """
      # Spark: INVALID_PARAMETER_VALUE.BIT_POSITION_RANGE, Sail: Compute error (different message, same semantics)
      Then query error bit_get

    Scenario: bit_get beyond bit width errors even with ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT bit_get(11, 32) AS result
        """
      # Spark: INVALID_PARAMETER_VALUE.BIT_POSITION_RANGE, Sail: Compute error (different message, same semantics)
      Then query error bit_get

    Scenario: shiftleft wraps on 32 with ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT shiftleft(1, 32) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: shiftrightunsigned wraps on 32 with ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT shiftrightunsigned(1, 32) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: shiftrightunsigned with negative shift ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT shiftleft(16, -1) AS r1, shiftright(16, -1) AS r2
        """
      Then query result
        | r1 | r2 |
        | 0  | 0  |

  Rule: Sail-specific (bitwise_not function)

    @sail-only
    Scenario: bitwise_not function basic
      When query
        """
        SELECT bitwise_not(0) AS r0, bitwise_not(1) AS r1, bitwise_not(-1) AS r2
        """
      Then query result
        | r0 | r1 | r2 |
        | -1 | -2 | 0  |

    @sail-only
    Scenario: bitwise_not function with NULL
      When query
        """
        SELECT bitwise_not(CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |
