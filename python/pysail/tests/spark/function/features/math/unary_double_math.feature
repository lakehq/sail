Feature: unary DOUBLE math functions (acos and siblings)

  # Spark's UnaryMathExpression family: ImplicitCastInputTypes(DoubleType), always nullable DOUBLE.
  # Expected values come from the Spark JVM (4.2.0); the `acosh` and `asinh` digits are 4.2's, which
  # rewrote both formulas. `signum`/`sign` (they also accept intervals), `ceil`/`floor` and the binary
  # functions (`atan2`, `hypot`, `pow`, `log`) are not part of this family here.

  Rule: Argument count error carries Spark's class

    Scenario Outline: <fn> with <count> arguments is WRONG_NUM_ARGS
      When query
        """
        SELECT <fn>(<args>) AS result
        """
      Then query error \[WRONG_NUM_ARGS.*The `<fn>` requires 1 parameters but the actual number is <count>

      Examples:
        | fn      | count | args |
        | acos    | 0     |      |
        | acos    | 2     | 1, 2 |
        | acosh   | 0     |      |
        | acosh   | 2     | 1, 2 |
        | asin    | 0     |      |
        | asin    | 2     | 1, 2 |
        | asinh   | 0     |      |
        | asinh   | 2     | 1, 2 |
        | atan    | 0     |      |
        | atan    | 2     | 1, 2 |
        | atanh   | 0     |      |
        | atanh   | 2     | 1, 2 |
        | cbrt    | 0     |      |
        | cbrt    | 2     | 1, 2 |
        | cos     | 0     |      |
        | cos     | 2     | 1, 2 |
        | cosh    | 0     |      |
        | cosh    | 2     | 1, 2 |
        | cot     | 0     |      |
        | cot     | 2     | 1, 2 |
        | csc     | 0     |      |
        | csc     | 2     | 1, 2 |
        | degrees | 0     |      |
        | degrees | 2     | 1, 2 |
        | exp     | 0     |      |
        | exp     | 2     | 1, 2 |
        | ln      | 0     |      |
        | ln      | 2     | 1, 2 |
        | log10   | 0     |      |
        | log10   | 2     | 1, 2 |
        | log1p   | 0     |      |
        | log1p   | 2     | 1, 2 |
        | log2    | 0     |      |
        | log2    | 2     | 1, 2 |
        | radians | 0     |      |
        | radians | 2     | 1, 2 |
        | sec     | 0     |      |
        | sec     | 2     | 1, 2 |
        | sin     | 0     |      |
        | sin     | 2     | 1, 2 |
        | sinh    | 0     |      |
        | sinh    | 2     | 1, 2 |
        | tan     | 0     |      |
        | tan     | 2     | 1, 2 |
        | tanh    | 0     |      |
        | tanh    | 2     | 1, 2 |

  Rule: Argument types Spark rejects

    Scenario Outline: <fn> of <type> is DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE
      When query
        """
        SELECT <fn>(<input>) AS result
        """
      Then query error \[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE\]

      Examples:
        | fn      | type                | input                          |
        | acos    | BOOLEAN             | true                           |
        | acos    | DATE                | DATE'2024-01-02'               |
        | acos    | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | acos    | BINARY              | X'01'                          |
        | acos    | ARRAY               | array(1.0D)                    |
        | acos    | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | acos    | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | acosh   | BOOLEAN             | true                           |
        | acosh   | DATE                | DATE'2024-01-02'               |
        | acosh   | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | acosh   | BINARY              | X'01'                          |
        | acosh   | ARRAY               | array(1.0D)                    |
        | acosh   | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | acosh   | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | asin    | BOOLEAN             | true                           |
        | asin    | DATE                | DATE'2024-01-02'               |
        | asin    | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | asin    | BINARY              | X'01'                          |
        | asin    | ARRAY               | array(1.0D)                    |
        | asin    | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | asin    | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | asinh   | BOOLEAN             | true                           |
        | asinh   | DATE                | DATE'2024-01-02'               |
        | asinh   | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | asinh   | BINARY              | X'01'                          |
        | asinh   | ARRAY               | array(1.0D)                    |
        | asinh   | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | asinh   | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | atan    | BOOLEAN             | true                           |
        | atan    | DATE                | DATE'2024-01-02'               |
        | atan    | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | atan    | BINARY              | X'01'                          |
        | atan    | ARRAY               | array(1.0D)                    |
        | atan    | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | atan    | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | atanh   | BOOLEAN             | true                           |
        | atanh   | DATE                | DATE'2024-01-02'               |
        | atanh   | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | atanh   | BINARY              | X'01'                          |
        | atanh   | ARRAY               | array(1.0D)                    |
        | atanh   | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | atanh   | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | cbrt    | BOOLEAN             | true                           |
        | cbrt    | DATE                | DATE'2024-01-02'               |
        | cbrt    | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | cbrt    | BINARY              | X'01'                          |
        | cbrt    | ARRAY               | array(1.0D)                    |
        | cbrt    | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | cbrt    | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | cos     | BOOLEAN             | true                           |
        | cos     | DATE                | DATE'2024-01-02'               |
        | cos     | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | cos     | BINARY              | X'01'                          |
        | cos     | ARRAY               | array(1.0D)                    |
        | cos     | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | cos     | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | cosh    | BOOLEAN             | true                           |
        | cosh    | DATE                | DATE'2024-01-02'               |
        | cosh    | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | cosh    | BINARY              | X'01'                          |
        | cosh    | ARRAY               | array(1.0D)                    |
        | cosh    | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | cosh    | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | cot     | BOOLEAN             | true                           |
        | cot     | DATE                | DATE'2024-01-02'               |
        | cot     | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | cot     | BINARY              | X'01'                          |
        | cot     | ARRAY               | array(1.0D)                    |
        | cot     | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | cot     | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | csc     | BOOLEAN             | true                           |
        | csc     | DATE                | DATE'2024-01-02'               |
        | csc     | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | csc     | BINARY              | X'01'                          |
        | csc     | ARRAY               | array(1.0D)                    |
        | csc     | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | csc     | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | degrees | BOOLEAN             | true                           |
        | degrees | DATE                | DATE'2024-01-02'               |
        | degrees | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | degrees | BINARY              | X'01'                          |
        | degrees | ARRAY               | array(1.0D)                    |
        | degrees | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | degrees | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | exp     | BOOLEAN             | true                           |
        | exp     | DATE                | DATE'2024-01-02'               |
        | exp     | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | exp     | BINARY              | X'01'                          |
        | exp     | ARRAY               | array(1.0D)                    |
        | exp     | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | exp     | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | ln      | BOOLEAN             | true                           |
        | ln      | DATE                | DATE'2024-01-02'               |
        | ln      | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | ln      | BINARY              | X'01'                          |
        | ln      | ARRAY               | array(1.0D)                    |
        | ln      | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | ln      | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | log10   | BOOLEAN             | true                           |
        | log10   | DATE                | DATE'2024-01-02'               |
        | log10   | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | log10   | BINARY              | X'01'                          |
        | log10   | ARRAY               | array(1.0D)                    |
        | log10   | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | log10   | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | log1p   | BOOLEAN             | true                           |
        | log1p   | DATE                | DATE'2024-01-02'               |
        | log1p   | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | log1p   | BINARY              | X'01'                          |
        | log1p   | ARRAY               | array(1.0D)                    |
        | log1p   | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | log1p   | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | log2    | BOOLEAN             | true                           |
        | log2    | DATE                | DATE'2024-01-02'               |
        | log2    | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | log2    | BINARY              | X'01'                          |
        | log2    | ARRAY               | array(1.0D)                    |
        | log2    | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | log2    | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | radians | BOOLEAN             | true                           |
        | radians | DATE                | DATE'2024-01-02'               |
        | radians | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | radians | BINARY              | X'01'                          |
        | radians | ARRAY               | array(1.0D)                    |
        | radians | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | radians | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | sec     | BOOLEAN             | true                           |
        | sec     | DATE                | DATE'2024-01-02'               |
        | sec     | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | sec     | BINARY              | X'01'                          |
        | sec     | ARRAY               | array(1.0D)                    |
        | sec     | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | sec     | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | sin     | BOOLEAN             | true                           |
        | sin     | DATE                | DATE'2024-01-02'               |
        | sin     | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | sin     | BINARY              | X'01'                          |
        | sin     | ARRAY               | array(1.0D)                    |
        | sin     | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | sin     | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | sinh    | BOOLEAN             | true                           |
        | sinh    | DATE                | DATE'2024-01-02'               |
        | sinh    | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | sinh    | BINARY              | X'01'                          |
        | sinh    | ARRAY               | array(1.0D)                    |
        | sinh    | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | sinh    | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | tan     | BOOLEAN             | true                           |
        | tan     | DATE                | DATE'2024-01-02'               |
        | tan     | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | tan     | BINARY              | X'01'                          |
        | tan     | ARRAY               | array(1.0D)                    |
        | tan     | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | tan     | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |
        | tanh    | BOOLEAN             | true                           |
        | tanh    | DATE                | DATE'2024-01-02'               |
        | tanh    | TIMESTAMP           | TIMESTAMP'2024-01-02 03:04:05' |
        | tanh    | BINARY              | X'01'                          |
        | tanh    | ARRAY               | array(1.0D)                    |
        | tanh    | YEAR-MONTH INTERVAL | INTERVAL '1' YEAR              |
        | tanh    | DAY-TIME INTERVAL   | INTERVAL '1' DAY               |

    Scenario Outline: <fn> of a BOOLEAN is rejected under ANSI=true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <fn>(true) AS result
        """
      Then query error \[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE\]

      Examples:
        | fn      |
        | acos    |
        | acosh   |
        | asin    |
        | asinh   |
        | atan    |
        | atanh   |
        | cbrt    |
        | cos     |
        | cosh    |
        | cot     |
        | csc     |
        | degrees |
        | exp     |
        | ln      |
        | log10   |
        | log1p   |
        | log2    |
        | radians |
        | sec     |
        | sin     |
        | sinh    |
        | tan     |
        | tanh    |

  Rule: A malformed string is NULL under ANSI=false

    Scenario Outline: <fn> of <what> is NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <fn>(<input>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | fn      | what                 | input |
        | acos    | a non-numeric string | 'abc' |
        | acos    | an empty string      | ''    |
        | acosh   | a non-numeric string | 'abc' |
        | acosh   | an empty string      | ''    |
        | asin    | a non-numeric string | 'abc' |
        | asin    | an empty string      | ''    |
        | asinh   | a non-numeric string | 'abc' |
        | asinh   | an empty string      | ''    |
        | atan    | a non-numeric string | 'abc' |
        | atan    | an empty string      | ''    |
        | atanh   | a non-numeric string | 'abc' |
        | atanh   | an empty string      | ''    |
        | cbrt    | a non-numeric string | 'abc' |
        | cbrt    | an empty string      | ''    |
        | cos     | a non-numeric string | 'abc' |
        | cos     | an empty string      | ''    |
        | cosh    | a non-numeric string | 'abc' |
        | cosh    | an empty string      | ''    |
        | cot     | a non-numeric string | 'abc' |
        | cot     | an empty string      | ''    |
        | csc     | a non-numeric string | 'abc' |
        | csc     | an empty string      | ''    |
        | degrees | a non-numeric string | 'abc' |
        | degrees | an empty string      | ''    |
        | exp     | a non-numeric string | 'abc' |
        | exp     | an empty string      | ''    |
        | ln      | a non-numeric string | 'abc' |
        | ln      | an empty string      | ''    |
        | log10   | a non-numeric string | 'abc' |
        | log10   | an empty string      | ''    |
        | log1p   | a non-numeric string | 'abc' |
        | log1p   | an empty string      | ''    |
        | log2    | a non-numeric string | 'abc' |
        | log2    | an empty string      | ''    |
        | radians | a non-numeric string | 'abc' |
        | radians | an empty string      | ''    |
        | sec     | a non-numeric string | 'abc' |
        | sec     | an empty string      | ''    |
        | sin     | a non-numeric string | 'abc' |
        | sin     | an empty string      | ''    |
        | sinh    | a non-numeric string | 'abc' |
        | sinh    | an empty string      | ''    |
        | tan     | a non-numeric string | 'abc' |
        | tan     | an empty string      | ''    |
        | tanh    | a non-numeric string | 'abc' |
        | tanh    | an empty string      | ''    |

  Rule: Values

    Scenario Outline: <fn>(<input>) = <result> (<what>)
      When query
        """
        SELECT <fn>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | fn      | what        | input                       | result                |
        | acos    | zero        | 0                           | 1.5707963267948966    |
        | acos    | one         | 1                           | 0.0                   |
        | acos    | neg_one     | -1                          | 3.141592653589793     |
        | acos    | two         | 2                           | NaN                   |
        | acos    | neg_two     | -2                          | NaN                   |
        | acos    | neg_half    | -0.5                        | 2.0943951023931957    |
        | acos    | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | acos    | inf         | CAST('Infinity' AS DOUBLE)  | NaN                   |
        | acos    | neg_inf     | CAST('-Infinity' AS DOUBLE) | NaN                   |
        | acos    | neg_zero    | CAST(-0.0 AS DOUBLE)        | 1.5707963267948966    |
        | acos    | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | acos    | null_int    | CAST(NULL AS INT)           | NULL                  |
        | acos    | tinyint     | CAST(1 AS TINYINT)          | 0.0                   |
        | acos    | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | 2.0943951023931957    |
        | acos    | str_padded  | ' 1 '                       | 0.0                   |
        | acosh   | zero        | 0                           | NaN                   |
        | acosh   | one         | 1                           | 0.0                   |
        | acosh   | neg_one     | -1                          | NaN                   |
        | acosh   | half        | 0.5                         | NaN                   |
        | acosh   | two         | 2                           | 1.3169578969248166    |
        | acosh   | neg_two     | -2                          | NaN                   |
        | acosh   | neg_half    | -0.5                        | NaN                   |
        | acosh   | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | acosh   | inf         | CAST('Infinity' AS DOUBLE)  | Infinity              |
        | acosh   | neg_inf     | CAST('-Infinity' AS DOUBLE) | NaN                   |
        | acosh   | neg_zero    | CAST(-0.0 AS DOUBLE)        | NaN                   |
        | acosh   | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | acosh   | null_int    | CAST(NULL AS INT)           | NULL                  |
        | acosh   | tinyint     | CAST(1 AS TINYINT)          | 0.0                   |
        | acosh   | float       | CAST(0.5 AS FLOAT)          | NaN                   |
        | acosh   | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | NaN                   |
        | acosh   | str_ok      | '0.5'                       | NaN                   |
        | acosh   | str_padded  | ' 1 '                       | 0.0                   |
        | asin    | zero        | 0                           | 0.0                   |
        | asin    | one         | 1                           | 1.5707963267948966    |
        | asin    | neg_one     | -1                          | -1.5707963267948966   |
        | asin    | two         | 2                           | NaN                   |
        | asin    | neg_two     | -2                          | NaN                   |
        | asin    | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | asin    | inf         | CAST('Infinity' AS DOUBLE)  | NaN                   |
        | asin    | neg_inf     | CAST('-Infinity' AS DOUBLE) | NaN                   |
        | asin    | neg_zero    | CAST(-0.0 AS DOUBLE)        | 0.0                   |
        | asin    | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | asin    | null_int    | CAST(NULL AS INT)           | NULL                  |
        | asin    | tinyint     | CAST(1 AS TINYINT)          | 1.5707963267948966    |
        | asin    | str_padded  | ' 1 '                       | 1.5707963267948966    |
        | asinh   | zero        | 0                           | 0.0                   |
        | asinh   | one         | 1                           | 0.881373587019543     |
        | asinh   | neg_one     | -1                          | -0.881373587019543    |
        | asinh   | half        | 0.5                         | 0.48121182505960347   |
        | asinh   | two         | 2                           | 1.4436354751788103    |
        | asinh   | neg_two     | -2                          | -1.4436354751788103   |
        | asinh   | neg_half    | -0.5                        | -0.48121182505960347  |
        | asinh   | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | asinh   | inf         | CAST('Infinity' AS DOUBLE)  | Infinity              |
        | asinh   | neg_inf     | CAST('-Infinity' AS DOUBLE) | -Infinity             |
        | asinh   | neg_zero    | CAST(-0.0 AS DOUBLE)        | 0.0                   |
        | asinh   | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | asinh   | null_int    | CAST(NULL AS INT)           | NULL                  |
        | asinh   | tinyint     | CAST(1 AS TINYINT)          | 0.881373587019543     |
        | asinh   | float       | CAST(0.5 AS FLOAT)          | 0.48121182505960347   |
        | asinh   | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | -0.48121182505960347  |
        | asinh   | str_ok      | '0.5'                       | 0.48121182505960347   |
        | asinh   | str_padded  | ' 1 '                       | 0.881373587019543     |
        | atan    | zero        | 0                           | 0.0                   |
        | atan    | one         | 1                           | 0.7853981633974483    |
        | atan    | neg_one     | -1                          | -0.7853981633974483   |
        | atan    | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | atan    | inf         | CAST('Infinity' AS DOUBLE)  | 1.5707963267948966    |
        | atan    | neg_inf     | CAST('-Infinity' AS DOUBLE) | -1.5707963267948966   |
        | atan    | neg_zero    | CAST(-0.0 AS DOUBLE)        | 0.0                   |
        | atan    | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | atan    | null_int    | CAST(NULL AS INT)           | NULL                  |
        | atan    | tinyint     | CAST(1 AS TINYINT)          | 0.7853981633974483    |
        | atan    | str_padded  | ' 1 '                       | 0.7853981633974483    |
        | atanh   | zero        | 0                           | 0.0                   |
        | atanh   | one         | 1                           | Infinity              |
        | atanh   | neg_one     | -1                          | -Infinity             |
        | atanh   | two         | 2                           | NaN                   |
        | atanh   | neg_two     | -2                          | NaN                   |
        | atanh   | neg_half    | -0.5                        | -0.5493061443340548   |
        | atanh   | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | atanh   | inf         | CAST('Infinity' AS DOUBLE)  | NaN                   |
        | atanh   | neg_inf     | CAST('-Infinity' AS DOUBLE) | NaN                   |
        | atanh   | neg_zero    | CAST(-0.0 AS DOUBLE)        | 0.0                   |
        | atanh   | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | atanh   | null_int    | CAST(NULL AS INT)           | NULL                  |
        | atanh   | tinyint     | CAST(1 AS TINYINT)          | Infinity              |
        | atanh   | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | -0.5493061443340548   |
        | atanh   | str_padded  | ' 1 '                       | Infinity              |
        | cbrt    | zero        | 0                           | 0.0                   |
        | cbrt    | one         | 1                           | 1.0                   |
        | cbrt    | neg_one     | -1                          | -1.0                  |
        | cbrt    | half        | 0.5                         | 0.7937005259840998    |
        | cbrt    | two         | 2                           | 1.2599210498948732    |
        | cbrt    | neg_two     | -2                          | -1.2599210498948732   |
        | cbrt    | neg_half    | -0.5                        | -0.7937005259840998   |
        | cbrt    | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | cbrt    | inf         | CAST('Infinity' AS DOUBLE)  | Infinity              |
        | cbrt    | neg_inf     | CAST('-Infinity' AS DOUBLE) | -Infinity             |
        | cbrt    | neg_zero    | CAST(-0.0 AS DOUBLE)        | 0.0                   |
        | cbrt    | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | cbrt    | null_int    | CAST(NULL AS INT)           | NULL                  |
        | cbrt    | tinyint     | CAST(1 AS TINYINT)          | 1.0                   |
        | cbrt    | float       | CAST(0.5 AS FLOAT)          | 0.7937005259840998    |
        | cbrt    | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | -0.7937005259840998   |
        | cbrt    | str_ok      | '0.5'                       | 0.7937005259840998    |
        | cbrt    | str_padded  | ' 1 '                       | 1.0                   |
        | cos     | zero        | 0                           | 1.0                   |
        | cos     | one         | 1                           | 0.5403023058681398    |
        | cos     | neg_one     | -1                          | 0.5403023058681398    |
        | cos     | half        | 0.5                         | 0.8775825618903728    |
        | cos     | two         | 2                           | -0.4161468365471424   |
        | cos     | neg_two     | -2                          | -0.4161468365471424   |
        | cos     | neg_half    | -0.5                        | 0.8775825618903728    |
        | cos     | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | cos     | inf         | CAST('Infinity' AS DOUBLE)  | NaN                   |
        | cos     | neg_inf     | CAST('-Infinity' AS DOUBLE) | NaN                   |
        | cos     | neg_zero    | CAST(-0.0 AS DOUBLE)        | 1.0                   |
        | cos     | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | cos     | null_int    | CAST(NULL AS INT)           | NULL                  |
        | cos     | tinyint     | CAST(1 AS TINYINT)          | 0.5403023058681398    |
        | cos     | float       | CAST(0.5 AS FLOAT)          | 0.8775825618903728    |
        | cos     | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | 0.8775825618903728    |
        | cos     | str_ok      | '0.5'                       | 0.8775825618903728    |
        | cos     | str_padded  | ' 1 '                       | 0.5403023058681398    |
        | cosh    | zero        | 0                           | 1.0                   |
        | cosh    | half        | 0.5                         | 1.1276259652063807    |
        | cosh    | two         | 2                           | 3.7621956910836314    |
        | cosh    | neg_two     | -2                          | 3.7621956910836314    |
        | cosh    | neg_half    | -0.5                        | 1.1276259652063807    |
        | cosh    | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | cosh    | inf         | CAST('Infinity' AS DOUBLE)  | Infinity              |
        | cosh    | neg_inf     | CAST('-Infinity' AS DOUBLE) | Infinity              |
        | cosh    | neg_zero    | CAST(-0.0 AS DOUBLE)        | 1.0                   |
        | cosh    | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | cosh    | null_int    | CAST(NULL AS INT)           | NULL                  |
        | cosh    | float       | CAST(0.5 AS FLOAT)          | 1.1276259652063807    |
        | cosh    | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | 1.1276259652063807    |
        | cosh    | str_ok      | '0.5'                       | 1.1276259652063807    |
        | cot     | zero        | 0                           | Infinity              |
        | cot     | half        | 0.5                         | 1.830487721712452     |
        | cot     | two         | 2                           | -0.45765755436028577  |
        | cot     | neg_two     | -2                          | 0.45765755436028577   |
        | cot     | neg_half    | -0.5                        | -1.830487721712452    |
        | cot     | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | cot     | inf         | CAST('Infinity' AS DOUBLE)  | NaN                   |
        | cot     | neg_inf     | CAST('-Infinity' AS DOUBLE) | NaN                   |
        | cot     | neg_zero    | CAST(-0.0 AS DOUBLE)        | Infinity              |
        | cot     | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | cot     | null_int    | CAST(NULL AS INT)           | NULL                  |
        | cot     | float       | CAST(0.5 AS FLOAT)          | 1.830487721712452     |
        | cot     | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | -1.830487721712452    |
        | cot     | str_ok      | '0.5'                       | 1.830487721712452     |
        | csc     | zero        | 0                           | Infinity              |
        | csc     | one         | 1                           | 1.1883951057781212    |
        | csc     | neg_one     | -1                          | -1.1883951057781212   |
        | csc     | half        | 0.5                         | 2.085829642933488     |
        | csc     | two         | 2                           | 1.0997501702946164    |
        | csc     | neg_two     | -2                          | -1.0997501702946164   |
        | csc     | neg_half    | -0.5                        | -2.085829642933488    |
        | csc     | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | csc     | inf         | CAST('Infinity' AS DOUBLE)  | NaN                   |
        | csc     | neg_inf     | CAST('-Infinity' AS DOUBLE) | NaN                   |
        | csc     | neg_zero    | CAST(-0.0 AS DOUBLE)        | Infinity              |
        | csc     | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | csc     | null_int    | CAST(NULL AS INT)           | NULL                  |
        | csc     | tinyint     | CAST(1 AS TINYINT)          | 1.1883951057781212    |
        | csc     | float       | CAST(0.5 AS FLOAT)          | 2.085829642933488     |
        | csc     | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | -2.085829642933488    |
        | csc     | str_ok      | '0.5'                       | 2.085829642933488     |
        | csc     | str_padded  | ' 1 '                       | 1.1883951057781212    |
        | degrees | zero        | 0                           | 0.0                   |
        | degrees | one         | 1                           | 57.29577951308232     |
        | degrees | neg_one     | -1                          | -57.29577951308232    |
        | degrees | half        | 0.5                         | 28.64788975654116     |
        | degrees | two         | 2                           | 114.59155902616465    |
        | degrees | neg_two     | -2                          | -114.59155902616465   |
        | degrees | neg_half    | -0.5                        | -28.64788975654116    |
        | degrees | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | degrees | inf         | CAST('Infinity' AS DOUBLE)  | Infinity              |
        | degrees | neg_inf     | CAST('-Infinity' AS DOUBLE) | -Infinity             |
        | degrees | neg_zero    | CAST(-0.0 AS DOUBLE)        | 0.0                   |
        | degrees | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | degrees | null_int    | CAST(NULL AS INT)           | NULL                  |
        | degrees | tinyint     | CAST(1 AS TINYINT)          | 57.29577951308232     |
        | degrees | float       | CAST(0.5 AS FLOAT)          | 28.64788975654116     |
        | degrees | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | -28.64788975654116    |
        | degrees | str_ok      | '0.5'                       | 28.64788975654116     |
        | degrees | str_padded  | ' 1 '                       | 57.29577951308232     |
        | exp     | zero        | 0                           | 1.0                   |
        | exp     | neg_one     | -1                          | 0.36787944117144233   |
        | exp     | half        | 0.5                         | 1.6487212707001282    |
        | exp     | two         | 2                           | 7.38905609893065      |
        | exp     | neg_two     | -2                          | 0.1353352832366127    |
        | exp     | neg_half    | -0.5                        | 0.6065306597126334    |
        | exp     | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | exp     | inf         | CAST('Infinity' AS DOUBLE)  | Infinity              |
        | exp     | neg_inf     | CAST('-Infinity' AS DOUBLE) | 0.0                   |
        | exp     | neg_zero    | CAST(-0.0 AS DOUBLE)        | 1.0                   |
        | exp     | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | exp     | null_int    | CAST(NULL AS INT)           | NULL                  |
        | exp     | float       | CAST(0.5 AS FLOAT)          | 1.6487212707001282    |
        | exp     | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | 0.6065306597126334    |
        | exp     | str_ok      | '0.5'                       | 1.6487212707001282    |
        | ln      | zero        | 0                           | NULL                  |
        | ln      | one         | 1                           | 0.0                   |
        | ln      | neg_one     | -1                          | NULL                  |
        | ln      | half        | 0.5                         | -0.6931471805599453   |
        | ln      | two         | 2                           | 0.6931471805599453    |
        | ln      | neg_two     | -2                          | NULL                  |
        | ln      | neg_half    | -0.5                        | NULL                  |
        | ln      | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | ln      | inf         | CAST('Infinity' AS DOUBLE)  | Infinity              |
        | ln      | neg_inf     | CAST('-Infinity' AS DOUBLE) | NULL                  |
        | ln      | neg_zero    | CAST(-0.0 AS DOUBLE)        | NULL                  |
        | ln      | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | ln      | null_int    | CAST(NULL AS INT)           | NULL                  |
        | ln      | tinyint     | CAST(1 AS TINYINT)          | 0.0                   |
        | ln      | float       | CAST(0.5 AS FLOAT)          | -0.6931471805599453   |
        | ln      | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | NULL                  |
        | ln      | str_ok      | '0.5'                       | -0.6931471805599453   |
        | ln      | str_padded  | ' 1 '                       | 0.0                   |
        | log10   | zero        | 0                           | NULL                  |
        | log10   | one         | 1                           | 0.0                   |
        | log10   | neg_one     | -1                          | NULL                  |
        | log10   | half        | 0.5                         | -0.3010299956639812   |
        | log10   | two         | 2                           | 0.3010299956639812    |
        | log10   | neg_two     | -2                          | NULL                  |
        | log10   | neg_half    | -0.5                        | NULL                  |
        | log10   | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | log10   | inf         | CAST('Infinity' AS DOUBLE)  | Infinity              |
        | log10   | neg_inf     | CAST('-Infinity' AS DOUBLE) | NULL                  |
        | log10   | neg_zero    | CAST(-0.0 AS DOUBLE)        | NULL                  |
        | log10   | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | log10   | null_int    | CAST(NULL AS INT)           | NULL                  |
        | log10   | tinyint     | CAST(1 AS TINYINT)          | 0.0                   |
        | log10   | float       | CAST(0.5 AS FLOAT)          | -0.3010299956639812   |
        | log10   | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | NULL                  |
        | log10   | str_ok      | '0.5'                       | -0.3010299956639812   |
        | log10   | str_padded  | ' 1 '                       | 0.0                   |
        | log1p   | zero        | 0                           | 0.0                   |
        | log1p   | one         | 1                           | 0.6931471805599453    |
        | log1p   | neg_one     | -1                          | NULL                  |
        | log1p   | half        | 0.5                         | 0.4054651081081644    |
        | log1p   | neg_two     | -2                          | NULL                  |
        | log1p   | neg_half    | -0.5                        | -0.6931471805599453   |
        | log1p   | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | log1p   | inf         | CAST('Infinity' AS DOUBLE)  | Infinity              |
        | log1p   | neg_inf     | CAST('-Infinity' AS DOUBLE) | NULL                  |
        | log1p   | neg_zero    | CAST(-0.0 AS DOUBLE)        | 0.0                   |
        | log1p   | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | log1p   | null_int    | CAST(NULL AS INT)           | NULL                  |
        | log1p   | tinyint     | CAST(1 AS TINYINT)          | 0.6931471805599453    |
        | log1p   | float       | CAST(0.5 AS FLOAT)          | 0.4054651081081644    |
        | log1p   | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | -0.6931471805599453   |
        | log1p   | str_ok      | '0.5'                       | 0.4054651081081644    |
        | log1p   | str_padded  | ' 1 '                       | 0.6931471805599453    |
        | log2    | zero        | 0                           | NULL                  |
        | log2    | one         | 1                           | 0.0                   |
        | log2    | neg_one     | -1                          | NULL                  |
        | log2    | half        | 0.5                         | -1.0                  |
        | log2    | two         | 2                           | 1.0                   |
        | log2    | neg_two     | -2                          | NULL                  |
        | log2    | neg_half    | -0.5                        | NULL                  |
        | log2    | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | log2    | inf         | CAST('Infinity' AS DOUBLE)  | Infinity              |
        | log2    | neg_inf     | CAST('-Infinity' AS DOUBLE) | NULL                  |
        | log2    | neg_zero    | CAST(-0.0 AS DOUBLE)        | NULL                  |
        | log2    | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | log2    | null_int    | CAST(NULL AS INT)           | NULL                  |
        | log2    | tinyint     | CAST(1 AS TINYINT)          | 0.0                   |
        | log2    | float       | CAST(0.5 AS FLOAT)          | -1.0                  |
        | log2    | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | NULL                  |
        | log2    | str_ok      | '0.5'                       | -1.0                  |
        | log2    | str_padded  | ' 1 '                       | 0.0                   |
        | radians | zero        | 0                           | 0.0                   |
        | radians | one         | 1                           | 0.017453292519943295  |
        | radians | neg_one     | -1                          | -0.017453292519943295 |
        | radians | half        | 0.5                         | 0.008726646259971648  |
        | radians | two         | 2                           | 0.03490658503988659   |
        | radians | neg_two     | -2                          | -0.03490658503988659  |
        | radians | neg_half    | -0.5                        | -0.008726646259971648 |
        | radians | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | radians | inf         | CAST('Infinity' AS DOUBLE)  | Infinity              |
        | radians | neg_inf     | CAST('-Infinity' AS DOUBLE) | -Infinity             |
        | radians | neg_zero    | CAST(-0.0 AS DOUBLE)        | 0.0                   |
        | radians | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | radians | null_int    | CAST(NULL AS INT)           | NULL                  |
        | radians | tinyint     | CAST(1 AS TINYINT)          | 0.017453292519943295  |
        | radians | float       | CAST(0.5 AS FLOAT)          | 0.008726646259971648  |
        | radians | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | -0.008726646259971648 |
        | radians | str_ok      | '0.5'                       | 0.008726646259971648  |
        | radians | str_padded  | ' 1 '                       | 0.017453292519943295  |
        | sec     | zero        | 0                           | 1.0                   |
        | sec     | one         | 1                           | 1.8508157176809255    |
        | sec     | neg_one     | -1                          | 1.8508157176809255    |
        | sec     | half        | 0.5                         | 1.139493927324549     |
        | sec     | two         | 2                           | -2.402997961722381    |
        | sec     | neg_two     | -2                          | -2.402997961722381    |
        | sec     | neg_half    | -0.5                        | 1.139493927324549     |
        | sec     | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | sec     | inf         | CAST('Infinity' AS DOUBLE)  | NaN                   |
        | sec     | neg_inf     | CAST('-Infinity' AS DOUBLE) | NaN                   |
        | sec     | neg_zero    | CAST(-0.0 AS DOUBLE)        | 1.0                   |
        | sec     | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | sec     | null_int    | CAST(NULL AS INT)           | NULL                  |
        | sec     | tinyint     | CAST(1 AS TINYINT)          | 1.8508157176809255    |
        | sec     | float       | CAST(0.5 AS FLOAT)          | 1.139493927324549     |
        | sec     | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | 1.139493927324549     |
        | sec     | str_ok      | '0.5'                       | 1.139493927324549     |
        | sec     | str_padded  | ' 1 '                       | 1.8508157176809255    |
        | sin     | zero        | 0                           | 0.0                   |
        | sin     | one         | 1                           | 0.8414709848078965    |
        | sin     | neg_one     | -1                          | -0.8414709848078965   |
        | sin     | half        | 0.5                         | 0.479425538604203     |
        | sin     | two         | 2                           | 0.9092974268256817    |
        | sin     | neg_two     | -2                          | -0.9092974268256817   |
        | sin     | neg_half    | -0.5                        | -0.479425538604203    |
        | sin     | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | sin     | inf         | CAST('Infinity' AS DOUBLE)  | NaN                   |
        | sin     | neg_inf     | CAST('-Infinity' AS DOUBLE) | NaN                   |
        | sin     | neg_zero    | CAST(-0.0 AS DOUBLE)        | 0.0                   |
        | sin     | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | sin     | null_int    | CAST(NULL AS INT)           | NULL                  |
        | sin     | tinyint     | CAST(1 AS TINYINT)          | 0.8414709848078965    |
        | sin     | float       | CAST(0.5 AS FLOAT)          | 0.479425538604203     |
        | sin     | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | -0.479425538604203    |
        | sin     | str_ok      | '0.5'                       | 0.479425538604203     |
        | sin     | str_padded  | ' 1 '                       | 0.8414709848078965    |
        | sinh    | zero        | 0                           | 0.0                   |
        | sinh    | one         | 1                           | 1.1752011936438014    |
        | sinh    | neg_one     | -1                          | -1.1752011936438014   |
        | sinh    | half        | 0.5                         | 0.5210953054937474    |
        | sinh    | neg_half    | -0.5                        | -0.5210953054937474   |
        | sinh    | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | sinh    | inf         | CAST('Infinity' AS DOUBLE)  | Infinity              |
        | sinh    | neg_inf     | CAST('-Infinity' AS DOUBLE) | -Infinity             |
        | sinh    | neg_zero    | CAST(-0.0 AS DOUBLE)        | 0.0                   |
        | sinh    | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | sinh    | null_int    | CAST(NULL AS INT)           | NULL                  |
        | sinh    | tinyint     | CAST(1 AS TINYINT)          | 1.1752011936438014    |
        | sinh    | float       | CAST(0.5 AS FLOAT)          | 0.5210953054937474    |
        | sinh    | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | -0.5210953054937474   |
        | sinh    | str_ok      | '0.5'                       | 0.5210953054937474    |
        | sinh    | str_padded  | ' 1 '                       | 1.1752011936438014    |
        | tan     | zero        | 0                           | 0.0                   |
        | tan     | half        | 0.5                         | 0.5463024898437905    |
        | tan     | two         | 2                           | -2.185039863261519    |
        | tan     | neg_two     | -2                          | 2.185039863261519     |
        | tan     | neg_half    | -0.5                        | -0.5463024898437905   |
        | tan     | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | tan     | inf         | CAST('Infinity' AS DOUBLE)  | NaN                   |
        | tan     | neg_inf     | CAST('-Infinity' AS DOUBLE) | NaN                   |
        | tan     | neg_zero    | CAST(-0.0 AS DOUBLE)        | 0.0                   |
        | tan     | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | tan     | null_int    | CAST(NULL AS INT)           | NULL                  |
        | tan     | float       | CAST(0.5 AS FLOAT)          | 0.5463024898437905    |
        | tan     | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | -0.5463024898437905   |
        | tan     | str_ok      | '0.5'                       | 0.5463024898437905    |
        | tanh    | zero        | 0                           | 0.0                   |
        | tanh    | one         | 1                           | 0.7615941559557649    |
        | tanh    | neg_one     | -1                          | -0.7615941559557649   |
        | tanh    | half        | 0.5                         | 0.46211715726000974   |
        | tanh    | two         | 2                           | 0.9640275800758169    |
        | tanh    | neg_two     | -2                          | -0.9640275800758169   |
        | tanh    | neg_half    | -0.5                        | -0.46211715726000974  |
        | tanh    | nan         | CAST('NaN' AS DOUBLE)       | NaN                   |
        | tanh    | inf         | CAST('Infinity' AS DOUBLE)  | 1.0                   |
        | tanh    | neg_inf     | CAST('-Infinity' AS DOUBLE) | -1.0                  |
        | tanh    | neg_zero    | CAST(-0.0 AS DOUBLE)        | 0.0                   |
        | tanh    | null_double | CAST(NULL AS DOUBLE)        | NULL                  |
        | tanh    | null_int    | CAST(NULL AS INT)           | NULL                  |
        | tanh    | tinyint     | CAST(1 AS TINYINT)          | 0.7615941559557649    |
        | tanh    | float       | CAST(0.5 AS FLOAT)          | 0.46211715726000974   |
        | tanh    | dec_small   | CAST(-0.5 AS DECIMAL(10,2)) | -0.46211715726000974  |
        | tanh    | str_ok      | '0.5'                       | 0.46211715726000974   |
        | tanh    | str_padded  | ' 1 '                       | 0.7615941559557649    |

    # Last-digit differences: Java's StrictMath is fdlibm, Sail calls the platform libm.
    # Fixing them needs an fdlibm port, tracked as its own change. Only the rows that differ on
    # glibc (Linux) as well as on macOS are listed: the result of the others depends on the platform.
    @sail-bug
    Scenario Outline: <fn>(<input>) = <result> to the last digit (<what>)
      When query
        """
        SELECT <fn>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | fn    | what       | input                       | result              |
        | cosh  | one        | 1                           | 1.543080634815244   |
        | cosh  | neg_one    | -1                          | 1.543080634815244   |
        | cosh  | tinyint    | CAST(1 AS TINYINT)          | 1.543080634815244   |
        | cosh  | str_padded | ' 1 '                       | 1.543080634815244   |
        | exp   | one        | 1                           | 2.7182818284590455  |
        | exp   | tinyint    | CAST(1 AS TINYINT)          | 2.7182818284590455  |
        | exp   | str_padded | ' 1 '                       | 2.7182818284590455  |
        | log1p | two        | 2                           | 1.0986122886681096  |

  Rule: String argument under ANSI=true

    # Cast-level gap shared with `CAST('abc' AS DOUBLE)`: no [CAST_INVALID_INPUT] class.
    @sail-bug
    Scenario Outline: <fn> of a malformed string under ANSI=true is CAST_INVALID_INPUT
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <fn>('abc') AS result
        """
      Then query error \[CAST_INVALID_INPUT\]

      Examples:
        | fn      |
        | acos    |
        | acosh   |
        | asin    |
        | asinh   |
        | atan    |
        | atanh   |
        | cbrt    |
        | cos     |
        | cosh    |
        | cot     |
        | csc     |
        | degrees |
        | exp     |
        | ln      |
        | log10   |
        | log1p   |
        | log2    |
        | radians |
        | sec     |
        | sin     |
        | sinh    |
        | tan     |
        | tanh    |

    # Spark parses with Double.valueOf, which accepts a type suffix and hex floats; Sail's cast does not.
    @sail-bug
    Scenario Outline: ln of a string Java parses as a double: <input>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT ln('<input>') AS result
        """
      Then query result
        | result |
        | 0.0    |

      Examples:
        | input |
        | 1d    |
        | 1f    |
        | 1D    |

  @function(nullability)
  Rule: Output schema is always a nullable double

    Scenario Outline: <fn> output schema is a nullable double (<form>)
      When query
        """
        <query>
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

      Examples:
        | fn      | form            | query                                                   |
        | acos    | literal         | SELECT acos(1) AS result                                |
        | acos    | non-null column | SELECT acos(CAST(id AS INT)) AS result FROM range(3)    |
        | acos    | string          | SELECT acos('0.5') AS result                            |
        | acosh   | literal         | SELECT acosh(1) AS result                               |
        | acosh   | non-null column | SELECT acosh(CAST(id AS INT)) AS result FROM range(3)   |
        | acosh   | string          | SELECT acosh('0.5') AS result                           |
        | asin    | literal         | SELECT asin(1) AS result                                |
        | asin    | non-null column | SELECT asin(CAST(id AS INT)) AS result FROM range(3)    |
        | asin    | string          | SELECT asin('0.5') AS result                            |
        | asinh   | literal         | SELECT asinh(1) AS result                               |
        | asinh   | non-null column | SELECT asinh(CAST(id AS INT)) AS result FROM range(3)   |
        | asinh   | string          | SELECT asinh('0.5') AS result                           |
        | atan    | literal         | SELECT atan(1) AS result                                |
        | atan    | non-null column | SELECT atan(CAST(id AS INT)) AS result FROM range(3)    |
        | atan    | string          | SELECT atan('0.5') AS result                            |
        | atanh   | literal         | SELECT atanh(1) AS result                               |
        | atanh   | non-null column | SELECT atanh(CAST(id AS INT)) AS result FROM range(3)   |
        | atanh   | string          | SELECT atanh('0.5') AS result                           |
        | cbrt    | literal         | SELECT cbrt(1) AS result                                |
        | cbrt    | non-null column | SELECT cbrt(CAST(id AS INT)) AS result FROM range(3)    |
        | cbrt    | string          | SELECT cbrt('0.5') AS result                            |
        | cos     | literal         | SELECT cos(1) AS result                                 |
        | cos     | non-null column | SELECT cos(CAST(id AS INT)) AS result FROM range(3)     |
        | cos     | string          | SELECT cos('0.5') AS result                             |
        | cosh    | literal         | SELECT cosh(1) AS result                                |
        | cosh    | non-null column | SELECT cosh(CAST(id AS INT)) AS result FROM range(3)    |
        | cosh    | string          | SELECT cosh('0.5') AS result                            |
        | cot     | literal         | SELECT cot(1) AS result                                 |
        | cot     | non-null column | SELECT cot(CAST(id AS INT)) AS result FROM range(3)     |
        | cot     | string          | SELECT cot('0.5') AS result                             |
        | csc     | literal         | SELECT csc(1) AS result                                 |
        | csc     | non-null column | SELECT csc(CAST(id AS INT)) AS result FROM range(3)     |
        | csc     | string          | SELECT csc('0.5') AS result                             |
        | degrees | literal         | SELECT degrees(1) AS result                             |
        | degrees | non-null column | SELECT degrees(CAST(id AS INT)) AS result FROM range(3) |
        | degrees | string          | SELECT degrees('0.5') AS result                         |
        | exp     | literal         | SELECT exp(1) AS result                                 |
        | exp     | non-null column | SELECT exp(CAST(id AS INT)) AS result FROM range(3)     |
        | exp     | string          | SELECT exp('0.5') AS result                             |
        | ln      | literal         | SELECT ln(1) AS result                                  |
        | ln      | non-null column | SELECT ln(CAST(id AS INT)) AS result FROM range(3)      |
        | ln      | string          | SELECT ln('0.5') AS result                              |
        | log10   | literal         | SELECT log10(1) AS result                               |
        | log10   | non-null column | SELECT log10(CAST(id AS INT)) AS result FROM range(3)   |
        | log10   | string          | SELECT log10('0.5') AS result                           |
        | log1p   | literal         | SELECT log1p(1) AS result                               |
        | log1p   | non-null column | SELECT log1p(CAST(id AS INT)) AS result FROM range(3)   |
        | log1p   | string          | SELECT log1p('0.5') AS result                           |
        | log2    | literal         | SELECT log2(1) AS result                                |
        | log2    | non-null column | SELECT log2(CAST(id AS INT)) AS result FROM range(3)    |
        | log2    | string          | SELECT log2('0.5') AS result                            |
        | radians | literal         | SELECT radians(1) AS result                             |
        | radians | non-null column | SELECT radians(CAST(id AS INT)) AS result FROM range(3) |
        | radians | string          | SELECT radians('0.5') AS result                         |
        | sec     | literal         | SELECT sec(1) AS result                                 |
        | sec     | non-null column | SELECT sec(CAST(id AS INT)) AS result FROM range(3)     |
        | sec     | string          | SELECT sec('0.5') AS result                             |
        | sin     | literal         | SELECT sin(1) AS result                                 |
        | sin     | non-null column | SELECT sin(CAST(id AS INT)) AS result FROM range(3)     |
        | sin     | string          | SELECT sin('0.5') AS result                             |
        | sinh    | literal         | SELECT sinh(1) AS result                                |
        | sinh    | non-null column | SELECT sinh(CAST(id AS INT)) AS result FROM range(3)    |
        | sinh    | string          | SELECT sinh('0.5') AS result                            |
        | tan     | literal         | SELECT tan(1) AS result                                 |
        | tan     | non-null column | SELECT tan(CAST(id AS INT)) AS result FROM range(3)     |
        | tan     | string          | SELECT tan('0.5') AS result                             |
        | tanh    | literal         | SELECT tanh(1) AS result                                |
        | tanh    | non-null column | SELECT tanh(CAST(id AS INT)) AS result FROM range(3)    |
        | tanh    | string          | SELECT tanh('0.5') AS result                            |

  Rule: The mismatch message names the function as Spark does

    Scenario Outline: <fn> of a BOOLEAN names <shown> in the message
      When query
        """
        SELECT <fn>(true) AS result
        """
      Then query error Cannot resolve "<shown>\(true\)" due to data type mismatch: The first parameter requires the "DOUBLE" type, however "true" has the type "BOOLEAN"

      Examples:
        | fn    | shown      |
        | acosh | ACOSH      |
        | sin   | SIN        |
        | ln    | ln         |

  Rule: An untyped NULL is cast to DOUBLE

    Scenario Outline: <fn>(NULL) is NULL
      When query
        """
        SELECT <fn>(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | fn    |
        | acos  |
        | acosh |
        | ln    |
        | log2  |
        | sin   |
        | csc   |

  Rule: A real negative zero

    Scenario Outline: <fn>(-0.0D) = <result>
      When query
        """
        SELECT <fn>(-CAST(0.0 AS DOUBLE)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | fn      | result    |
        | asin    | -0.0      |
        | asinh   | -0.0      |
        | atan    | -0.0      |
        | atanh   | -0.0      |
        | cbrt    | -0.0      |
        | cot     | -Infinity |
        | csc     | -Infinity |
        | degrees | -0.0      |
        | radians | -0.0      |
        | sec     | 1.0       |
        | sin     | -0.0      |
        | sinh    | -0.0      |
        | tan     | -0.0      |
        | tanh    | -0.0      |

  Rule: A typed NULL keeps its Spark type

    Scenario Outline: <fn> of a NULL typed as <type> is DATATYPE_MISMATCH
      When query
        """
        SELECT <fn>(CAST(NULL AS <type>)) AS result
        """
      Then query error \[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE\]

      Examples:
        | fn   | type       |
        | sin  | BOOLEAN    |
        | sin  | DATE       |
        | sin  | ARRAY<INT> |
        | sqrt | BOOLEAN    |
        | ln   | BOOLEAN    |

    Scenario Outline: <fn> of a NULL typed as <type> is NULL
      When query
        """
        SELECT <fn>(CAST(NULL AS <type>)) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | fn   | type          |
        | sin  | STRING        |
        | sin  | DECIMAL(5,2)  |
        | sqrt | STRING        |
        | ln   | STRING        |

  Rule: The error names the function and the argument the way Spark does

    Scenario Outline: <call> keeps the function name Spark prints
      When query
        """
        SELECT <call> AS result
        """
      Then query error Cannot resolve "<shown>\(true\)" due to data type mismatch: The first parameter requires the "DOUBLE" type, however "true" has the type "BOOLEAN"

      Examples:
        | call       | shown |
        | SIN(true)  | SIN   |
        | Sqrt(true) | SQRT  |

    Scenario: the function name in the arity error is the registered one
      When query
        """
        SELECT ACOS(1, 2) AS result
        """
      Then query error The `acos` requires 1 parameters but the actual number is 2

    # Spark shows the attribute without its qualifier: `SIN(f)` for `sin(t.f)`.
    Scenario: a qualified column is named without its qualifier
      When query
        """
        SELECT sin(t.f) AS result FROM VALUES (true) AS t(f)
        """
      Then query error Cannot resolve "SIN\(f\)" due to data type mismatch: The first parameter requires the "DOUBLE" type, however "f" has the type "BOOLEAN"

  Rule: Argument shapes other than a literal

    Scenario: qualified columns of a join that has the same name on both sides
      When query
        """
        SELECT sqrt(l.b) AS lb, sqrt(r.b) AS rb
        FROM (SELECT 1 AS a, 0.5 AS b) l JOIN (SELECT 1 AS a, 0.25 AS b) r ON l.a = r.a
        """
      Then query result
        | lb                 | rb  |
        | 0.7071067811865476 | 0.5 |

    Scenario: a qualified boolean column is rejected
      When query
        """
        SELECT sin(t.f) AS result FROM VALUES (true) AS t(f)
        """
      Then query error \[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE\]

    Scenario: a column of strings with a malformed value and a NULL under ANSI=false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT sqrt(c) AS result FROM VALUES ('0.25'), ('abc'), (NULL) AS t(c)
        """
      Then query result
        | result |
        | 0.5    |
        | NULL   |
        | NULL   |

    Scenario Outline: an element of a <container>
      When query
        """
        SELECT sqrt(<value>) AS result
        """
      Then query result
        | result |
        | 0.5    |

      Examples:
        | container | value                              |
        | array     | array(0.25D)[0]                    |
        | map       | map('k', 0.25D)['k']               |
        | struct    | named_struct('x', 0.25D).x         |

    Scenario: a lambda variable that is a string
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT transform(array('0.25', 'abc'), x -> sqrt(x)) AS result
        """
      Then query result
        | result      |
        | [0.5, NULL] |

    Scenario: a lambda variable that is a boolean is rejected
      When query
        """
        SELECT transform(array(true), x -> sin(x)) AS result
        """
      Then query error \[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE\]

    Scenario Outline: <fn> of a <type> value inside a container is rejected
      When query
        """
        SELECT <fn>(<value>) AS result
        """
      Then query error \[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE\]

      Examples:
        | fn  | type   | value                     |
        | sin | array  | array(1.0D)               |
        | sin | struct | named_struct('x', 0.5D)   |
        | sin | map    | map('k', 0.5D)            |

    Scenario Outline: <fn> of a <type> is rejected
      When query
        """
        SELECT <fn>(<value>) AS result
        """
      Then query error \[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE\]

      Examples:
        | fn  | type              | value                                |
        | sin | TIMESTAMP_NTZ     | TIMESTAMP_NTZ'2024-01-02 03:04:05'   |
        | sin | DAY-TIME INTERVAL | INTERVAL '1' DAY                     |

  @spark-4
  Rule: A VARIANT argument is rejected

    Scenario: sin of a VARIANT
      When query
        """
        SELECT sin(parse_json('1')) AS result
        """
      Then query error \[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE\]

  @spark-4.1
  Rule: A TIME argument is rejected

    Scenario: sin of a TIME
      When query
        """
        SELECT sin(TIME'10:00:00') AS result
        """
      Then query error \[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE\]

  Rule: log2 of an exact power of two is exact

    # Deliberate difference. Spark computes `StrictMath.log(x) / StrictMath.log(2)`
    # (mathExpressions.scala, `Log2`), which rounds away from the exact result: the Spark 4.2.0 JVM
    # gives 29.000000000000004 for 2^29 and 31.000000000000004 for 2^31. Sail returns the exact log2.
    @sail-only
    Scenario Outline: log2(<input>) = <result>
      When query
        """
        SELECT log2(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | input                         | result |
        | 536870912                     | 29.0   |
        | CAST(2147483648 AS DOUBLE)    | 31.0   |
        | 8                             | 3.0    |
