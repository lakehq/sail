Feature: format_string function

  Rule: format_string retains the physical value of ANSI intervals

    # Spark's FormatString forwards values as AnyDataType to java.util.Formatter. A day-time
    # interval is therefore its microsecond LONG, rather than the qualified SQL string used by
    # functions that first coerce their arguments to STRING.
    Scenario: formatting a day-time interval with percent-s uses microseconds
      When query
        """
        SELECT format_string('%s', INTERVAL '2' HOUR) AS result
        """
      Then query result
        | result     |
        | 7200000000 |

    # Spark registers `printf` as an alias of FormatString, so it receives the same physical
    # interval value as `format_string`.
    Scenario: printf forwards the physical value like format_string
      When query
        """
        SELECT printf('%s', INTERVAL '2' HOUR) AS result
        """
      Then query result
        | result     |
        | 7200000000 |

    # A year-month interval reaches FormatString as its physical month count.
    Scenario Outline: formatting <case> uses its month count
      When query
        """
        SELECT format_string('%s', <input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case               | input               | result |
        | a month interval   | INTERVAL '2' MONTH  | 2      |
        | a year interval    | INTERVAL '2' YEAR   | 24     |

    # The same rule holds for every argument shape, so these guard the day-time leaf that already
    # works: the conversion is of the physical value, never of the rendered SQL string.
    Scenario Outline: formatting <case> forwards the physical value
      When query
        """
        SELECT format_string('<format>', <input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                        | format | input                                 | result           |
        | a day-time with percent-d   | %d     | INTERVAL '2' HOUR                     | 7200000000       |
        | a fractional second         | %s     | INTERVAL '1.5' SECOND                 | 1500000          |
        | a negative day-time         | %s     | INTERVAL '-2' HOUR                    | -7200000000      |
        | a date                      | %s     | DATE'2020-01-02'                      | 18263            |
        | a timestamp                 | %s     | TIMESTAMP'2020-01-02 03:04:05'        | 1577934245000000 |
        | a naive timestamp           | %s     | TIMESTAMP_NTZ'2020-01-02 03:04:05'    | 1577934245000000 |
        | a boolean                   | %s     | true                                  | true             |

    # A non-foldable argument takes the columnar path, which is a different kernel from the
    # constant-folded one above.
    Scenario: formatting a day-time interval column forwards the physical value
      When query
        """
        SELECT format_string('%s', v) AS result FROM VALUES (INTERVAL '2' HOUR) AS t(v)
        """
      Then query result
        | result     |
        | 7200000000 |

    Scenario: formatting a NULL interval prints the null marker
      When query
        """
        SELECT format_string('%s', CAST(NULL AS INTERVAL HOUR)) AS result
        """
      Then query result
        | result |
        | null   |

  Rule: format_string forwards the physical value of every other type

    # Spark renders a DECIMAL through Decimal.toString(), preserving its declared scale.
    Scenario: formatting a decimal keeps its scale
      When query
        """
        SELECT format_string('%s', CAST(1.50 AS DECIMAL(5,2))) AS result
        """
      Then query result
        | result |
        | 1.50   |

    Scenario: formatting a decimal column keeps its scale
      When query
        """
        SELECT format_string('%s', value) AS result
        FROM VALUES (CAST(1.50 AS DECIMAL(5,2))) AS t(value)
        """
      Then query result
        | result |
        | 1.50   |

    # TODO: implement Formatter-compatible physical values for complex arguments. Spark receives
    # Java collection values here; Sail currently rejects Arrow collection scalars.
    @sail-bug
    Scenario Outline: formatting <case> uses Spark's internal rendering
      When query
        """
        SELECT format_string('%s', <input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case     | input                | result                 |
        | an array | array(1, 2)          | [1,2]                  |
        | a struct | named_struct('a', 1) | [1]                    |
        | a map    | map('a', 1)          | keys: [a], values: [1] |

    # BINARY has no pinnable expectation: Spark prints the Java array's identity hash
    # (`[B@7bb6161c`), which differs on every run, so this leaf is measured and left untested.

  Rule: format_string applies Java Formatter conversions to decimal text

    # Decimal.toString() preserves the scale before Formatter applies string precision and width.
    Scenario Outline: formatting a decimal with <format> applies string formatting
      When query
        """
        SELECT format_string('[<format>]', CAST(<input> AS DECIMAL(5,2))) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | format | input | result   |
        | %S     | 1.50  | [1.50]     |
        | %.1s   | 1.50  | [1]        |
        | %8s    | -1.50 | [   -1.50] |

    Scenario: formatting a null decimal prints the null marker
      When query
        """
        SELECT format_string('%s', CAST(NULL AS DECIMAL(5,2))) AS result
        """
      Then query result
        | result |
        | null   |

    Scenario: formatting a decimal with a column format keeps its scale
      When query
        """
        SELECT format_string(format, value) AS result
        FROM VALUES ('%s', CAST(1.50 AS DECIMAL(5,2))) AS t(format, value)
        """
      Then query result
        | result |
        | 1.50   |

    # java.util.Formatter rejects a Decimal for floating-point conversions.
    Scenario: formatting a decimal as a floating point value errors
      When query
        """
        SELECT format_string('%f', CAST(1.50 AS DECIMAL(5,2))) AS result
        """
      Then query error .*

    # java.util.Formatter prints true for every non-null Decimal; DataFusion only accepts BOOLEAN.
    # This is confirmed on main too.
    @sail-bug
    Scenario: formatting a decimal as a boolean reports that it is non-null
      When query
        """
        SELECT format_string('%b', CAST(1.50 AS DECIMAL(5,2))) AS result
        """
      Then query result
        | result |
        | true   |

    # Spark delegates %h to Decimal.hashCode(), while DataFusion's Formatter has not implemented
    # hexadecimal hash conversions (datafusion#17093). This is confirmed on main too.
    @sail-bug
    Scenario: formatting a decimal hash uses Decimal hashCode
      When query
        """
        SELECT format_string('%h', CAST(1.50 AS DECIMAL(5,2))) AS result
        """
      Then query result
        | result   |
        | 3fc00000 |
