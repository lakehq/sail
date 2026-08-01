@csv @to_csv
Feature: to_csv converts a struct value to a CSV string

  Rule: Basic serialization

    Scenario Outline: Basic: <case>
      When query
        """
        SELECT to_csv(named_struct(<args>))
        """
      Then query result
        | to_csv(named_struct(<name>)) |
        | <result>                     |

      Examples:
        | case                                     | args                        | name                  | result     |
        | Convert a struct with two integer fields | 'a', 1, 'b', 2              | a, 1, b, 2            | 1,2        |
        | Convert a struct with mixed types        | 'a', 1, 'b', 2.5, 'c', true | a, 1, b, 2.5, c, true | 1,2.5,true |

    Scenario: Convert a struct from a table column
      When query
        """
        SELECT to_csv(named_struct('a', value, 'b', value))
        FROM VALUES (1), (2), (3) AS t(value)
        ORDER BY value
        """
      Then query result
        | to_csv(named_struct(a, value, b, value)) |
        | 1,1                                      |
        | 2,2                                      |
        | 3,3                                      |

  Rule: NULL handling

    @sail-bug
    Scenario: NULL struct returns NULL
      # The value matches Spark, but Sail's column name diverges: Spark folds the NULL cast to
      # `to_csv(NULL)` in the display name (verified against JVM 4.2), while Sail keeps the full
      # `CAST(NULL AS STRUCT<...>)`.
      When query
        """
        SELECT to_csv(CAST(NULL AS STRUCT<a:INT, b:INT>))
        """
      Then query result
        | to_csv(NULL) |
        | NULL         |

    Scenario: Struct with NULL field serializes null field as empty string
      When query
        """
        SELECT to_csv(named_struct('a', 1, 'b', CAST(NULL AS INT)))
        """
      Then query result
        | to_csv(named_struct(a, 1, b, CAST(NULL AS INT))) |
        | 1,                                               |

  Rule: Separator options

    # Kept separate: both the SQL and the expected value contain `|`, which a
    # data-table cell would re-escape.
    Scenario: Custom separator via sep option
      When query
        """
        SELECT to_csv(named_struct('a', 1, 'b', 2), map('sep', '|'))
        """
      Then query result
        | to_csv(named_struct(a, 1, b, 2)) |
        | 1\|2                             |

    Scenario: Custom separator via delimiter option
      When query
        """
        SELECT to_csv(named_struct('a', 1, 'b', 2), map('delimiter', '|'))
        """
      Then query result
        | to_csv(named_struct(a, 1, b, 2)) |
        | 1\|2                             |

  Rule: CSV writer options

    Scenario: Quote fields containing separators and newlines
      When query
        """
        SELECT regexp_replace(
          to_csv(named_struct(
            'a', 'hello,world',
            'b', concat('line', chr(10), 'break')
          )),
          chr(10),
          '<LF>'
        ) AS result
        """
      Then query result
        | result                        |
        | "hello,world","line<LF>break" |

    Scenario Outline: Writer option: <case>
      When query
        """
        SELECT to_csv(named_struct(<fields>), map(<opts>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                                      | fields                              | opts                                  | result       |
        | Custom escape character is used for quote escaping                        | 'a', 'say "hi"'                     | 'escape', '#'                         | "say #"hi#"" |
        | escapeQuotes false does not quote a field only because it contains quotes | 'a', 'say "hi"'                     | 'escapeQuotes', 'false'               | say "hi"     |
        | quoteAll quotes every non-null field                                      | 'a', 'x', 'b', 'y'                  | 'quoteAll', 'true'                    | "x","y"      |
        | nullValue and emptyValue options customize null and empty string output   | 'a', CAST(NULL AS STRING), 'b', ''  | 'nullValue', '-', 'emptyValue', '_'   | -,_          |
        | quoteAll quotes null replacement fields                                   | 'a', CAST(NULL AS STRING), 'b', 'x' | 'quoteAll', 'true'                    | "","x"       |
        | nullValue containing separator is quoted                                  | 'a', CAST(NULL AS STRING), 'b', 'x' | 'nullValue', ','                      | ",",x        |
        | quoteAll quotes custom emptyValue fields                                  | 'a', '', 'b', 'x'                   | 'emptyValue', '_', 'quoteAll', 'true' | "_","x"      |
        | emptyValue containing separator is quoted                                 | 'a', '', 'b', 'x'                   | 'emptyValue', ','                     | ",",x        |
        | Literal escape characters are escaped in quoted fields                    | 'a', 'a#b,c'                        | 'escape', '#'                         | "a##b,c"     |
        | Custom quote character is used                                            | 'a', 'x'                            | 'quote', '#', 'quoteAll', 'true'      | #x#          |

  Rule: Timestamp formatting

    Scenario Outline: Timestamp: <case>
      When query
        """
        SELECT to_csv(named_struct(<args>)<opts>)
        """
      Then query result
        | to_csv(named_struct(<name>)) |
        | <result>                     |

      Examples:
        | case                                                    | args                                             | opts                                   | name                                       | result                   |
        | Timestamp field uses default ISO 8601 UTC format        | 'ts', to_timestamp('2015-08-26', 'yyyy-MM-dd')   |                                        | ts, to_timestamp(2015-08-26, yyyy-MM-dd)   | 2015-08-26T00:00:00.000Z |
        | Pre-epoch timestamp is formatted correctly              | 'ts', to_timestamp('1969-12-31', 'yyyy-MM-dd')   |                                        | ts, to_timestamp(1969-12-31, yyyy-MM-dd)   | 1969-12-31T00:00:00.000Z |
        | Custom timestampFormat option changes the output format | 'time', to_timestamp('2015-08-26', 'yyyy-MM-dd') | , map('timestampFormat', 'dd/MM/yyyy') | time, to_timestamp(2015-08-26, yyyy-MM-dd) | 26/08/2015               |

  Rule: Date formatting

    Scenario Outline: Date: <case>
      When query
        """
        SELECT to_csv(named_struct(<args>)<opts>)
        """
      Then query result
        | to_csv(named_struct(<name>)) |
        | <result>                     |

      Examples:
        | case                                               | args                   | opts                              | name                 | result     |
        | Date field uses default yyyy-MM-dd format          | 'd', DATE '2015-08-26' |                                   | d, DATE '2015-08-26' | 2015-08-26 |
        | Pre-epoch date is formatted correctly              | 'd', DATE '1969-12-31' |                                   | d, DATE '1969-12-31' | 1969-12-31 |
        | Custom dateFormat option changes the output format | 'd', DATE '2015-08-26' | , map('dateFormat', 'dd/MM/yyyy') | d, DATE '2015-08-26' | 26/08/2015 |

  Rule: Decimal formatting

    Scenario: Decimal field is formatted as fixed-point string
      When query
        """
        SELECT to_csv(named_struct('price', CAST(9.99 AS DECIMAL(5,2))))
        """
      Then query result
        | to_csv(named_struct(price, CAST(9.99 AS DECIMAL(5,2)))) |
        | 9.99                                                    |

    @sail-bug
    Scenario: Negative decimal preserves sign including fractional-only values
      # The value matches Spark; the column name diverges: Spark writes `-0.99` (verified against
      # JVM 4.2) while Sail renders the negative literal as `(- 0.99)`.
      When query
        """
        SELECT to_csv(named_struct('price', CAST(-0.99 AS DECIMAL(5,2))))
        """
      Then query result
        | to_csv(named_struct(price, CAST(-0.99 AS DECIMAL(5,2)))) |
        | -0.99                                                   |

  Rule: Complex and special values

    Scenario Outline: Complex value: <case>
      When query
        """
        SELECT to_csv(named_struct(<args>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                          | args                                                                                                | result                 |
        | Non-null complex values are formatted as Spark pretty strings | 'a', array(1, 2, CAST(NULL AS INT)), 'm', map('x', 1), 's', named_struct('b', 2)                    | "[1, 2,]",{x -> 1},{2} |
        | Floating-point special values use Spark display strings       | 'nan', CAST('NaN' AS DOUBLE), 'pos', CAST('Infinity' AS DOUBLE), 'neg', CAST('-Infinity' AS DOUBLE) | NaN,Infinity,-Infinity |
        | Binary values use Spark hex pretty strings                    | 'b', CAST('abc' AS BINARY)                                                                          | [61 62 63]             |

  @spark_null
  Rule: Output schema

    Scenario: a non-null struct literal is nullable (to_csv is inherently nullable in Spark)
      When query
        """
        SELECT to_csv(struct(1, 'a')) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null struct column is nullable (to_csv is inherently nullable in Spark)
      When query
        """
        SELECT to_csv(struct(id, id)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable struct column stays nullable
      When query
        """
        SELECT to_csv(c) AS result FROM VALUES (named_struct('a', 1)), (CAST(NULL AS STRUCT<a:INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
