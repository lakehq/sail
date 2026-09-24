Feature: unary + and - operand types vs Spark 4.2.0

  # The arity-1 sibling of `arithmetic_operand_rejection.feature`. `UnaryMinus` and `UnaryPositive`
  # both declare `inputTypes = Seq(TypeCollection.NumericAndInterval)` (`arithmetic.scala:54,124`),
  # so Spark rejects every operand that is neither numeric nor an interval -- the same rule the
  # binary operators apply, one operand short.
  #
  # The full matrix is the same 28-token alphabet as the binary files, both operators, measured
  # against the JVM: 56 cells, of which 34 are accepted by both engines and 22 rejected by Spark.
  # Sail's `-` rejects the same 11 as Spark; its `+` accepts all of them, because the arity-1
  # branch of `spark_plus` returns the operand unchanged with no guard at all.
  #
  # ANSI is NOT an axis here, and that is measured rather than assumed: all 112 cells were run
  # under both modes and no verdict changes in either engine, unlike the binary files where ANSI
  # flips 16 string rows. Running one mode is therefore the whole contract.

  Rule: both unary operators accept a numeric, an interval or a string

    Scenario Outline: unary <op> accepts a <case> operand
      When query
        """
        SELECT typeof(<op>(<operand>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | op | case     | operand                        |
        | +  | tinyint  | CAST(2 AS TINYINT)             |
        | +  | smallint | CAST(2 AS SMALLINT)            |
        | +  | int      | CAST(2 AS INT)                 |
        | +  | bigint   | CAST(2 AS BIGINT)              |
        | +  | float    | CAST(2 AS FLOAT)               |
        | +  | double   | CAST(2 AS DOUBLE)              |
        | +  | dec      | CAST(2 AS DECIMAL(10,2))       |
        | +  | str      | '2'                            |
        | +  | null     | CAST(NULL AS INT)              |
        | +  | unull    | NULL                           |
        | +  | ival_y   | INTERVAL '2' YEAR              |
        | +  | ival_m   | INTERVAL '2' MONTH             |
        | +  | ival_ym  | INTERVAL '1-2' YEAR TO MONTH   |
        | +  | ival_d   | INTERVAL '2' DAY               |
        | +  | ival_dt  | INTERVAL '25' HOUR             |
        | +  | ival_ds  | INTERVAL '1 02:03:04' DAY TO SECOND |
        | +  | calendar | make_interval(0,1,0,1,0,0,0)   |
        | -  | tinyint  | CAST(2 AS TINYINT)             |
        | -  | smallint | CAST(2 AS SMALLINT)            |
        | -  | int      | CAST(2 AS INT)                 |
        | -  | bigint   | CAST(2 AS BIGINT)              |
        | -  | float    | CAST(2 AS FLOAT)               |
        | -  | double   | CAST(2 AS DOUBLE)              |
        | -  | dec      | CAST(2 AS DECIMAL(10,2))       |
        | -  | str      | '2'                            |
        | -  | null     | CAST(NULL AS INT)              |
        | -  | unull    | NULL                           |
        | -  | ival_y   | INTERVAL '2' YEAR              |
        | -  | ival_m   | INTERVAL '2' MONTH             |
        | -  | ival_ym  | INTERVAL '1-2' YEAR TO MONTH   |
        | -  | ival_d   | INTERVAL '2' DAY               |
        | -  | ival_dt  | INTERVAL '25' HOUR             |
        | -  | ival_ds  | INTERVAL '1 02:03:04' DAY TO SECOND |
        | -  | calendar | make_interval(0,1,0,1,0,0,0)   |

  Rule: unary - rejects everything else

    # Sail always refused these eleven; the message used to be DataFusion's `Failed to coerce
    # arguments to satisfy a call to 'negative' function`, with an Arrow `Debug` dump in it. The
    # operand is now judged before `negative` is reached, with the same guard as the unary `+`.
    Scenario Outline: unary minus rejects a <case> operand
      When query
        """
        SELECT -(<operand>) AS result
        """
      Then query error (?i)cannot resolve

      Examples:
        | case    | operand                                                                   |
        | bool    | true                                                                      |
        | bin     | CAST('2' AS BINARY)                                                       |
        | date    | DATE'2024-01-15'                                                          |
        | ts      | TIMESTAMP'2024-01-15 12:00:00'                                            |
        | ts_ntz  | TIMESTAMP_NTZ'2024-01-15 12:00:00'                                        |
        | time    | TIME '12:00:00'                                                           |
        | array   | array(1,2)                                                                |
        | struct  | named_struct('a',1)                                                       |
        | map     | map('a',1)                                                                |
        | variant | parse_json('{"a":1}')                                                     |
        | geom    | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |

  Rule: unary + rejects everything else

    # The arity-1 `+` used to be a bare identity, so each of these came back with the operand
    # unchanged -- a DATE, a BOOLEAN, an ARRAY answered where Spark fails analysis. It takes
    # `NumericAndInterval` (`arithmetic.scala:124`) like the unary `-`, and shares its guard.
    Scenario Outline: unary plus rejects a <case> operand
      When query
        """
        SELECT +(<operand>) AS result
        """
      Then query error (?i)cannot resolve

      Examples:
        | case    | operand                                                                   |
        | bool    | true                                                                      |
        | bin     | CAST('2' AS BINARY)                                                       |
        | date    | DATE'2024-01-15'                                                          |
        | ts      | TIMESTAMP'2024-01-15 12:00:00'                                            |
        | ts_ntz  | TIMESTAMP_NTZ'2024-01-15 12:00:00'                                        |
        | time    | TIME '12:00:00'                                                           |
        | array   | array(1,2)                                                                |
        | struct  | named_struct('a',1)                                                       |
        | map     | map('a',1)                                                                |
        | variant | parse_json('{"a":1}')                                                     |
        | geom    | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |

  Rule: the positive function is the unary +

    # `positive` is registered as `UnaryPositive` (`FunctionRegistry.scala:470`), the very expression
    # the unary `+` parses to, so it takes the same operands and promotes a STRING to a DOUBLE. As a
    # bare identity it answered a DATE, an ARRAY or a TIMESTAMP back unchanged.
    Scenario Outline: positive rejects a <case> operand with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT positive(<operand>) AS result
        """
      Then query error (?i)cannot resolve

      Examples:
        | case   | ansi  | operand                        |
        | bool   | false | true                           |
        | date   | false | DATE'2024-01-15'               |
        | ts     | true  | TIMESTAMP'2024-01-15 12:00:00' |
        | array  | false | array(1)                       |
        | map    | true  | map('a',1)                     |
        | struct | false | named_struct('a',1)            |
        | bin    | true  | CAST('2' AS BINARY)            |

    Scenario Outline: positive of <case> is <type>
      When query
        """
        SELECT typeof(positive(<operand>)) AS t
        """
      Then query result
        | t      |
        | <type> |

      Examples:
        | case         | operand                  | type                   |
        | an int       | CAST(2 AS INT)           | int                    |
        | a string     | '2'                      | double                 |
        | a NULL       | NULL                     | double                 |
        | a decimal    | CAST(2 AS DECIMAL(10,2)) | decimal(10,2)          |
        | an interval  | INTERVAL '1-2' YEAR TO MONTH | interval year to month |

  Rule: a negative number written as a literal keeps its literal type

    # The minus belongs to the NUMBER (`number: MINUS? INTEGER_VALUE`), so a postfix `::` casts the
    # SIGNED literal: `-1::BOOLEAN` is `CAST(-1 AS BOOLEAN)`. Parsing it as `-(1::BOOLEAN)` refused a
    # query Spark answers, and for a STRING it answered a DOUBLE instead of the cast.
    Scenario Outline: a signed literal is cast as a whole: <case>
      When query
        """
        SELECT <expression> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                     | expression              | result |
        | a boolean                | -1::BOOLEAN             | true   |
        | a boolean from zero      | -0::BOOLEAN             | false  |
        | a boolean from a bigint  | -1L::BOOLEAN            | true   |
        | a boolean from a decimal | -1.5::BOOLEAN           | true   |
        | a string                 | -1::STRING              | -1     |
        | a chain of casts         | -1::INT::STRING         | -1     |
        | a timestamp              | -1::TIMESTAMP = CAST(-1 AS TIMESTAMP) | true |

    Scenario: the cast of a signed literal keeps the target type
      When query
        """
        SELECT typeof(-1::STRING) AS t, typeof(-1::BOOLEAN) AS b
        """
      Then query result
        | t      | b       |
        | string | boolean |

    # The sign is folded by the GRAMMAR, so only a minus written right before the number counts: a
    # parenthesized operand, a unary plus, or anything that is not a plain literal keeps the unary
    # operator, and Spark refuses the ones whose operand is not numeric.
    Scenario Outline: <case> is not folded into the literal
      When query
        """
        SELECT <expression> AS result
        """
      Then query error (?i)cannot resolve

      Examples:
        | case             | expression      |
        | a parenthesized 1 | -(1::BOOLEAN)  |
        | a unary plus      | +1::BOOLEAN    |

    Scenario: an operand that is not a literal keeps the unary minus
      When query
        """
        SELECT -CAST(1 AS INT)::STRING AS a, 2 -1::STRING AS b
        """
      Then query result
        | a    | b |
        | -1.0 | 1 |

    # Every INT literal is an ordinal (`SubstituteUnresolvedOrdinals`), and `Analyzer.scala:2157-2161`
    # takes it only when `index > 0 && index <= child.output.size`; otherwise
    # `orderByPositionRangeError` names the index it READ, sign included
    # (`QueryCompilationErrors.scala:698-705`). Folding the sign into the literal is what brings a
    # negative one down this path, so the position has to keep its sign to be named.
    Scenario Outline: ORDER BY <position> names the position out of range
      When query
        """
        SELECT x, x + 1 AS y FROM VALUES (1), (2) AS t(x) ORDER BY <position>
        """
      Then query error ORDER BY position <position> is not in select list

      Examples:
        | position |
        | -1       |
        | -2       |
        | 0        |
        | 3        |

    Scenario: SORT BY a negative position names it the same way
      When query
        """
        SELECT x, x + 1 AS y FROM VALUES (1), (2) AS t(x) SORT BY -1
        """
      Then query error ORDER BY position -1 is not in select list

    Scenario: a position in range still sorts by that column
      When query
        """
        SELECT x, 10 - x AS y FROM VALUES (1), (2) AS t(x) ORDER BY 2
        """
      Then query result ordered
        | x | y |
        | 2 | 8 |
        | 1 | 9 |

    # Only an INT literal is an ordinal (`TryExtractOrdinal.scala:30-34`, `AstBuilder.scala:7591`),
    # so a BIGINT is a constant to sort by: it neither picks a column nor names a position out of
    # range, whatever its value.
    Scenario: ORDER BY a negative BIGINT literal sorts by a constant
      When query
        """
        SELECT 7 AS v ORDER BY -1L
        """
      Then query result
        | v |
        | 7 |

    # Only INT literals are ordinals (TryExtractOrdinal.scala:30-34 and
    # literals.scala:338-342). Other integral literals are constant grouping keys.
    # This test pins rejection; exact error wording is a separate parity concern.
    Scenario Outline: GROUP BY a non-INT literal rejects an ungrouped column: <literal>
      When query
        """
        SELECT x, count(*) AS c FROM VALUES (1), (2) AS t(x) GROUP BY <literal>
        """
      Then query error (?s).+

      Examples:
        | literal |
        | 1L      |
        | 0L      |
        | -1L     |
        | 3L      |
        | 1Y      |
        | 1S      |

    Scenario Outline: GROUP BY a non-INT literal accepts an aggregate: <literal>
      When query
        """
        SELECT count(*) AS c FROM VALUES (1), (2) AS t(x) GROUP BY <literal>
        """
      Then query result
        | c |
        | 2 |

      Examples:
        | literal |
        | 1L      |
        | 0L      |
        | -1L     |
        | 3L      |
        | 1Y      |
        | 1S      |

    Scenario: GROUP BY an INT literal still selects the projection
      When query
        """
        SELECT x, count(*) AS c FROM VALUES (1), (2) AS t(x) GROUP BY 1
        """
      Then query result
        | x | c |
        | 1 | 1 |
        | 2 | 1 |

    Scenario Outline: ORDER BY <position> sorts by a constant, not by a position
      When query
        """
        SELECT x FROM VALUES (2), (1) AS t(x) ORDER BY <position>
        """
      Then query result ordered
        | x |
        | 2 |
        | 1 |

      Examples:
        | position   |
        | 1L         |
        | 5L         |
        | 2147483648 |
