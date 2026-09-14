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

    # TODO: Spark folds the sign before a postfix `::` (`-1::BOOLEAN` is `CAST(-1 AS BOOLEAN)`); Sail
    #  parses `-(1::BOOLEAN)` and refuses the unary minus. Already so on `main`.
    @sail-bug
    Scenario: a signed literal is cast as a whole
      When query
        """
        SELECT -1::BOOLEAN AS result
        """
      Then query result
        | result |
        | true   |

    # TODO: Sail refuses `ORDER BY -1` like Spark, but leaks the `Debug` of the sort order instead of
    #  `ORDER_BY_POS_OUT_OF_RANGE` (`AstBuilder.scala:7591`).
    @sail-bug
    Scenario: ORDER BY -1 names the position out of range
      When query
        """
        SELECT x FROM VALUES (1), (2) AS t(x) ORDER BY -1
        """
      Then query error ORDER BY position -1 is not in select list

    # `number: MINUS? BIGINT_LITERAL` makes `-1L` one BIGINT literal, and only an INT literal is an
    # ORDER BY ordinal (`AstBuilder.scala:7591`), so this sorts by a constant.
    Scenario: ORDER BY a negative BIGINT literal sorts by a constant
      When query
        """
        SELECT 7 AS v ORDER BY -1L
        """
      Then query result
        | v |
        | 7 |
