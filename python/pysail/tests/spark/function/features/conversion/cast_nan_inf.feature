Feature: CAST and type constructors with NaN and Infinity (issue #630)

  Rule: FLOAT type constructor

    Scenario Outline: FLOAT constructor: <case>
      When query
        """
        SELECT FLOAT(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                     | arg         | result    |
        | FLOAT NaN                | 'NAN'       | NaN       |
        | FLOAT NaN lowercase      | 'nan'       | NaN       |
        | FLOAT NaN mixed case     | 'Nan'       | NaN       |
        | FLOAT negative NaN       | '-NaN'      | NaN       |
        | FLOAT Infinity           | 'Infinity'  | Infinity  |
        | FLOAT negative Infinity  | '-Infinity' | -Infinity |
        | FLOAT Infinity lowercase | 'infinity'  | Infinity  |
        | FLOAT normal value       | '42'        | 42.0      |

    Scenario: FLOAT NaN with spaces
      When query
        """
        SELECT FLOAT(' NaN ') AS result
        """
      Then query result
        | result |
        | NaN    |

  Rule: DOUBLE type constructor

    Scenario Outline: DOUBLE constructor: <case>
      When query
        """
        SELECT DOUBLE(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                               | arg         | result    |
        | DOUBLE NaN                         | 'NAN'       | NaN       |
        | DOUBLE Infinity uppercase          | 'INFINITY'  | Infinity  |
        | DOUBLE negative Infinity uppercase | '-INFINITY' | -Infinity |
        | DOUBLE normal value                | '3.14'      | 3.14      |

    Scenario: DOUBLE Infinity with spaces
      When query
        """
        SELECT DOUBLE(' Infinity ') AS result
        """
      Then query result
        | result   |
        | Infinity |

  Rule: CAST to FLOAT/DOUBLE

    Scenario Outline: CAST: <case>
      When query
        """
        SELECT CAST('NaN' AS <type>) AS result
        """
      Then query result
        | result |
        | NaN    |

      Examples:
        | case               | type   |
        | CAST NaN to FLOAT  | FLOAT  |
        | CAST NaN to DOUBLE | DOUBLE |

  Rule: Integer types reject NaN and Infinity

    Scenario Outline: Integer rejects: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query error .*

      Examples:
        | case                      | expr                         |
        | INT NaN errors            | INT('NAN')                   |
        | CAST NaN to INT errors    | CAST('NaN' AS INT)           |
        | INT Infinity errors       | INT('Infinity')              |
        | BIGINT NaN errors         | BIGINT('NaN')                |
        | SMALLINT NaN errors       | SMALLINT('NaN')              |
        | TINYINT NaN errors        | TINYINT('NaN')               |
        | DECIMAL NaN errors        | CAST('NaN' AS DECIMAL(10,2)) |
        | INT invalid string errors | INT('hello')                 |

  Rule: TRY_CAST with NaN

    Scenario Outline: TRY_CAST: <case>
      When query
        """
        SELECT TRY_CAST('NaN' AS <type>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                              | type  | result |
        | TRY_CAST NaN to INT returns NULL  | INT   | NULL   |
        | TRY_CAST NaN to FLOAT returns NaN | FLOAT | NaN    |

    # A DOUBLE/FLOAT NaN source cast to TIMESTAMP takes a different resolver arm than a
    # STRING source (the numeric-to-timestamp path multiplies by a microsecond unit and
    # NaN-guards the result), so it needs its own scenario, under both ANSI settings since
    # TRY_CAST must return NULL regardless of `spark.sql.ansi.enabled`.
    Scenario Outline: TRY_CAST NaN DOUBLE to TIMESTAMP returns NULL: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT TRY_CAST(CAST('NaN' AS DOUBLE) AS TIMESTAMP) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case      | ansi  |
        | ANSI off  | false |
        | ANSI on   | true  |

    # `doubleToTimestampAnsi` (DateTimeUtils.scala:74-80) throws `CAST_INVALID_INPUT`
    # for NaN/Infinite under ANSI, rather than returning NULL (only TRY_CAST/non-ANSI
    # return NULL for this pair). Verified against the Spark 4.2 JVM.
    Scenario: CAST NaN DOUBLE to TIMESTAMP raises CAST_INVALID_INPUT under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(CAST('NaN' AS DOUBLE) AS TIMESTAMP) AS result
        """
      Then query error CAST_INVALID_INPUT

    # A finite DOUBLE that overflows once multiplied by MICROS_PER_SECOND throws
    # `CAST_OVERFLOW` under ANSI (from the intermediate Double->Long conversion,
    # `DoubleExactNumeric.toLong`, numerics.scala:168-174) rather than saturating
    # (only the non-ANSI path saturates). Verified against the Spark 4.2 JVM.
    Scenario: CAST an overflowing DOUBLE to TIMESTAMP raises CAST_OVERFLOW under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(CAST('1e20' AS DOUBLE) AS TIMESTAMP) AS result
        """
      Then query error CAST_OVERFLOW

  Rule: NaN arithmetic

    Scenario Outline: NaN arithmetic: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                   | expr                        | result |
        | NaN plus number is NaN                 | FLOAT('NaN') + 1            | NaN    |
        | NaN equals NaN is true in Spark        | FLOAT('NaN') = FLOAT('NaN') | true   |
        | NaN greater than zero is true in Spark | FLOAT('NaN') > 0            | true   |

  Rule: Multi-row with NaN and Infinity

    Scenario: VALUES with FLOAT NaN Infinity and NULL
      When query
        """
        SELECT * FROM VALUES (FLOAT('NaN')), (FLOAT('Infinity')), (FLOAT('-Infinity')), (NULL), (0.0), (1.5) AS t(v)
        """
      Then query result
        | v         |
        | NaN       |
        | Infinity  |
        | -Infinity |
        | NULL      |
        | 0.0       |
        | 1.5       |

    Scenario: VALUES with DOUBLE NaN Infinity and NULL
      When query
        """
        SELECT * FROM VALUES (DOUBLE('NaN')), (DOUBLE('Infinity')), (DOUBLE('-Infinity')), (NULL), (0.0) AS t(v)
        """
      Then query result
        | v         |
        | NaN       |
        | Infinity  |
        | -Infinity |
        | NULL      |
        | 0.0       |
