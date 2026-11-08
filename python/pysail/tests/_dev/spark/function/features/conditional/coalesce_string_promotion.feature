Feature: coalesce promotes STRING values like Spark

  With ANSI mode, a STRING value is promoted to the type of the other values (BIGINT for
  integral values, DOUBLE for fractional values, BOOLEAN, or BINARY). Without ANSI mode, numeric
  values are promoted to STRING, and BOOLEAN values with STRING values are rejected.

  Rule: ANSI mode

    Background:
      Given config spark.sql.ansi.enabled = true

    Scenario Outline: <function> of STRING and <case> values (ANSI)
      When query
        """
        SELECT <expression> AS result, typeof(<expression>) AS result_type
        FROM VALUES (<values>) AS t(s, v)
        """
      Then query result
        | result   | result_type |
        | <result> | <type>      |

      Examples:
        | function | case    | values                               | expression     | result | type    |
        | coalesce | INT     | '7', 1                               | coalesce(s, v) | 7      | bigint  |
        | coalesce | INT     | CAST(NULL AS STRING), 1              | coalesce(s, v) | 1      | bigint  |
        | nvl      | INT     | ' 7 ', 2                             | nvl(s, v)      | 7      | bigint  |
        | coalesce | BIGINT  | '7', 1L                              | coalesce(v, s) | 1      | bigint  |
        | coalesce | DOUBLE  | '2', 1.5D                            | coalesce(s, v) | 2.0    | double  |
        | coalesce | DOUBLE  | CAST(NULL AS STRING), 1.5D           | coalesce(s, v) | 1.5    | double  |
        | coalesce | FLOAT   | '2', CAST(1.5 AS FLOAT)              | coalesce(s, v) | 2.0    | double  |
        | coalesce | DECIMAL | '2', CAST(1.5 AS DECIMAL(5,2))       | coalesce(s, v) | 2.0    | double  |
        | coalesce | BOOLEAN | '1', false                           | coalesce(s, v) | true   | boolean |
        | ifnull   | BOOLEAN | CAST(NULL AS STRING), true           | ifnull(s, v)   | true   | boolean |
        | coalesce | BOOLEAN | 'false', true                        | coalesce(v, s) | true   | boolean |

    Scenario Outline: coalesce of STRING and TIME values is TIME (ANSI)
      When query
        """
        SELECT coalesce(<expression>) AS result, typeof(coalesce(<expression>)) AS result_type
        """
      Then query result
        | result   | result_type |
        | <result> | time(6)     |

      Examples:
        | expression                 | result   |
        | '10:00:00', TIME'11:00:00' | 10:00:00 |
        | TIME'11:00:00', '10:00:00' | 11:00:00 |

    Scenario: coalesce of STRING, INT, and FLOAT values is DOUBLE (ANSI)
      When query
        """
        SELECT coalesce(s, i, f) AS result, typeof(coalesce(s, i, f)) AS result_type
        FROM VALUES (CAST(NULL AS STRING), CAST(NULL AS INT), CAST(1.5 AS FLOAT)) AS t(s, i, f)
        """
      Then query result
        | result | result_type |
        | 1.5    | double      |

    Scenario Outline: coalesce with an invalid STRING value for <type> (ANSI)
      When query
        """
        SELECT coalesce(s, v) AS result FROM VALUES (<values>) AS t(s, v)
        """
      Then query error (?i)(cast|invalid|parse)

      Examples:
        | type    | values        |
        | BIGINT  | 'abc', 1      |
        | BOOLEAN | 'abc', true   |

  Rule: Without ANSI mode

    Background:
      Given config spark.sql.ansi.enabled = false

    Scenario Outline: coalesce of STRING and <case> values (non-ANSI)
      When query
        """
        SELECT coalesce(<expression>) AS result, typeof(coalesce(<expression>)) AS result_type
        """
      Then query result
        | result   | result_type |
        | <result> | string      |

      Examples:
        | case             | expression                      | result |
        | INT              | '7', 1                          | 7      |
        | INT first        | 1, 'x'                          | 1      |
        | DOUBLE           | 1e20D, 'x'                      | 1.0E20 |
        | NULL DOUBLE      | CAST(NULL AS DOUBLE), 'x'       | x      |
        | DECIMAL          | CAST(1.50 AS DECIMAL(5,2)), 'x' | 1.50   |
        | FLOAT            | CAST(1.5 AS FLOAT), 'x'         | 1.5    |

    Scenario: coalesce of STRING and BOOLEAN values is rejected (non-ANSI)
      When query
        """
        SELECT coalesce(s, bo) AS result FROM VALUES ('1', false) AS t(s, bo)
        """
      Then query error .
