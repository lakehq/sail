@bin
Feature: bin converts integral values to binary strings

  Rule: String arguments follow Spark cast semantics

    Scenario: bin trims strings before casting to integers
      When query
        """
        SELECT bin(value) AS result
        FROM VALUES (' 13 '), (' -13 ') AS data(value)
        ORDER BY value
        """
      Then query result
        | result                                                           |
        | 1111111111111111111111111111111111111111111111111111111111110011 |
        | 1101                                                             |

    Scenario: bin malformed string errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT bin('ab') AS result
        """
      Then query error CAST_INVALID_INPUT

    Scenario: bin malformed string returns NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT bin(value) AS result
        FROM VALUES ('ab'), (CAST(NULL AS STRING)) AS data(value)
        ORDER BY value IS NULL, value
        """
      Then query result
        | result |
        | NULL   |
        | NULL   |

    Scenario: bin empty string errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT bin('') AS result
        """
      Then query error CAST_INVALID_INPUT

    Scenario: bin empty string returns NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT bin('') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: bin decimal string errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT bin('13.9') AS result
        """
      Then query error CAST_INVALID_INPUT

    Scenario: bin decimal string truncates toward zero under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT bin(value) AS result
        FROM VALUES (0, '13.9'), (1, '-13.9'), (2, '.3') AS data(id, value)
        ORDER BY id
        """
      Then query result
        | result                                                           |
        | 1101                                                             |
        | 1111111111111111111111111111111111111111111111111111111111110011 |
        | 0                                                                |

    Scenario: bin out-of-range string errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT bin('99999999999999999999') AS result
        """
      Then query error CAST_INVALID_INPUT

    Scenario: bin out-of-range string returns NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT bin('99999999999999999999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: bin scientific notation string errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT bin('1e3') AS result
        """
      Then query error CAST_INVALID_INPUT

    Scenario: bin scientific notation strings return NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT bin(value) AS result FROM VALUES
          (0, '1e3'),
          (1, '1E3'),
          (2, '  1e3  ')
        AS t(id, value) ORDER BY id
        """
      Then query result
        | result |
        | NULL   |
        | NULL   |
        | NULL   |

  Rule: Integer boundaries

    Scenario: bin zero
      When query
        """
        SELECT bin(0) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: bin minus one is all ones
      When query
        """
        SELECT bin(-1) AS result
        """
      Then query result
        | result                                                           |
        | 1111111111111111111111111111111111111111111111111111111111111111 |

    Scenario: bin INT_MAX uses 31 bits
      When query
        """
        SELECT bin(2147483647) AS result
        """
      Then query result
        | result                          |
        | 1111111111111111111111111111111 |

    Scenario: bin INT_MIN sign-extends to 64 bits
      When query
        """
        SELECT bin(-2147483648) AS result
        """
      Then query result
        | result                                                           |
        | 1111111111111111111111111111111110000000000000000000000000000000 |

    Scenario: bin LONG_MAX uses 63 bits
      When query
        """
        SELECT bin(9223372036854775807L) AS result
        """
      Then query result
        | result                                                          |
        | 111111111111111111111111111111111111111111111111111111111111111 |

    Scenario: bin LONG_MIN is 1 followed by 63 zeros
      When query
        """
        SELECT bin(CAST(-9223372036854775808 AS BIGINT)) AS result
        """
      Then query result
        | result                                                           |
        | 1000000000000000000000000000000000000000000000000000000000000000 |

    Scenario: bin TINYINT input promotes to BIGINT semantics
      When query
        """
        SELECT bin(CAST(99 AS TINYINT)) AS result
        """
      Then query result
        | result  |
        | 1100011 |

  Rule: Floating-point truncation toward zero

    Scenario: bin DOUBLE 1.5 truncates to 1
      When query
        """
        SELECT bin(1.5) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: bin DOUBLE 0.5 truncates to 0
      When query
        """
        SELECT bin(0.5) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: bin DOUBLE -0.5 truncates toward zero
      When query
        """
        SELECT bin(-0.5) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: bin DOUBLE -1.5 truncates toward zero then sign-extends
      When query
        """
        SELECT bin(-1.5) AS result
        """
      Then query result
        | result                                                           |
        | 1111111111111111111111111111111111111111111111111111111111111111 |

    Scenario: bin DECIMAL truncates fractional part
      When query
        """
        SELECT bin(CAST(1.5 AS DECIMAL(3,2))) AS result
        """
      Then query result
        | result |
        | 1      |

  Rule: NULL inputs

    Scenario: bin untyped NULL returns NULL
      When query
        """
        SELECT bin(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: bin typed NULL BIGINT returns NULL
      When query
        """
        SELECT bin(CAST(NULL AS BIGINT)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Unsupported types are rejected

    Scenario: bin rejects BOOLEAN
      When query
        """
        SELECT bin(true) AS result
        """
      Then query error .*

    Scenario: bin rejects DATE
      When query
        """
        SELECT bin(DATE '2024-01-15') AS result
        """
      Then query error .*

    Scenario: bin rejects TIMESTAMP
      When query
        """
        SELECT bin(TIMESTAMP '2024-01-15 12:00:00') AS result
        """
      Then query error .*

    Scenario: bin rejects BINARY
      When query
        """
        SELECT bin(X'01') AS result
        """
      Then query error .*

    Scenario: bin rejects ARRAY
      When query
        """
        SELECT bin(array(1)) AS result
        """
      Then query error .*

    Scenario: bin rejects MAP
      When query
        """
        SELECT bin(map('a', 1)) AS result
        """
      Then query error .*

    Scenario: bin rejects STRUCT
      When query
        """
        SELECT bin(named_struct('a', 1)) AS result
        """
      Then query error .*

  Rule: Float NaN and Infinity follow cast semantics

    Scenario: bin NaN errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT bin(CAST('NaN' AS DOUBLE)) AS result
        """
      Then query error .*

    Scenario: bin NaN truncates to zero under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT bin(CAST('NaN' AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: bin Infinity errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT bin(CAST('Infinity' AS DOUBLE)) AS result
        """
      Then query error .*

    Scenario: bin Infinity saturates to LONG_MAX under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT bin(CAST('Infinity' AS DOUBLE)) AS result
        """
      Then query result
        | result                                                          |
        | 111111111111111111111111111111111111111111111111111111111111111 |
