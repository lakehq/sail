@is_valid_utf8
Feature: is_valid_utf8() reports whether a value is valid UTF-8

  # Spark declares the parameter as STRING and the analyzer inserts an implicit
  # cast to STRING for other castable types, then validates the bytes. A NULL
  # input yields NULL (not false). All expected values were verified against the
  # Spark JVM (PySpark 4.1.1).

  Rule: NULL input returns NULL

    Scenario: NULL string input
      When query
        """
        SELECT is_valid_utf8(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL binary input
      When query
        """
        SELECT is_valid_utf8(CAST(NULL AS BINARY)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: String input

    Scenario: a valid string is valid
      When query
        """
        SELECT is_valid_utf8('hello') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: the empty string is valid
      When query
        """
        SELECT is_valid_utf8('') AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Non-string castable input is cast to string

    Scenario: integer input is read as a string
      When query
        """
        SELECT is_valid_utf8(123) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: double input is read as a string
      When query
        """
        SELECT is_valid_utf8(1.5) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: boolean input is read as a string
      When query
        """
        SELECT is_valid_utf8(true) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: timestamp input is read as a string
      When query
        """
        SELECT is_valid_utf8(TIMESTAMP '2024-01-15 12:00:00') AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Binary input is validated byte by byte

    Scenario: valid UTF-8 bytes are valid
      When query
        """
        SELECT is_valid_utf8(X'68656C6C6F') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: the empty binary is valid
      When query
        """
        SELECT is_valid_utf8(X'') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: a lone continuation byte is invalid
      When query
        """
        SELECT is_valid_utf8(X'80') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: an overlong encoding is invalid
      When query
        """
        SELECT is_valid_utf8(X'C080') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: a truncated multibyte sequence is invalid
      When query
        """
        SELECT is_valid_utf8(X'C3') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: a valid two-byte sequence is valid
      When query
        """
        SELECT is_valid_utf8(X'C3A9') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: a valid four-byte sequence is valid
      When query
        """
        SELECT is_valid_utf8(X'F09F9880') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: a valid three-byte sequence is valid
      When query
        """
        SELECT is_valid_utf8(X'E282AC') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: a UTF-8 BOM is valid content
      When query
        """
        SELECT is_valid_utf8(X'EFBBBF') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: a surrogate-range encoding is invalid
      When query
        """
        SELECT is_valid_utf8(X'EDA080') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: a code point above U+10FFFF is invalid
      When query
        """
        SELECT is_valid_utf8(X'F4908080') AS result
        """
      Then query result
        | result |
        | false  |

  Rule: Column expressions over multiple rows

    Scenario: string column with a NULL row
      When query
        """
        SELECT is_valid_utf8(s) AS result
        FROM VALUES ('hello'), (CAST(NULL AS STRING)), ('') AS t(s)
        """
      Then query result
        | result |
        | true   |
        | NULL   |
        | true   |

    Scenario: binary column mixing valid, invalid and NULL rows
      When query
        """
        SELECT is_valid_utf8(b) AS result
        FROM VALUES (X'68656C6C6F'), (X'80'), (CAST(NULL AS BINARY)) AS t(b)
        """
      Then query result
        | result |
        | true   |
        | false  |
        | NULL   |

  Rule: Argument count

    Scenario: zero arguments is an error
      When query
        """
        SELECT is_valid_utf8() AS result
        """
      Then query error .*

    Scenario: two arguments is an error
      When query
        """
        SELECT is_valid_utf8('a', 'b') AS result
        """
      Then query error .*
