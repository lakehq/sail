@length
Feature: length() returns character length for strings and byte length for binary

  Rule: Character length for string data

    Scenario: length counts characters including trailing spaces
      When query
        """
        SELECT length('Spark SQL ') AS result
        """
      Then query result
        | result |
        | 10     |

    Scenario: length of the empty string is zero
      When query
        """
        SELECT length('') AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: length counts characters not bytes for multi-byte text
      When query
        """
        SELECT length('josé') AS accented, length('中文') AS cjk, length('😀') AS emoji
        """
      Then query result
        | accented | cjk | emoji |
        | 4        | 2   | 1     |

  Rule: Byte length for binary data

    Scenario: length of binary counts bytes not characters
      When query
        """
        SELECT length(CAST('josé' AS BINARY)) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: length of binary that is not valid UTF-8
      When query
        """
        SELECT length(X'DEADBEEF') AS result
        """
      Then query result
        | result |
        | 4      |

    Scenario: length of empty binary is zero
      When query
        """
        SELECT length(X'') AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: length of binary counts every UTF-8 byte of an emoji
      When query
        """
        SELECT length(CAST('😀' AS BINARY)) AS result
        """
      Then query result
        | result |
        | 4      |

    Scenario: aliases len, char_length and character_length also count binary bytes
      When query
        """
        SELECT len(X'DEADBEEF') AS a, char_length(X'DEADBEEF') AS b, character_length(X'DEADBEEF') AS c
        """
      Then query result
        | a | b | c |
        | 4 | 4 | 4 |

    Scenario: length of binary includes binary zeros
      When query
        """
        SELECT length(X'00010203') AS result
        """
      Then query result
        | result |
        | 4      |

    Scenario: length of a single byte that is not valid UTF-8 on its own
      When query
        """
        SELECT length(unhex('FF')) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: length of UTF-16 encoded binary counts the BOM and the NUL bytes
      When query
        """
        SELECT length(encode('ab', 'UTF-16')) AS result
        """
      Then query result
        | result |
        | 6      |

    Scenario: length of a binary column mixes valid UTF-8, NULL and raw bytes
      When query
        """
        SELECT length(b) AS result FROM VALUES (CAST('josé' AS BINARY)), (NULL), (X'00') AS t(b)
        """
      Then query result
        | result |
        | 5      |
        | NULL   |
        | 1      |

    Scenario: binary byte lengths can be aggregated
      When query
        """
        SELECT sum(length(b)) AS result FROM VALUES (X'AABB'), (X'CC'), (NULL) AS t(b)
        """
      Then query result
        | result |
        | 3      |

    Scenario: length of large binary counts every byte
      When query
        """
        SELECT length(CAST(repeat('é', 10000) AS BINARY)) AS result
        """
      Then query result
        | result |
        | 20000  |

  Rule: octet_length and bit_length measure bytes and bits

    Scenario: octet_length and bit_length of binary that is not valid UTF-8
      When query
        """
        SELECT octet_length(X'DEADBEEF') AS octets, bit_length(X'DEADBEEF') AS bits
        """
      Then query result
        | octets | bits |
        | 4      | 32   |

    Scenario: octet_length and bit_length of binary count bytes not characters
      When query
        """
        SELECT octet_length(CAST('josé' AS BINARY)) AS octets, bit_length(CAST('josé' AS BINARY)) AS bits
        """
      Then query result
        | octets | bits |
        | 5      | 40   |

    Scenario: octet_length and bit_length of UTF-16 encoded binary
      When query
        """
        SELECT octet_length(encode('ab', 'UTF-16')) AS octets, bit_length(encode('ab', 'UTF-16')) AS bits
        """
      Then query result
        | octets | bits |
        | 6      | 48   |

    Scenario: octet_length and bit_length of empty binary are zero
      When query
        """
        SELECT octet_length(X'') AS octets, bit_length(X'') AS bits
        """
      Then query result
        | octets | bits |
        | 0      | 0    |

    Scenario: octet_length and bit_length of strings measure UTF-8 bytes not characters
      When query
        """
        SELECT octet_length('josé') AS accented_octets, bit_length('josé') AS accented_bits, octet_length('😀') AS emoji_octets, bit_length('😀') AS emoji_bits
        """
      Then query result
        | accented_octets | accented_bits | emoji_octets | emoji_bits |
        | 5               | 40            | 4            | 32         |

    Scenario: octet_length and bit_length of NULL binary are NULL
      When query
        """
        SELECT octet_length(CAST(NULL AS BINARY)) AS octets, bit_length(CAST(NULL AS BINARY)) AS bits
        """
      Then query result
        | octets | bits |
        | NULL   | NULL |

  Rule: NULL propagation

    Scenario: length of a NULL string is NULL
      When query
        """
        SELECT length(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: length of a NULL binary is NULL
      When query
        """
        SELECT length(CAST(NULL AS BINARY)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: length of an untyped NULL is NULL
      When query
        """
        SELECT length(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Implicit coercion of non-string input to its string form

    Scenario: numeric and boolean input is measured as its string form
      When query
        """
        SELECT length(12345) AS int_in, length(1.5) AS double_in, length(true) AS bool_in, length(CAST(1.0 AS DECIMAL(10,2))) AS decimal_in
        """
      Then query result
        | int_in | double_in | bool_in | decimal_in |
        | 5      | 3         | 4       | 4          |

    Scenario: date and timestamp input is measured as its string form
      When query
        """
        SELECT length(DATE '2024-01-15') AS date_in, length(TIMESTAMP '2024-01-15 12:00:00') AS ts_in
        """
      Then query result
        | date_in | ts_in |
        | 10      | 19    |

    Scenario: a timestamp with microseconds keeps its fractional part, with or without a time zone
      When query
        """
        SELECT length(TIMESTAMP '2024-01-15 12:00:00.123456') AS ts_in, length(TIMESTAMP_NTZ '2024-01-15 12:00:00.123456') AS ntz_in
        """
      Then query result
        | ts_in | ntz_in |
        | 26    | 26     |

    Scenario: octet_length of a timestamp measures its string form, without a time zone suffix
      When query
        """
        SELECT octet_length(TIMESTAMP '2024-01-15 12:00:00') AS result
        """
      Then query result
        | result |
        | 19     |

    Scenario: time input is measured as its string form
      When query
        """
        SELECT length(TIME '23:59:59.999999') AS result
        """
      Then query result
        | result |
        | 15     |

    Scenario: char input is not padded to its declared length
      When query
        """
        SELECT length(CAST('ab' AS CHAR(10))) AS char_in, length(CAST('ab' AS VARCHAR(10))) AS varchar_in
        """
      Then query result
        | char_in | varchar_in |
        | 2       | 2          |

  Rule: The result is always a non-nullable-preserving 32-bit integer

    # Spark's Length/OctetLength/BitLength always return INT, never BIGINT, whatever the
    # width of the input's offsets, and they propagate the nullability of their input.

    Scenario: length of a string literal is a non-nullable integer
      When query
        """
        SELECT length('josé') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: length of a binary literal is a non-nullable integer
      When query
        """
        SELECT length(X'DEADBEEF') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: octet_length and bit_length also return integers
      When query
        """
        SELECT octet_length(X'DEADBEEF') AS octets, bit_length(X'DEADBEEF') AS bits
        """
      Then query schema
        """
        root
         |-- octets: integer (nullable = false)
         |-- bits: integer (nullable = false)
        """

    Scenario: length of a nullable input is a nullable integer
      When query
        """
        SELECT length(b) AS result FROM VALUES (X'00'), (NULL) AS t(b)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Types whose string form Sail renders differently from Spark

    # These measure correctly — they measure the string form Sail produces, and that
    # string form is what diverges. The root cause is Sail's CAST-to-string formatter,
    # not the length family: Sail renders a double in scientific notation as `1e18`
    # where Spark renders `1.0E18`, and a day interval as `INTERVAL '1 00:00:00' DAY TO
    # SECOND` where Spark renders `INTERVAL '1' DAY`. Fixing the formatter fixes these.

    @sail-bug
    Scenario: a double in scientific notation is measured as Spark renders it
      When query
        """
        SELECT length(CAST(1e18 AS DOUBLE)) AS big, length(CAST(1e-7 AS DOUBLE)) AS small
        """
      Then query result
        | big | small |
        | 6   | 6     |

    @sail-bug
    Scenario: an interval is measured as Spark renders it
      When query
        """
        SELECT length(INTERVAL 1 DAY) AS result
        """
      Then query result
        | result |
        | 16     |

  Rule: Collections are rejected

    Scenario: length of an array is an error
      When query
        """
        SELECT length(array(1, 2, 3)) AS result
        """
      Then query error .*
