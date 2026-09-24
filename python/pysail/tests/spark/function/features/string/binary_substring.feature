Feature: substr, substring, left and overlay over a BINARY, vs Spark 4.2.0

  # Spark's `Substring`, `Left` and `Overlay` accept a BINARY and return a BINARY, cut by BYTES
  # (`ByteArray.subStringSQL`; `Left` is `Substring(str, 1, len)` and a binary `Overlay` is
  # `concat(subStringSQL(input, 1, pos - 1), replace, subStringSQL(input, pos + length))`,
  # `stringExpressions.scala:1000-1010,2408`). Sail cast the BINARY to a STRING first, which was
  # wrong three ways: the result was typed STRING -- an arithmetic operand, so `2 / substr(bin, 5)`
  # resolved where Spark refuses it -- it cut by CHARACTERS, so a multibyte input came back a
  # different slice, and an input that is not valid UTF-8 raised.
  #
  # Values are compared as HEX, the bytes themselves, so a rendering cannot hide a wrong slice.
  # Every row was measured on the JVM; ANSI does not change any of them.

  Rule: the result is a BINARY cut by bytes

    Scenario Outline: a binary <case> is typed <type> with bytes <hex>
      When query
        """
        SELECT typeof(<expression>) AS t, hex(<expression>) AS h
        """
      Then query result
        | t      | h     |
        | <type> | <hex> |

      Examples:
        | case              | expression                                                  | type   | hex                |
        | substr pos        | substr(X'537061726B2053514C', 5)                            | binary | 6B2053514C         |
        | substr pos len    | substr(X'537061726B2053514C', 2, 3)                         | binary | 706172             |
        | substr zero       | substr(X'537061726B2053514C', 0, 3)                         | binary | 537061             |
        | substr neg        | substr(X'537061726B2053514C', -3)                           | binary | 53514C             |
        | substr neg len    | substr(X'537061726B2053514C', -3, 2)                        | binary | 5351               |
        | substr multibyte  | substr(X'41E282AC42C3A943', 2, 3)                           | binary | E282AC             |
        | substr bad utf8   | substr(X'FF00FE41', 2, 2)                                   | binary | 00FE               |
        | left              | left(X'537061726B2053514C', 3)                              | binary | 537061             |
        | left multibyte    | left(X'41E282AC42C3A943', 2)                                | binary | 41E2               |
        | overlay           | overlay(X'537061726B2053514C' PLACING X'5F' FROM 6)         | binary | 537061726B5F53514C |
        | overlay for       | overlay(X'537061726B2053514C' PLACING X'5F5F' FROM 2 FOR 3) | binary | 535F5F6B2053514C   |
        | overlay multibyte | overlay(X'41E282AC42C3A943' PLACING X'2D' FROM 2 FOR 3)     | binary | 412D42C3A943       |

    Scenario Outline: a binary <case> is an empty BINARY
      When query
        """
        SELECT typeof(<expression>) AS t, length(<expression>) AS n
        """
      Then query result
        | t      | n |
        | binary | 0 |

      Examples:
        | case            | expression                        |
        | substr past end | substr(X'537061726B2053514C', 20) |
        | left zero       | left(X'537061726B2053514C', 0)    |
        | left neg        | left(X'537061726B2053514C', -1)   |

    Scenario: a NULL binary input stays a NULL BINARY
      When query
        """
        SELECT typeof(substr(CAST(NULL AS BINARY), 1)) AS t, substr(CAST(NULL AS BINARY), 1) IS NULL AS n
        """
      Then query result
        | t      | n    |
        | binary | true |

    # `Overlay` is null-intolerant (`stringExpressions.scala:1041`): a NULL length is a NULL result,
    # not a fall-back to the length of the replacement.
    Scenario: a binary overlay with a NULL length is a NULL BINARY
      When query
        """
        SELECT typeof(overlay(X'537061726B' PLACING X'5F' FROM 2 FOR CAST(NULL AS INT))) AS t,
          overlay(X'537061726B' PLACING X'5F' FROM 2 FOR CAST(NULL AS INT)) IS NULL AS n
        """
      Then query result
        | t      | n    |
        | binary | true |

    # `Left` takes an INT length under `ImplicitCastInputTypes`, so a TINYINT or SMALLINT widens.
    Scenario Outline: left over a binary takes a <case> length
      When query
        """
        SELECT typeof(<expression>) AS t, hex(<expression>) AS h
        """
      Then query result
        | t      | h     |
        | binary | <hex> |

      Examples:
        | case     | expression                                        | hex    |
        | tinyint  | left(X'4142', 1Y)                                 | 41     |
        | smallint | left(X'537061726B2053514C', CAST(3 AS SMALLINT))  | 537061 |

  Rule: right is the exception -- Spark reads its BINARY input as a STRING

    # `Right` takes only strings, so a BINARY is implicitly cast and the result is a STRING, which
    # stays an arithmetic operand. Sail already agrees; this pins it so the binary fix above is not
    # extended to it by mistake.
    Scenario Outline: right over a binary is typed string: <case>
      When query
        """
        SELECT typeof(<expression>) AS t, hex(<expression>) AS h
        """
      Then query result
        | t      | h     |
        | string | <hex> |

      Examples:
        | case            | expression                        | hex    |
        | ascii           | right(X'537061726B2053514C', 3)   | 53514C |
        | multibyte       | right(X'41E282AC42C3A943', 2)     | C3A943 |
