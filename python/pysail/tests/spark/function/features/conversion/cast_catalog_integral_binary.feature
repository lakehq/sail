# Regression coverage for CAST(integral AS BINARY) / binary(integral): Spark's
# `NumberConverter.toBinary` (Cast.scala:688-694) produces fixed-width BIG-ENDIAN bytes,
# unlike Arrow's own numeric-to-binary cast kernel, which uses native (little-endian on
# common platforms) byte order. `binary(expr)` is `Cast(expr, BinaryType)` under the hood
# in Spark, so it must agree with CAST exactly, including the ANSI-mode rejection.
Feature: Additional CAST coverage for integral to BINARY

  Rule: CAST truncates to the source width and uses big-endian byte order

    Scenario: integral_binary catalog: CAST of each integral width to BINARY without ANSI
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          CAST(CAST(1 AS TINYINT) AS BINARY) AS tiny,
          CAST(CAST(1 AS SMALLINT) AS BINARY) AS small,
          CAST(1 AS BINARY) AS int_,
          CAST(1L AS BINARY) AS big
        """
      Then query result
        | tiny | small   | int_          | big                       |
        | [01] | [00 01] | [00 00 00 01] | [00 00 00 00 00 00 00 01] |

    Scenario: integral_binary catalog: binary() function agrees with CAST
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          binary(CAST(1 AS TINYINT)) AS tiny,
          binary(CAST(1 AS SMALLINT)) AS small,
          binary(1) AS int_,
          binary(1L) AS big
        """
      Then query result
        | tiny | small   | int_          | big                       |
        | [01] | [00 01] | [00 00 00 01] | [00 00 00 00 00 00 00 01] |

  Rule: ANSI mode rejects integral -> BINARY (only the legacy, non-ANSI rule allows it)

    # Spark raises DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION here; Sail does not yet
    # reproduce that exact error class (see test_cast_matrix.py's alias mechanism for the
    # full matrix of these), so this checks Sail's own rejection message instead.
    Scenario Outline: integral_binary catalog: <expr> is rejected under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS result
        """
      Then query error cannot cast Int32 to binary

      Examples:
        | expr              |
        | CAST(1 AS BINARY) |
        | binary(1)         |
