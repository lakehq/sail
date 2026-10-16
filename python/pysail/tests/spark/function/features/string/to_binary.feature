Feature: to_binary output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to to_binary yields the schema Spark declares
      When query
        """
        SELECT to_binary('abc', 'utf-8') AS result
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a non-null column input to to_binary yields the schema Spark declares
      When query
        """
        SELECT to_binary(CAST(id AS STRING), 'utf-8') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a nullable column input to to_binary stays nullable
      When query
        """
        SELECT to_binary(c, 'utf-8') AS result FROM VALUES ('abc'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario Outline: to_binary with format <format> is <nullable>
      When query
        """
        SELECT to_binary(<args>) AS result
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = <nullable>)
        """

      Examples:
        | format  | args               | nullable |
        | hex     | '414243', 'hex'    | true     |
        | default | '414243'           | true     |

    @sail-bug
    Scenario Outline: to_binary with format <format> is <nullable> (known Sail bug)
      When query
        """
        SELECT to_binary(<args>) AS result
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = <nullable>)
        """

      Examples:
        | format  | args               | nullable |
        | base64  | 'YWJj', 'base64'   | false    |

    # Spark's rule for `fmt` is FOLDABLE, not literal (`ToBinary.checkInputDataTypes` only
    # requires `f.foldable`, and `ToBinary.fmt` evaluates it), so a folded expression picks the
    # same replacement as the equivalent literal. Sail reads the format from
    # `ReturnFieldArgs::scalar_arguments`, which DataFusion populates only for a bare literal,
    # so the base64 branch is missed and the output falls back to nullable.
    # Value verified against the Spark 4 JVM (`SPARK_REMOTE="local"`): nullable = false.
    @sail-bug
    Scenario: a foldable non-literal format picks the same branch as the literal
      When query
        """
        SELECT to_binary('YWJj', concat('base', '64')) AS result
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = false)
        """

  # FOLLOW-UP, deliberately not fixed in this PR (which only derives output nullability).
  # Spark lowercases `fmt` but never trims it (`ToBinary.fmt`, stringExpressions.scala), and
  # `ToBinary.checkInputDataTypes` rejects anything outside {hex, utf-8, utf8, base64} with
  # DATATYPE_MISMATCH.INVALID_ARG_VALUE. `try_to_binary` passes `nullOnInvalidFormat = true`,
  # so it yields NULL instead of raising.
  # Sail instead trims the format in every code path (`return_field_from_args`, `simplify`,
  # `invoke_with_args`) and never validates it, so a padded format silently decodes.
  # All expected values below were captured from the Spark 4 JVM (`SPARK_REMOTE="local"`).
  Rule: The fmt argument is matched exactly, never trimmed

    @sail-bug
    Scenario: a base64 format padded with spaces is rejected
      When query
        """
        SELECT to_binary('YWJj', ' base64 ') AS result
        """
      Then query error DATATYPE_MISMATCH\.INVALID_ARG_VALUE

    @sail-bug
    Scenario: a hex format padded with spaces is rejected
      When query
        """
        SELECT to_binary('414243', ' hex ') AS result
        """
      Then query error DATATYPE_MISMATCH\.INVALID_ARG_VALUE

    @sail-bug
    Scenario: a utf-8 format padded with spaces is rejected
      When query
        """
        SELECT to_binary('abc', ' utf-8 ') AS result
        """
      Then query error DATATYPE_MISMATCH\.INVALID_ARG_VALUE

    Scenario: an uppercase format is accepted, because Spark lowercases it
      When query
        """
        SELECT CAST(to_binary('YWJj', 'BASE64') AS STRING) AS result
        """
      Then query result
        | result |
        | abc    |

    @sail-bug
    Scenario: an unknown format is rejected with Spark's own error
      When query
        """
        SELECT to_binary('YWJj', 'foo') AS result
        """
      Then query error DATATYPE_MISMATCH\.INVALID_ARG_VALUE

    @sail-bug
    Scenario: try_to_binary yields NULL for a padded format instead of decoding it
      When query
        """
        SELECT try_to_binary('YWJj', ' base64 ') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_to_binary yields NULL for an unknown format
      When query
        """
        SELECT try_to_binary('YWJj', 'foo') AS result
        """
      Then query result
        | result |
        | NULL   |
