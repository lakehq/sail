Feature: Spark-compatible structured arithmetic errors

  # Spark picks the DATATYPE_MISMATCH subclass per expression: BINARY_OP_WRONG_TYPE,
  # BINARY_OP_DIFF_TYPES or UNEXPECTED_INPUT_TYPE, all with SQLSTATE 42K09. Sail's
  # arithmetic rejects carry neither the subclass nor the SQLSTATE.
  @sail-bug
  @spark-4
  Scenario Outline: rejected arithmetic carries the exact Spark error subclass and SQLSTATE
    When query
      """
      SELECT <expression>
      """
    Then query error <error>

    # No backslashes in the regex: a Gherkin table cell doubles them, so `\[` would reach
    # the matcher as a literal backslash. An unescaped `.` still matches the dot.
    Examples:
      | expression              | error                                                         |
      | true + true             | (?s)DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE.*SQLSTATE: 42K09   |
      | array(1, 2) + 1         | (?s)DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES.*SQLSTATE: 42K09   |
      | INTERVAL '2' DAY + true | (?s)DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE.*SQLSTATE: 42K09  |
