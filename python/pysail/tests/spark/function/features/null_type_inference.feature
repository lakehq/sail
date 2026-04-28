Feature: Correct type inference for NULL literals and to_timestamp

  Rule: NULL literal resolves to string type

    Scenario: SELECT NULL returns string schema
      When query
      """
      SELECT NULL
      """
      Then query schema
      """
      root
       |-- NULL: string (nullable = true)
      """

    Scenario: NULL literal in a named column returns string schema
      When query
      """
      SELECT NULL AS col
      """
      Then query schema
      """
      root
       |-- col: string (nullable = true)
      """

  Rule: to_timestamp always returns timestamp type with local timezone

    Scenario: to_timestamp of NULL returns timestamp schema
      When query
      """
      SELECT TO_TIMESTAMP(NULL)
      """
      Then query schema
      """
      root
       |-- TO_TIMESTAMP(NULL): timestamp (nullable = true)
      """

    Scenario: to_timestamp_ltz of NULL returns timestamp schema
      When query
      """
      SELECT TO_TIMESTAMP_LTZ(NULL)
      """
      Then query schema
      """
      root
       |-- TO_TIMESTAMP_LTZ(NULL): timestamp (nullable = true)
      """

    Scenario: to_timestamp_ntz of NULL returns timestamp_ntz schema
      When query
      """
      SELECT TO_TIMESTAMP_NTZ(NULL)
      """
      Then query schema
      """
      root
       |-- TO_TIMESTAMP_NTZ(NULL): timestamp_ntz (nullable = true)
      """
