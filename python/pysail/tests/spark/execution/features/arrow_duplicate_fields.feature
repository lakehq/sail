Feature: Arrow execution with duplicate field names
  Scenario: Collect duplicate nested struct fields
    When dataframe for duplicate struct fields
    Then dataframe collect matches duplicate struct rows

  Scenario: Convert duplicate top-level columns to Pandas
    When dataframe for duplicate top-level columns
    Then dataframe pandas row count is 3
    And dataframe pandas columns
      """
      ["id", "id", "id"]
      """

  Scenario: Convert duplicate nested struct fields to Pandas dictionaries
    Given config spark.sql.execution.pandas.structHandlingMode = dict
    When dataframe for duplicate struct fields
    Then dataframe pandas columns
      """
      ["struct", "struct"]
      """
    And dataframe pandas dict structs have deduplicated field names
