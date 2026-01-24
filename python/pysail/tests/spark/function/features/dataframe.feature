Feature: DataFrame creation and basic operations
  As a developer
  I want to create DataFrames with specified columns and data
  So that I can test DataFrame operations

  Scenario: Create a simple DataFrame with string and integer columns
    Given a DataFrame with columns "name, age" and data:
      | name  | age |
      | Alice | 25  |
      | Bob   | 30  |
    Then the resulting DataFrame contains only columns "name, age"
    And the data is:
      | name  | age |
      | Alice | 25  |
      | Bob   | 30  |
