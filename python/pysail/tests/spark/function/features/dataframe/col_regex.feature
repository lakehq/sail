Feature: Column selection using colRegex
  As a developer
  I want to select columns from a DataFrame using a regular expression
  So that I can work with dynamic schemas flexibly

  Scenario: Select columns using colRegex pattern
    Given a DataFrame with columns "col1, col2, other" and data:
      | col1 | col2 | other |
      | 1    | 2    | 3     |
    When I select columns using colRegex "`col.*`"
    Then the resulting DataFrame contains only columns "col1, col2"
    And the data is:
      | col1 | col2 |
      | 1    | 2    |
