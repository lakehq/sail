Feature: Column selection using colRegex in PySpark
  As a developer
  I want to select columns from a DataFrame using a regular expression
  So that I can work with dynamic schemas flexibly

  Scenario: Select columns not starting with Col1 using colRegex
    Given a DataFrame with columns "Col1, Col2" and data:
      | Col1 | Col2 |
      | a    | 1    |
      | b    | 2    |
      | c    | 3    |
    When I select columns using colRegex "`Col[^1]`"
    Then the resulting DataFrame contains only columns "Col2"
    And the data is:
      | Col2 |
      | 1    |
      | 2    |
      | 3    |

  Scenario: Select columns starting with col using colRegex
    Given a DataFrame with columns "col1, col2, col3, other" and data:
      | col1 | col2 | col3 | other |
      | 1    | 2    | 3    | 4     |
    When I select columns using colRegex "`col.*`"
    Then the resulting DataFrame contains only columns "col1, col2, col3"
    And the data is:
      | col1 | col2 | col3 |
      | 1    | 2    | 3    |

  Scenario: Select columns ending with name using colRegex
    Given a DataFrame with columns "firstname, lastname, age" and data:
      | firstname | lastname | age |
      | Alice     | Smith    | 25  |
      | Bob       | Jones    | 30  |
    When I select columns using colRegex "`.*name$`"
    Then the resulting DataFrame contains only columns "firstname, lastname"
    And the data is:
      | firstname | lastname |
      | Alice     | Smith    |
      | Bob       | Jones    |

  Scenario: Select a single column using colRegex
    Given a DataFrame with columns "name, age" and data:
      | name  | age |
      | Alice | 25  |
    When I select columns using colRegex "`age`"
    Then the resulting DataFrame contains only columns "age"
    And the data is:
      | age |
      | 25  |

  Scenario: Select columns with numeric pattern using colRegex
    Given a DataFrame with columns "col1, col2, col10, column" and data:
      | col1 | col2 | col10 | column |
      | 1    | 2    | 3     | 4      |
    When I select columns using colRegex "`col[0-9]+`"
    Then the resulting DataFrame contains only columns "col1, col2, col10"
    And the data is:
      | col1 | col2 | col10 |
      | 1    | 2    | 3     |

  Scenario: Select all columns using colRegex wildcard
    Given a DataFrame with columns "a, b, c" and data:
      | a | b | c |
      | 1 | 2 | 3 |
    When I select columns using colRegex "`.*`"
    Then the resulting DataFrame contains only columns "a, b, c"
    And the data is:
      | a | b | c |
      | 1 | 2 | 3 |

  Scenario: No columns match the regex pattern returns empty DataFrame
    Given a DataFrame with columns "col1, col2, col3" and data:
      | col1 | col2 | col3 |
      | 1    | 2    | 3    |
    When I select columns using colRegex "`nonexistent.*`"
    Then the resulting DataFrame has no columns
