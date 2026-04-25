Feature: DataFrame sampleBy operations

  Scenario: DataFrame sampleBy selects matching strata and preserves columns
    When DataFrame sampleBy selects matching strata
    Then DataFrame columns
      | key | value | payload |
    And DataFrame result ordered
      | key | value | payload |
      | 0   | 0     | zero    |
      | 0   | 3     | three   |
      | 2   | 2     | two     |

  Scenario: DataFrameStatFunctions sampleBy returns empty result for unmatched strata
    When DataFrameStatFunctions sampleBy has no matching strata
    Then DataFrame columns
      | key | value | payload |
    And DataFrame row count is 0

  Scenario: DataFrame sampleBy returns empty result for empty fractions
    When DataFrame sampleBy has empty fractions
    Then DataFrame columns
      | key | value | payload |
    And DataFrame row count is 0

  Scenario: DataFrame sampleBy accepts column expressions
    When DataFrame sampleBy uses a column expression
    Then DataFrame result ordered
      | key | value | payload |
      | 0   | 0     | zero    |
      | 0   | 3     | three   |

  Scenario: DataFrame sampleBy rejects invalid fractions
    When DataFrame sampleBy with fraction 1.2 is collected with error fraction

  Scenario: DataFrame sampleBy rejects NaN fractions
    When DataFrame sampleBy with fraction NaN is collected with error fraction
