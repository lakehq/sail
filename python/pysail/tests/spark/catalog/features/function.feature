Feature: Catalog function queries

  Scenario: Built-in function exists
    When catalog functionExists for count
    Then the function existence result is true

  Scenario: Built-in function name lookup is case-insensitive
    When catalog functionExists for COUNT
    Then the function existence result is true

  Scenario: Missing function does not exist
    When catalog functionExists for this_function_definitely_does_not_exist
    Then the function existence result is false

  Scenario: Missing qualified function does not exist
    When catalog functionExists for default.unexisting_function
    Then the function existence result is false

  Scenario: Missing fully qualified function does not exist
    When catalog functionExists for spark_catalog.default.unexisting_function
    Then the function existence result is false

  Scenario: Get a built-in function returns expected metadata
    When catalog getFunction for count
    Then the function attribute name is count
    And the function attribute isTemporary is true

  Scenario: Get a missing function raises an error
    When catalog getFunction for this_function_definitely_does_not_exist
    Then the getFunction call raises an error matching (?i)not found|does not exist|cannot find

  Scenario: List functions returns built-in functions
    When catalog listFunctions
    Then the listFunctions result is not empty
    And the listFunctions result contains a function named count

  Scenario: List functions with pattern filters results
    When catalog listFunctions with pattern to_*
    Then the listFunctions result is not empty
    And the listFunctions result contains a function named to_date

  Scenario: List functions with non-matching pattern returns empty
    When catalog listFunctions with pattern *this_pattern_will_never_match*
    Then the listFunctions result is empty
