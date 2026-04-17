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
