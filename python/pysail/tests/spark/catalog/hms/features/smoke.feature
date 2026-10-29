Feature: HMS connectivity

  Scenario: List the default database
    When query
      """
      SHOW DATABASES
      """
    Then query result has row where "name" is "default"
