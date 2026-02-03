Feature: Division by zero behavior

  Rule: All division by zero returns NULL when ANSI mode is disabled (Spark 4.x behavior)
    Scenario: Float divided by zero returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 1.0 / 0.0 AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Negative float divided by zero returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT -1.0 / 0.0 AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Zero divided by zero returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 0.0 / 0.0 AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Integer divided by integer zero returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 1 / 0 AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Integer divided by float zero returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 1 / 0.0 AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Division by zero throws error when ANSI mode is enabled
    Scenario: Integer divided by zero throws error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT 1 / 0 AS result
        """
      Then query error (?i)divide.*zero

    Scenario: Float divided by zero throws error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT 1.0 / 0.0 AS result
        """
      Then query error (?i)divide.*zero
