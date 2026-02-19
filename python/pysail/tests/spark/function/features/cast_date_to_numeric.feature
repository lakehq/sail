Feature: CAST date to numeric types returns null

  In Spark legacy mode, casting a DATE to any numeric type returns NULL.
  In ANSI mode, the cast raises an error. TRY_CAST always returns NULL.

  Rule: CAST date to numeric types returns null (legacy mode)

    Scenario: cast date to int returns null
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(DATE '2023-01-15' AS INT) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: cast date to bigint returns null
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(DATE '2023-01-15' AS BIGINT) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: cast date to smallint returns null
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(DATE '2023-01-15' AS SMALLINT) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: cast date to tinyint returns null
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(DATE '2023-01-15' AS TINYINT) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: cast date to float returns null
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(DATE '2023-01-15' AS FLOAT) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: cast date to double returns null
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(DATE '2023-01-15' AS DOUBLE) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: cast date to decimal returns null
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(DATE '2023-01-15' AS DECIMAL(10,2)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: cast date to boolean returns null
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(DATE '2023-01-15' AS BOOLEAN) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: cast null date to int returns null
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(CAST(NULL AS DATE) AS INT) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: CAST date to numeric in ANSI mode raises error

    @sail-only
    Scenario: cast date to int in ANSI mode raises error
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(DATE '2023-01-15' AS INT) AS result
        """
      Then query error cannot cast date
      Given config spark.sql.ansi.enabled = false

    @sail-only
    Scenario: cast date to double in ANSI mode raises error
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(DATE '2023-01-15' AS DOUBLE) AS result
        """
      Then query error cannot cast date
      Given config spark.sql.ansi.enabled = false

    @sail-only
    Scenario: cast date to boolean in ANSI mode raises error
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(DATE '2023-01-15' AS BOOLEAN) AS result
        """
      Then query error cannot cast date
      Given config spark.sql.ansi.enabled = false
