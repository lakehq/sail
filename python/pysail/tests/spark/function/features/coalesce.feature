Feature: coalesce returns the first non-null argument

  Rule: Basic usage

    Scenario: All non-null integers returns the first
      When query
        """
        SELECT coalesce(1, 2, 3) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: First argument is null returns the second
      When query
        """
        SELECT coalesce(NULL, 2) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: All nulls returns null
      When query
        """
        SELECT coalesce(CAST(NULL AS INT), CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Single non-null argument
      When query
        """
        SELECT coalesce(42) AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: Single null argument
      When query
        """
        SELECT coalesce(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Mixed string and date types

    Scenario: String column with date column returns string
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW coalesce_test AS
        SELECT CAST(NULL AS STRING) AS string_col, DATE '2024-01-15' AS date_col
        """
      When query
        """
        SELECT coalesce(string_col, date_col) AS result FROM coalesce_test
        """
      Then query result
        | result     |
        | 2024-01-15 |

    Scenario: Non-null string with date column returns string value
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW coalesce_test AS
        SELECT CAST('hello' AS STRING) AS string_col, DATE '2024-01-15' AS date_col
        """
      When query
        """
        SELECT coalesce(string_col, date_col) AS result FROM coalesce_test
        """
      Then query result
        | result |
        | hello  |

    Scenario: String literal with date column
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW coalesce_test AS
        SELECT DATE '2024-01-15' AS date_col
        """
      When query
        """
        SELECT coalesce('default', date_col) AS result FROM coalesce_test
        """
      Then query result
        | result  |
        | default |

    Scenario: Null string literal with date column returns date as string
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW coalesce_test AS
        SELECT DATE '2024-01-15' AS date_col
        """
      When query
        """
        SELECT coalesce(CAST(NULL AS STRING), date_col) AS result FROM coalesce_test
        """
      Then query result
        | result     |
        | 2024-01-15 |

  Rule: Mixed string and timestamp types

    Scenario: String column with timestamp column returns string
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW coalesce_test AS
        SELECT CAST(NULL AS STRING) AS string_col, TIMESTAMP '2024-01-15 10:30:00' AS ts_col
        """
      When query
        """
        SELECT coalesce(string_col, ts_col) AS result FROM coalesce_test
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:00 |

    Scenario: Non-null string with timestamp column returns string value
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW coalesce_test AS
        SELECT CAST('hello' AS STRING) AS string_col, TIMESTAMP '2024-01-15 10:30:00' AS ts_col
        """
      When query
        """
        SELECT coalesce(string_col, ts_col) AS result FROM coalesce_test
        """
      Then query result
        | result |
        | hello  |

    Scenario: String literal with timestamp column
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW coalesce_test AS
        SELECT TIMESTAMP '2024-01-15 10:30:00' AS ts_col
        """
      When query
        """
        SELECT coalesce('default', ts_col) AS result FROM coalesce_test
        """
      Then query result
        | result  |
        | default |

  Rule: Multiple arguments with mixed types

    Scenario: Three arguments with mixed string and date
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW coalesce_test AS
        SELECT CAST(NULL AS STRING) AS a, CAST(NULL AS DATE) AS b, DATE '2024-06-01' AS c
        """
      When query
        """
        SELECT coalesce(a, b, c) AS result FROM coalesce_test
        """
      Then query result
        | result     |
        | 2024-06-01 |

    Scenario: Multiple nulls with final date value
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW coalesce_test AS
        SELECT CAST(NULL AS STRING) AS a, CAST(NULL AS STRING) AS b, DATE '2024-03-20' AS c
        """
      When query
        """
        SELECT coalesce(a, b, c) AS result FROM coalesce_test
        """
      Then query result
        | result     |
        | 2024-03-20 |

  Rule: Column ordering

    Scenario: Date column first then string column
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW coalesce_test AS
        SELECT DATE '2024-01-15' AS date_col, CAST(NULL AS STRING) AS string_col
        """
      When query
        """
        SELECT coalesce(date_col, string_col) AS result FROM coalesce_test
        """
      Then query result
        | result     |
        | 2024-01-15 |

    Scenario: Timestamp column first then string column
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW coalesce_test AS
        SELECT TIMESTAMP '2024-01-15 10:30:00' AS ts_col, CAST(NULL AS STRING) AS string_col
        """
      When query
        """
        SELECT coalesce(ts_col, string_col) AS result FROM coalesce_test
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:00 |

  Rule: Empty result and null handling

    Scenario: Coalesce with all null mixed types
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW coalesce_test AS
        SELECT CAST(NULL AS STRING) AS string_col, CAST(NULL AS DATE) AS date_col
        """
      When query
        """
        SELECT coalesce(string_col, date_col) AS result FROM coalesce_test
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Coalesce with empty table
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW coalesce_empty AS
        SELECT CAST(NULL AS STRING) AS string_col, CAST(NULL AS DATE) AS date_col
        WHERE 1 = 0
        """
      When query
        """
        SELECT coalesce(string_col, date_col) AS result FROM coalesce_empty
        """
      Then query result
        | result |
