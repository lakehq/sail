# CAST scenarios imported from test/bug_catalog (0886a9e7f): variant/variant.feature
@spark-4
Feature: Additional CAST coverage from variant

  Rule: CAST to VARIANT

    Scenario: variant catalog: CAST string to variant
      When query
        """
        SELECT CAST('hello' AS VARIANT) AS result
        """
      Then query result
        | result  |
        | "hello" |

    Scenario: variant catalog: CAST integer to variant
      When query
        """
        SELECT CAST(42 AS VARIANT) AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: variant catalog: CAST null to variant
      When query
        """
        SELECT CAST(NULL AS VARIANT) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: variant catalog: CAST boolean to variant
      When query
        """
        SELECT CAST(true AS VARIANT) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: variant catalog: CAST decimal to variant
      When query
        """
        SELECT CAST(CAST(99.99 AS DECIMAL(10,2)) AS VARIANT) AS result
        """
      Then query result
        | result |
        | 99.99  |

    Scenario: variant catalog: CAST array to variant
      When query
        """
        SELECT CAST(array(1,2,3) AS VARIANT) AS result
        """
      Then query result
        | result  |
        | [1,2,3] |

  Rule: Variant NULL handling

    Scenario: variant catalog: CAST NULL AS VARIANT returns SQL NULL
      When query
        """
        SELECT CAST(NULL AS VARIANT) AS result
        """
      Then query result
        | result |
        | NULL   |
