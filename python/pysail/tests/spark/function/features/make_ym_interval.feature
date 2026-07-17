@make_ym_interval
Feature: make_ym_interval builds a year-month interval from years and months

  Rule: A NULL in any argument yields NULL (Spark MakeYMInterval is null-intolerant)
    Scenario: NULL months yields NULL
      When query
        """
        SELECT make_ym_interval(1, NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL years yields NULL
      When query
        """
        SELECT make_ym_interval(NULL, 6) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: both arguments NULL yields NULL
      When query
        """
        SELECT make_ym_interval(NULL, NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: typed NULL argument yields NULL
      When query
        """
        SELECT make_ym_interval(CAST(NULL AS INT), 6) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL propagates per row over a column
      When query
        """
        SELECT make_ym_interval(y, m) AS result
        FROM VALUES (1, 6), (CAST(NULL AS INT), 3), (2, CAST(NULL AS INT)) AS t(y, m)
        """
      Then query result
        | result                       |
        | INTERVAL '1-6' YEAR TO MONTH |
        | NULL                         |
        | NULL                         |

  Rule: Non-NULL arguments build a year-month interval
    Scenario: years and months combine
      When query
        """
        SELECT make_ym_interval(1, 6) AS result
        """
      Then query result
        | result                       |
        | INTERVAL '1-6' YEAR TO MONTH |

    Scenario: zero years and months
      When query
        """
        SELECT make_ym_interval(0, 0) AS result
        """
      Then query result
        | result                       |
        | INTERVAL '0-0' YEAR TO MONTH |

    Scenario: negative years and months
      When query
        """
        SELECT make_ym_interval(-1, -6) AS result
        """
      Then query result
        | result                        |
        | INTERVAL '-1-6' YEAR TO MONTH |

    Scenario: months overflowing into years are normalized
      When query
        """
        SELECT make_ym_interval(2, 13) AS result
        """
      Then query result
        | result                       |
        | INTERVAL '3-1' YEAR TO MONTH |

    Scenario: negative years with positive months normalize below zero
      When query
        """
        SELECT make_ym_interval(-1, 1) AS result
        """
      Then query result
        | result                         |
        | INTERVAL '-0-11' YEAR TO MONTH |

  Rule: Omitted arguments default to zero
    Scenario: no arguments builds a zero interval
      When query
        """
        SELECT make_ym_interval() AS result
        """
      Then query result
        | result                       |
        | INTERVAL '0-0' YEAR TO MONTH |

    Scenario: single argument builds a whole-year interval
      When query
        """
        SELECT make_ym_interval(2) AS result
        """
      Then query result
        | result                       |
        | INTERVAL '2-0' YEAR TO MONTH |

    Scenario: single NULL argument yields NULL
      When query
        """
        SELECT make_ym_interval(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: More than two arguments is an error
    Scenario: three arguments is rejected
      When query
        """
        SELECT make_ym_interval(1, 2, 3) AS result
        """
      Then query error make_ym_interval

  Rule: Integer overflow is an error regardless of ANSI mode
    Scenario: overflow errors with ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_ym_interval(200000000, 0) AS result
        """
      Then query error INTERVAL_ARITHMETIC_OVERFLOW

    Scenario: overflow errors with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_ym_interval(200000000, 0) AS result
        """
      Then query error INTERVAL_ARITHMETIC_OVERFLOW
