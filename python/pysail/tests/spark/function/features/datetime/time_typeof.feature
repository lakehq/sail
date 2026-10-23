Feature: typeof reports the declared precision of a TIME

  # `main` prints the declared precision (time(3)); a TIME is now stored as microseconds whatever
  # its precision, and the declared precision travels as field metadata that `typeof` must read.
  Rule: typeof of TIME(n) is time(n)

    Scenario Outline: typeof of a cast to TIME(<p>)
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(CAST('12:34:56.789' AS TIME(<p>))) AS r
        """
      Then query result ordered
        | r         |
        | time(<p>) |

      Examples:
        | p |
        | 0 |
        | 3 |
        | 6 |

    Scenario Outline: typeof of an array element of TIME(<p>)
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(array(CAST('12:34:56.789' AS TIME(<p>)))[0]) AS r
        """
      Then query result ordered
        | r         |
        | time(<p>) |

      Examples:
        | p |
        | 0 |
        | 3 |
        | 6 |
