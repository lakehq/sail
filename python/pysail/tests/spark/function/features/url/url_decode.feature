Feature: url_decode output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to url_decode yields the schema Spark declares
      When query
        """
        SELECT url_decode('https%3A%2F%2Fspark.apache.org') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null column input to url_decode yields the schema Spark declares
      When query
        """
        SELECT url_decode(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable column input to url_decode stays nullable
      When query
        """
        SELECT url_decode(c) AS result FROM VALUES ('https%3A%2F%2Fspark.apache.org'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  Rule: Result values (migrated from test_url_decode.txt doctests)

    Scenario: url_decode doctest #1 (result)
      When query
        """
        select url_decode('https%3A%2F%2Fspark.apache.org')
        """
      Then query result
        | url_decode(https%3A%2F%2Fspark.apache.org) |
        | https://spark.apache.org                   |

    Scenario: url_decode doctest #2 (result)
      When query
        """
        select url_decode('inva lid://user:pass@host/file\;param?query\;p2')
        """
      Then query result
        | url_decode(inva lid://user:pass@host/file;param?query;p2) |
        | inva lid://user:pass@host/file;param?query;p2             |

    Scenario: url_decode doctest #3 (result)
      When query
        """
        select url_decode(null)
        """
      Then query result
        | url_decode(NULL) |
        | NULL             |
