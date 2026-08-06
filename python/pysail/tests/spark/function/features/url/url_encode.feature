Feature: url_encode output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to url_encode yields the schema Spark declares
      When query
        """
        SELECT url_encode('https://spark.apache.org') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null column input to url_encode yields the schema Spark declares
      When query
        """
        SELECT url_encode(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable column input to url_encode stays nullable
      When query
        """
        SELECT url_encode(c) AS result FROM VALUES ('https://spark.apache.org'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  Rule: Result values (migrated from test_url_encode.txt doctests)

    Scenario: url_encode doctest #1 (result)
      When query
        """
        select url_encode('https://spark.apache.org')
        """
      Then query result
        | url_encode(https://spark.apache.org) |
        | https%3A%2F%2Fspark.apache.org       |

    Scenario: url_encode doctest #2 (result)
      When query
        """
        select url_encode('inva lid://user:pass@host/file\;param?query\;p2')
        """
      Then query result
        | url_encode(inva lid://user:pass@host/file;param?query;p2)       |
        | inva+lid%3A%2F%2Fuser%3Apass%40host%2Ffile%3Bparam%3Fquery%3Bp2 |

    Scenario: url_encode doctest #3 (result)
      When query
        """
        select url_encode(null)
        """
      Then query result
        | url_encode(NULL) |
        | NULL             |
