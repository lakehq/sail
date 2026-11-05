Feature: months_between output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to months_between yields the schema Spark declares
      When query
        """
        SELECT months_between('1997-02-28 10:30:00', '1996-10-30') AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to months_between yields the schema Spark declares
      When query
        """
        SELECT months_between(CAST(id AS STRING), '1996-10-30') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to months_between stays nullable
      When query
        """
        SELECT months_between(c, '1996-10-30') AS result FROM VALUES ('1997-02-28 10:30:00'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
