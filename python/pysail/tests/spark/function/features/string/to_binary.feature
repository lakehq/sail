Feature: to_binary output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to to_binary yields the schema Spark declares
      When query
        """
        SELECT to_binary('abc', 'utf-8') AS result
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a non-null column input to to_binary yields the schema Spark declares
      When query
        """
        SELECT to_binary(CAST(id AS STRING), 'utf-8') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a nullable column input to to_binary stays nullable
      When query
        """
        SELECT to_binary(c, 'utf-8') AS result FROM VALUES ('abc'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

  Rule: Nullability depends on the format's replacement

    # Spark rewrites `to_binary` to a different expression per format
    # (stringExpressions.scala:3249), and each one has its own nullability: `hex` and the
    # single-argument form become `Unhex`, which is nullable, while `base64` becomes
    # `UnBase64`, which is null-intolerant and therefore follows its input.
    Scenario Outline: to_binary with format <format> is <nullable>
      When query
        """
        SELECT to_binary(<args>) AS result
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = <nullable>)
        """

      Examples:
        | format  | args               | nullable |
        | base64  | 'YWJj', 'base64'   | false    |
        | hex     | '414243', 'hex'    | true     |
        | default | '414243'           | true     |
