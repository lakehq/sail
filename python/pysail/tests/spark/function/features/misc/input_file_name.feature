Feature: input_file_name

  Rule: Inputs not backed by files

    Scenario: input_file_name returns an empty string without a file source
      When query
        """
        SELECT length(input_file_name()) AS result
        """
      Then query result collected ordered
        | result |
        | 0      |

  @function(nullability)
  Rule: Output schema

    Scenario: input_file_name returns the non-nullable string schema Spark declares
      When query
        """
        SELECT input_file_name() AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """
