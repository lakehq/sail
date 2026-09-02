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

    Scenario: input file block functions return minus one without a file source
      When query
        """
        SELECT input_file_block_start() AS block_start,
               input_file_block_length() AS block_length
        """
      Then query result collected ordered
        | block_start | block_length |
        | -1          | -1           |

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

    Scenario: input file block functions return non-nullable longs
      When query
        """
        SELECT input_file_block_start() AS block_start,
               input_file_block_length() AS block_length
        """
      Then query schema
        """
        root
         |-- block_start: long (nullable = false)
         |-- block_length: long (nullable = false)
        """
