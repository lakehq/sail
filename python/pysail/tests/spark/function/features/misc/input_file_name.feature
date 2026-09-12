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

    Scenario: input file metadata can be used in windows without a file source
      When query
        """
        SELECT id,
               row_number() OVER (PARTITION BY input_file_name() ORDER BY id) AS file_row,
               row_number() OVER (PARTITION BY input_file_block_start() ORDER BY id) AS start_row,
               row_number() OVER (PARTITION BY input_file_block_length() ORDER BY id) AS length_row
        FROM range(3)
        ORDER BY id
        """
      Then query result collected ordered
        | id | file_row | start_row | length_row |
        | 0  | 1        | 1         | 1          |
        | 1  | 2        | 2         | 2          |
        | 2  | 3        | 3         | 3          |

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
