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

  Rule: SQL grouping

    Scenario: Input file metadata is materialized for SQL grouping
      When query
        """
        SELECT length(input_file_name()) AS name_length,
               input_file_block_start() AS block_start,
               input_file_block_length() AS block_length,
               count(*) AS row_count
        FROM range(3)
        GROUP BY input_file_name(), input_file_block_start(), input_file_block_length()
        HAVING length(input_file_name()) = 0
        """
      Then query result
        | name_length | block_start | block_length | row_count |
        | 0           | -1          | -1           | 3         |

    Scenario Outline: Ungrouped metadata remains invalid inside aggregates
      When query
        """
        SELECT max(<function>()) FROM range(3) GROUP BY id % 2
        """
      Then query error Non-deterministic expression

      Examples:
        | function                |
        | input_file_name         |
        | input_file_block_start  |
        | input_file_block_length |

    Scenario: Grouped metadata does not exempt another volatile aggregate argument
      When query
        """
        SELECT input_file_name(), max(rand(1))
        FROM range(3)
        GROUP BY input_file_name()
        """
      Then query error Non-deterministic expression

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
