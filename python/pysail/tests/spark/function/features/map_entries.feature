Feature: map_entries function

  Rule: Basic usage

    Scenario: map_entries returns entries for a non-nullable map value
      When query
        """
        SELECT
          map_entries(map(1, 'a', 2, 'b')) AS result,
          typeof(map_entries(map(1, 'a', 2, 'b'))) AS type
        """
      Then query result
        | result           | type                                |
        | [{1, a}, {2, b}] | array<struct<key:int,value:string>> |
