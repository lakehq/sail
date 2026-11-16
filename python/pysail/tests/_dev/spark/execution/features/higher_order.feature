Feature: Higher-order functions in distributed execution

  Scenario: Distributed zip_with resolves the deferred right array
    When query
      """
      SELECT id, zip_with(
        array(id, id + 1),
        array(CAST(id AS STRING)),
        (left_value, right_value) -> concat(CAST(left_value AS STRING), ':', coalesce(right_value, 'missing'))
      ) AS result
      FROM range(0, 2, 1, 2)
      ORDER BY id
      """
    Then query result collected ordered
      | id | result                  |
      | 0  | ['0:0', '1:missing']    |
      | 1  | ['1:1', '2:missing']    |

  Scenario: Distributed map_zip_with resolves the deferred right map
    When query
      """
      SELECT id, map_zip_with(
        map(id, id + 1),
        map(id, CAST(id AS STRING), id + 1, CAST(id + 10 AS STRING)),
        (key, left_value, right_value) -> concat(
          CAST(key AS STRING), ':', coalesce(CAST(left_value AS STRING), '-'), ':', coalesce(right_value, '-')
        )
      ) AS result
      FROM range(0, 2, 1, 2)
      ORDER BY id
      """
    Then query result collected ordered
      | id | result                          |
      | 0  | {0: '0:1:0', 1: '1:-:10'}      |
      | 1  | {1: '1:2:1', 2: '2:-:11'}      |