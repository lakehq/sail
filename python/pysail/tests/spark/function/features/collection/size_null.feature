Feature: size and cardinality null handling

  Scenario Outline: Nullable collections with ANSI <ansi> and legacy size of null <legacy>
    Given config spark.sql.ansi.enabled = <ansi>
    And config spark.sql.legacy.sizeOfNull = <legacy>
    When query
      """
      SELECT size(a) AS array_size, cardinality(a) AS array_cardinality,
             size(m) AS map_size, cardinality(m) AS map_cardinality
      FROM VALUES
        (0, CAST(NULL AS ARRAY<INT>), CAST(NULL AS MAP<INT, INT>)),
        (1, array(), map()),
        (2, array(1, NULL, 3), map(1, NULL, 3, 4))
      AS t(id, a, m)
      ORDER BY id
      """
    Then query result ordered
      | array_size | array_cardinality | map_size | map_cardinality |
      | <null>     | <null>            | <null>   | <null>          |
      | 0          | 0                 | 0        | 0               |
      | 3          | 3                 | 2        | 2               |
    And query schema
      """
      root
       |-- array_size: integer (nullable = <nullable>)
       |-- array_cardinality: integer (nullable = <nullable>)
       |-- map_size: integer (nullable = <nullable>)
       |-- map_cardinality: integer (nullable = <nullable>)
      """

    Examples:
      | ansi  | legacy           | null | nullable |
      | false | true             | -1   | false    |
      | false | false            | NULL | true     |
      | true  | true             | NULL | true     |
      | true  | false            | NULL | true     |
      | false | {{ ' TrUe ' }}  | -1   | false    |
      | false | {{ ' FaLsE ' }} | NULL | true     |

  Scenario: Legacy size and cardinality of null collection literals are non-null integers
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.legacy.sizeOfNull = true
    When query
      """
      SELECT size(CAST(NULL AS ARRAY<INT>)) AS array_size,
             cardinality(CAST(NULL AS ARRAY<INT>)) AS array_cardinality,
             size(CAST(NULL AS MAP<INT, INT>)) AS map_size,
             cardinality(CAST(NULL AS MAP<INT, INT>)) AS map_cardinality
      """
    Then query result
      | array_size | array_cardinality | map_size | map_cardinality |
      | -1         | -1                | -1       | -1              |
    And query schema
      """
      root
       |-- array_size: integer (nullable = false)
       |-- array_cardinality: integer (nullable = false)
       |-- map_size: integer (nullable = false)
       |-- map_cardinality: integer (nullable = false)
      """
