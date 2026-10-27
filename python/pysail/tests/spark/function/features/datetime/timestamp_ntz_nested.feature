Feature: Nested TIMESTAMP_NTZ values compare with TIMESTAMP values in the session time zone
  Spark finds the common type of arrays, structs and maps element by element, so TIMESTAMP_NTZ elements become TIMESTAMP
  in the session time zone, like top-level values, including DST gaps and overlaps.

  Background:
    Given variable location for temporary directory timestamp_ntz_nested
    Given final statement
      """
      DROP TABLE IF EXISTS timestamp_ntz_nested
      """
    Given config spark.sql.session.timeZone = UTC
    Given statement template
      """
      CREATE TABLE timestamp_ntz_nested (id INT, ntz TIMESTAMP_NTZ, ltz TIMESTAMP, s STRING) USING parquet LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO timestamp_ntz_nested VALUES
        (0, TIMESTAMP_NTZ '2020-06-14 12:00:00', TIMESTAMP '2020-06-14 19:00:00', '2020-06-14 12:00:00'),
        (1, TIMESTAMP_NTZ '2020-03-08 02:30:00', TIMESTAMP '2020-03-08 10:30:00', '2020-03-08 03:30:00'),
        (2, TIMESTAMP_NTZ '2020-11-01 01:30:00', TIMESTAMP '2020-11-01 08:30:00', '2020-11-01 01:30:00'),
        (3, TIMESTAMP_NTZ '2020-11-01 01:30:00', TIMESTAMP '2020-11-01 09:30:00', '2020-11-01 01:30:00-08:00'),
        (4, TIMESTAMP_NTZ '2020-06-14 19:00:00', TIMESTAMP '2020-06-14 19:00:00', '2020-06-14 19:00:00'),
        (5, TIMESTAMP_NTZ '2020-06-15 03:00:00', TIMESTAMP '2020-06-14 19:00:00', '2020-06-15 03:00:00'),
        (6, NULL, TIMESTAMP '2020-06-14 19:00:00', NULL)
      """

  Scenario: Arrays and structs compare TIMESTAMP_NTZ with TIMESTAMP elements in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        array(ntz) = array(ltz) AS array_equals,
        array(ntz) < array(ltz) AS array_less_than,
        named_struct('t', ntz) = named_struct('t', ltz) AS struct_equals,
        named_struct('t', ntz) <= named_struct('t', ltz) AS struct_at_most,
        array(ntz) = array(CAST(s AS TIMESTAMP)) AS array_equals_session,
        named_struct('t', ntz) IN (named_struct('t', ltz), named_struct('t', TIMESTAMP '2020-01-01 00:00:00')) AS struct_in
      FROM timestamp_ntz_nested
      ORDER BY id
      """
    Then query result ordered
      | id | array_equals | array_less_than | struct_equals | struct_at_most | array_equals_session | struct_in |
      | 0  | true         | false           | true          | true           | true                 | true      |
      | 1  | true         | false           | true          | true           | true                 | true      |
      | 2  | true         | false           | true          | true           | true                 | true      |
      | 3  | false        | true            | false         | true           | false                | false     |
      | 4  | false        | false           | false         | false          | true                 | false     |
      | 5  | false        | false           | false         | false          | true                 | false     |
      | 6  | false        | true            | false         | true           | true                 | false     |

  Scenario: Arrays and structs compare TIMESTAMP_NTZ with TIMESTAMP elements in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        id,
        array(ntz) = array(ltz) AS array_equals,
        array(ntz) < array(ltz) AS array_less_than,
        named_struct('t', ntz) = named_struct('t', ltz) AS struct_equals,
        named_struct('t', ntz) <= named_struct('t', ltz) AS struct_at_most,
        array(ntz) = array(CAST(s AS TIMESTAMP)) AS array_equals_session,
        named_struct('t', ntz) IN (named_struct('t', ltz), named_struct('t', TIMESTAMP '2020-01-01 00:00:00')) AS struct_in
      FROM timestamp_ntz_nested
      ORDER BY id
      """
    Then query result ordered
      | id | array_equals | array_less_than | struct_equals | struct_at_most | array_equals_session | struct_in |
      | 0  | false        | true            | false         | true           | true                 | false     |
      | 1  | false        | true            | false         | true           | false                | false     |
      | 2  | false        | true            | false         | true           | true                 | false     |
      | 3  | false        | true            | false         | true           | false                | false     |
      | 4  | false        | true            | false         | true           | true                 | false     |
      | 5  | true         | false           | true          | true           | true                 | true      |
      | 6  | false        | true            | false         | true           | true                 | false     |

  Scenario: Arrays and structs compare TIMESTAMP_NTZ with TIMESTAMP elements in the session time zone (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT
        id,
        array(ntz) = array(ltz) AS array_equals,
        array(ntz) < array(ltz) AS array_less_than,
        named_struct('t', ntz) = named_struct('t', ltz) AS struct_equals,
        named_struct('t', ntz) <= named_struct('t', ltz) AS struct_at_most,
        array(ntz) = array(CAST(s AS TIMESTAMP)) AS array_equals_session,
        named_struct('t', ntz) IN (named_struct('t', ltz), named_struct('t', TIMESTAMP '2020-01-01 00:00:00')) AS struct_in
      FROM timestamp_ntz_nested
      ORDER BY id
      """
    Then query result ordered
      | id | array_equals | array_less_than | struct_equals | struct_at_most | array_equals_session | struct_in |
      | 0  | false        | true            | false         | true           | true                 | false     |
      | 1  | false        | true            | false         | true           | false                | false     |
      | 2  | false        | true            | false         | true           | true                 | false     |
      | 3  | false        | true            | false         | true           | false                | false     |
      | 4  | false        | true            | false         | true           | true                 | false     |
      | 5  | false        | false           | false         | false          | true                 | false     |
      | 6  | false        | true            | false         | true           | true                 | false     |

  Scenario: Arrays and structs compare TIMESTAMP_NTZ with TIMESTAMP elements in the session time zone (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT
        id,
        array(ntz) = array(ltz) AS array_equals,
        array(ntz) < array(ltz) AS array_less_than,
        named_struct('t', ntz) = named_struct('t', ltz) AS struct_equals,
        named_struct('t', ntz) <= named_struct('t', ltz) AS struct_at_most,
        array(ntz) = array(CAST(s AS TIMESTAMP)) AS array_equals_session,
        named_struct('t', ntz) IN (named_struct('t', ltz), named_struct('t', TIMESTAMP '2020-01-01 00:00:00')) AS struct_in
      FROM timestamp_ntz_nested
      ORDER BY id
      """
    Then query result ordered
      | id | array_equals | array_less_than | struct_equals | struct_at_most | array_equals_session | struct_in |
      | 0  | false        | true            | false         | true           | true                 | false     |
      | 1  | false        | true            | false         | true           | false                | false     |
      | 2  | false        | true            | false         | true           | true                 | false     |
      | 3  | false        | true            | false         | true           | false                | false     |
      | 4  | true         | false           | true          | true           | true                 | true      |
      | 5  | false        | false           | false         | false          | true                 | false     |
      | 6  | false        | true            | false         | true           | true                 | false     |

  Scenario: Array functions compare TIMESTAMP_NTZ with TIMESTAMP elements in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        array_contains(array(ntz), ltz) AS contains_timestamp,
        array_contains(array(ltz), ntz) AS contains_ntz,
        arrays_overlap(array(ntz), array(ltz)) AS overlap,
        size(array_except(array(ntz), array(ltz))) AS except_size,
        size(array_union(array(ntz), array(ltz))) AS union_size,
        size(array_intersect(array(ntz), array(ltz))) AS intersect_size
      FROM timestamp_ntz_nested
      WHERE ntz IS NOT NULL
      ORDER BY id
      """
    Then query result ordered
      | id | contains_timestamp | contains_ntz | overlap | except_size | union_size | intersect_size |
      | 0  | true               | true         | true    | 0           | 1          | 1              |
      | 1  | true               | true         | true    | 0           | 1          | 1              |
      | 2  | true               | true         | true    | 0           | 1          | 1              |
      | 3  | false              | false        | false   | 1           | 2          | 0              |
      | 4  | false              | false        | false   | 1           | 2          | 0              |
      | 5  | false              | false        | false   | 1           | 2          | 0              |

  Scenario: Array functions compare TIMESTAMP_NTZ with TIMESTAMP elements in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        id,
        array_contains(array(ntz), ltz) AS contains_timestamp,
        array_contains(array(ltz), ntz) AS contains_ntz,
        arrays_overlap(array(ntz), array(ltz)) AS overlap,
        size(array_except(array(ntz), array(ltz))) AS except_size,
        size(array_union(array(ntz), array(ltz))) AS union_size,
        size(array_intersect(array(ntz), array(ltz))) AS intersect_size
      FROM timestamp_ntz_nested
      WHERE ntz IS NOT NULL
      ORDER BY id
      """
    Then query result ordered
      | id | contains_timestamp | contains_ntz | overlap | except_size | union_size | intersect_size |
      | 0  | false              | false        | false   | 1           | 2          | 0              |
      | 1  | false              | false        | false   | 1           | 2          | 0              |
      | 2  | false              | false        | false   | 1           | 2          | 0              |
      | 3  | false              | false        | false   | 1           | 2          | 0              |
      | 4  | false              | false        | false   | 1           | 2          | 0              |
      | 5  | true               | true         | true    | 0           | 1          | 1              |

  Scenario: Array functions compare TIMESTAMP_NTZ with TIMESTAMP elements in the session time zone (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT
        id,
        array_contains(array(ntz), ltz) AS contains_timestamp,
        array_contains(array(ltz), ntz) AS contains_ntz,
        arrays_overlap(array(ntz), array(ltz)) AS overlap,
        size(array_except(array(ntz), array(ltz))) AS except_size,
        size(array_union(array(ntz), array(ltz))) AS union_size,
        size(array_intersect(array(ntz), array(ltz))) AS intersect_size
      FROM timestamp_ntz_nested
      WHERE ntz IS NOT NULL
      ORDER BY id
      """
    Then query result ordered
      | id | contains_timestamp | contains_ntz | overlap | except_size | union_size | intersect_size |
      | 0  | false              | false        | false   | 1           | 2          | 0              |
      | 1  | false              | false        | false   | 1           | 2          | 0              |
      | 2  | false              | false        | false   | 1           | 2          | 0              |
      | 3  | false              | false        | false   | 1           | 2          | 0              |
      | 4  | false              | false        | false   | 1           | 2          | 0              |
      | 5  | false              | false        | false   | 1           | 2          | 0              |

  Scenario: Array functions compare TIMESTAMP_NTZ with TIMESTAMP elements in the session time zone (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT
        id,
        array_contains(array(ntz), ltz) AS contains_timestamp,
        array_contains(array(ltz), ntz) AS contains_ntz,
        arrays_overlap(array(ntz), array(ltz)) AS overlap,
        size(array_except(array(ntz), array(ltz))) AS except_size,
        size(array_union(array(ntz), array(ltz))) AS union_size,
        size(array_intersect(array(ntz), array(ltz))) AS intersect_size
      FROM timestamp_ntz_nested
      WHERE ntz IS NOT NULL
      ORDER BY id
      """
    Then query result ordered
      | id | contains_timestamp | contains_ntz | overlap | except_size | union_size | intersect_size |
      | 0  | false              | false        | false   | 1           | 2          | 0              |
      | 1  | false              | false        | false   | 1           | 2          | 0              |
      | 2  | false              | false        | false   | 1           | 2          | 0              |
      | 3  | false              | false        | false   | 1           | 2          | 0              |
      | 4  | true               | true         | true    | 0           | 1          | 1              |
      | 5  | false              | false        | false   | 1           | 2          | 0              |

  Scenario: Nested TIMESTAMP_NTZ values widen to TIMESTAMP in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        transform(array(ntz, ltz), x -> unix_seconds(x) - unix_seconds(ltz)) AS array_offsets,
        unix_seconds(named_struct('t', coalesce(ntz, ltz)).t) - unix_seconds(ltz) AS struct_coalesce_offset,
        array(coalesce(ntz, ltz)) = array(ltz) AS array_coalesce_equals,
        transform(array_union(array(ntz), array(ltz)), x -> unix_seconds(x) - unix_seconds(ltz)) AS union_offsets,
        unix_seconds(map_concat(map('n', ntz), map('l', ltz))['n']) - unix_seconds(ltz) AS map_value_offset,
        map_contains_key(map(coalesce(ntz, TIMESTAMP_NTZ '2000-01-01 00:00:00'), 1), ltz) AS map_key_contains
      FROM timestamp_ntz_nested
      ORDER BY id
      """
    Then query result ordered
      | id | array_offsets | struct_coalesce_offset | array_coalesce_equals | union_offsets | map_value_offset | map_key_contains |
      | 0  | [0, 0]        | 0                      | true                  | [0]           | 0                | true             |
      | 1  | [0, 0]        | 0                      | true                  | [0]           | 0                | true             |
      | 2  | [0, 0]        | 0                      | true                  | [0]           | 0                | true             |
      | 3  | [-3600, 0]    | -3600                  | false                 | [-3600, 0]    | -3600            | false            |
      | 4  | [25200, 0]    | 25200                  | false                 | [25200, 0]    | 25200            | false            |
      | 5  | [54000, 0]    | 54000                  | false                 | [54000, 0]    | 54000            | false            |
      | 6  | [NULL, 0]     | 0                      | true                  | [NULL, 0]     | NULL             | false            |

  Scenario: Nested TIMESTAMP_NTZ values widen to TIMESTAMP in the session time zone (+05:30)
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT
        id,
        transform(array(ntz, ltz), x -> unix_seconds(x) - unix_seconds(ltz)) AS array_offsets,
        unix_seconds(named_struct('t', coalesce(ntz, ltz)).t) - unix_seconds(ltz) AS struct_coalesce_offset,
        array(coalesce(ntz, ltz)) = array(ltz) AS array_coalesce_equals,
        transform(array_union(array(ntz), array(ltz)), x -> unix_seconds(x) - unix_seconds(ltz)) AS union_offsets,
        unix_seconds(map_concat(map('n', ntz), map('l', ltz))['n']) - unix_seconds(ltz) AS map_value_offset,
        map_contains_key(map(coalesce(ntz, TIMESTAMP_NTZ '2000-01-01 00:00:00'), 1), ltz) AS map_key_contains
      FROM timestamp_ntz_nested
      ORDER BY id
      """
    Then query result ordered
      | id | array_offsets | struct_coalesce_offset | array_coalesce_equals | union_offsets | map_value_offset | map_key_contains |
      | 0  | [-45000, 0]   | -45000                 | false                 | [-45000, 0]   | -45000           | false            |
      | 1  | [-48600, 0]   | -48600                 | false                 | [-48600, 0]   | -48600           | false            |
      | 2  | [-45000, 0]   | -45000                 | false                 | [-45000, 0]   | -45000           | false            |
      | 3  | [-48600, 0]   | -48600                 | false                 | [-48600, 0]   | -48600           | false            |
      | 4  | [-19800, 0]   | -19800                 | false                 | [-19800, 0]   | -19800           | false            |
      | 5  | [9000, 0]     | 9000                   | false                 | [9000, 0]     | 9000             | false            |
      | 6  | [NULL, 0]     | 0                      | true                  | [NULL, 0]     | NULL             | false            |

  Scenario: CASE, COALESCE and IF widen nested TIMESTAMP_NTZ values in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        unix_seconds(coalesce(named_struct('t', ntz), named_struct('t', ltz)).t) - unix_seconds(ltz) AS coalesce_struct_offset,
        unix_seconds(if(id >= 0, named_struct('t', ntz), named_struct('t', ltz)).t) - unix_seconds(ltz) AS if_struct_offset,
        unix_seconds((CASE WHEN id >= 0 THEN array(ntz) ELSE array(ltz) END)[0]) - unix_seconds(ltz) AS case_array_offset,
        unix_seconds(array(array(ntz), array(ltz))[0][0]) - unix_seconds(ltz) AS nested_array_offset,
        CAST(coalesce(named_struct('t', ntz), named_struct('t', ltz)) AS STRING) AS coalesce_struct_string
      FROM timestamp_ntz_nested
      ORDER BY id
      """
    Then query result ordered
      | id | coalesce_struct_offset | if_struct_offset | case_array_offset | nested_array_offset | coalesce_struct_string |
      | 0  | 0                      | 0                | 0                 | 0                   | {2020-06-14 12:00:00}  |
      | 1  | 0                      | 0                | 0                 | 0                   | {2020-03-08 03:30:00}  |
      | 2  | 0                      | 0                | 0                 | 0                   | {2020-11-01 01:30:00}  |
      | 3  | -3600                  | -3600            | -3600             | -3600               | {2020-11-01 01:30:00}  |
      | 4  | 25200                  | 25200            | 25200             | 25200               | {2020-06-14 19:00:00}  |
      | 5  | 54000                  | 54000            | 54000             | 54000               | {2020-06-15 03:00:00}  |
      | 6  | NULL                   | NULL             | NULL              | NULL                | {null}                 |

  Scenario: CASE, COALESCE and IF widen nested TIMESTAMP_NTZ values in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        id,
        unix_seconds(coalesce(named_struct('t', ntz), named_struct('t', ltz)).t) - unix_seconds(ltz) AS coalesce_struct_offset,
        unix_seconds(if(id >= 0, named_struct('t', ntz), named_struct('t', ltz)).t) - unix_seconds(ltz) AS if_struct_offset,
        unix_seconds((CASE WHEN id >= 0 THEN array(ntz) ELSE array(ltz) END)[0]) - unix_seconds(ltz) AS case_array_offset,
        unix_seconds(array(array(ntz), array(ltz))[0][0]) - unix_seconds(ltz) AS nested_array_offset,
        CAST(coalesce(named_struct('t', ntz), named_struct('t', ltz)) AS STRING) AS coalesce_struct_string
      FROM timestamp_ntz_nested
      ORDER BY id
      """
    Then query result ordered
      | id | coalesce_struct_offset | if_struct_offset | case_array_offset | nested_array_offset | coalesce_struct_string |
      | 0  | -54000                 | -54000           | -54000            | -54000              | {2020-06-14 12:00:00}  |
      | 1  | -57600                 | -57600           | -57600            | -57600              | {2020-03-08 02:30:00}  |
      | 2  | -54000                 | -54000           | -54000            | -54000              | {2020-11-01 01:30:00}  |
      | 3  | -57600                 | -57600           | -57600            | -57600              | {2020-11-01 01:30:00}  |
      | 4  | -28800                 | -28800           | -28800            | -28800              | {2020-06-14 19:00:00}  |
      | 5  | 0                      | 0                | 0                 | 0                   | {2020-06-15 03:00:00}  |
      | 6  | NULL                   | NULL             | NULL              | NULL                | {null}                 |

  Scenario: UNION widens nested TIMESTAMP_NTZ values in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        u.id,
        unix_seconds(u.s.t) - unix_seconds(n.ltz) AS union_struct_offset,
        u.s.t = n.ltz AS union_struct_equals,
        CAST(u.s AS STRING) AS union_struct_string
      FROM (SELECT id, named_struct('t', ntz) AS s FROM timestamp_ntz_nested UNION ALL SELECT id + 100, named_struct('t', ltz) FROM timestamp_ntz_nested) u
      JOIN timestamp_ntz_nested n ON u.id = n.id
      ORDER BY u.id
      """
    Then query result ordered
      | id | union_struct_offset | union_struct_equals | union_struct_string   |
      | 0  | 0                   | true                | {2020-06-14 12:00:00} |
      | 1  | 0                   | true                | {2020-03-08 03:30:00} |
      | 2  | 0                   | true                | {2020-11-01 01:30:00} |
      | 3  | -3600               | false               | {2020-11-01 01:30:00} |
      | 4  | 25200               | false               | {2020-06-14 19:00:00} |
      | 5  | 54000               | false               | {2020-06-15 03:00:00} |
      | 6  | NULL                | NULL                | {null}                |

  Scenario: UNION widens nested TIMESTAMP_NTZ values in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        u.id,
        unix_seconds(u.s.t) - unix_seconds(n.ltz) AS union_struct_offset,
        u.s.t = n.ltz AS union_struct_equals,
        CAST(u.s AS STRING) AS union_struct_string
      FROM (SELECT id, named_struct('t', ntz) AS s FROM timestamp_ntz_nested UNION ALL SELECT id + 100, named_struct('t', ltz) FROM timestamp_ntz_nested) u
      JOIN timestamp_ntz_nested n ON u.id = n.id
      ORDER BY u.id
      """
    Then query result ordered
      | id | union_struct_offset | union_struct_equals | union_struct_string   |
      | 0  | -54000              | false               | {2020-06-14 12:00:00} |
      | 1  | -57600              | false               | {2020-03-08 02:30:00} |
      | 2  | -54000              | false               | {2020-11-01 01:30:00} |
      | 3  | -57600              | false               | {2020-11-01 01:30:00} |
      | 4  | -28800              | false               | {2020-06-14 19:00:00} |
      | 5  | 0                   | true                | {2020-06-15 03:00:00} |
      | 6  | NULL                | NULL                | {null}                |
