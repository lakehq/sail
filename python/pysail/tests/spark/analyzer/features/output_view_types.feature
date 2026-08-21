Feature: View types are expanded only at query output

  Background:
    Given variable location for temporary directory explain_parquet_view_types
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ location.sql }}
      USING parquet
      SELECT
        input.row_id,
        'top-level' AS label,
        named_struct(
          'name', 'nested',
          'aliases', array('left', 'right'),
          'payload', CAST('bytes' AS BINARY),
          'strings', array('a', 'b'),
          'binary_values', array(CAST('a' AS BINARY), CAST('b' AS BINARY)),
          'binary_map', map(CAST('a' AS BINARY), CAST('b' AS BINARY)),
          'hll', sketches.hll,
          'theta', sketches.theta,
          'bitmap', sketches.bitmap,
          'wkb', to_binary(
            '0101000000000000000000F03F0000000000000040',
            'hex'
          )
        ) AS details,
        map('first', 'value') AS tags,
        '1,2' AS csv_text,
        '10' AS conv_text,
        input.hex_text,
        '#,###.##' AS number_pattern
      FROM VALUES (0, '10'), (1, '11') AS input(row_id, hex_text)
      CROSS JOIN (
        SELECT
          hll_sketch_agg(id) AS hll,
          theta_sketch_agg(id) AS theta,
          bitmap_construct_agg(bitmap_bit_position(id)) AS bitmap
        FROM range(3)
      ) AS sketches
      """

  Scenario: EXPLAIN FORMATTED shows view types below the output boundary
    When query template
      """
      EXPLAIN FORMATTED
      SELECT label, details, tags
      FROM parquet.`{{ location.string }}`
      WHERE row_id = 0
      ORDER BY label
      """
    Then query plan matches snapshot

  Scenario: Parquet view strings are accepted by custom string and hash functions
    When query template
      """
      SELECT
        regexp_extract(label, '(top)-(level)', 2) AS extracted,
        regexp_extract_all(label, '(top)-(level)', 1) AS extracted_all,
        split(label, '-')[1] AS split_part,
        from_csv(csv_text, 'a INT, b INT').a AS csv_a,
        from_csv(
          csv_text,
          'a INT, b INT',
          map('mode', 'PERMISSIVE')
        ).b AS csv_b,
        conv(conv_text, 10, 16) AS converted,
        hash(label) AS hashed,
        hash(details.payload) AS binary_hashed
      FROM parquet.`{{ location.string }}`
      WHERE row_id = 0
      """
    Then query result
      | extracted | extracted_all | split_part | csv_a | csv_b | converted | hashed     | binary_hashed |
      | level     | [top]         | level      | 1     | 2     | A         | -835272491 | 2065139274    |

  Scenario: Parquet view strings are accepted by hex decoding functions
    When query template
      """
      SELECT
        hex(unhex(hex_text)) AS unhex_value,
        hex(to_binary(hex_text)) AS to_binary_value,
        hex(to_binary(hex_text, 'hex')) AS to_binary_hex_value,
        hex(try_to_binary(hex_text)) AS try_to_binary_value,
        hex(try_to_binary(hex_text, 'hex')) AS try_to_binary_hex_value
      FROM parquet.`{{ location.string }}`
      ORDER BY row_id
      """
    Then query result
      | unhex_value | to_binary_value | to_binary_hex_value | try_to_binary_value | try_to_binary_hex_value |
      | 10          | 10              | 10                  | 10                  | 10                      |
      | 11          | 11              | 11                  | 11                  | 11                      |

  Scenario: A scalarized Parquet view pattern is accepted by format_number
    When query template
      """
      SELECT format_number(1234.5, number_pattern) AS formatted
      FROM parquet.`{{ location.string }}`
      ORDER BY row_id
      """
    Then query result
      | formatted |
      | 1,234.5   |
      | 1,234.5   |

  Scenario: Parquet string arrays combine with regular string literals
    When query template
      """
      SELECT
        array_intersect(details.strings, array('b', 'c')) AS intersection,
        concat(details.strings, array('c')) AS concatenated
      FROM parquet.`{{ location.string }}`
      WHERE row_id = 0
      """
    Then query result
      | intersection | concatenated |
      | [b]          | [a, b, c]    |

  Scenario: Nested Parquet binary fields are accepted by sketch aggregates
    When query template
      """
      SELECT
        hll_sketch_agg(details.payload) IS NOT NULL AS hll_agg_ok,
        theta_sketch_agg(details.payload) IS NOT NULL AS theta_agg_ok,
        count_min_sketch(details.payload, 0.5d, 0.5d, 1) IS NOT NULL AS count_min_ok,
        hll_union_agg(details.hll) IS NOT NULL AS hll_union_agg_ok,
        theta_union_agg(details.theta) IS NOT NULL AS theta_union_agg_ok,
        theta_intersection_agg(details.theta) IS NOT NULL AS theta_intersection_agg_ok,
        bitmap_and_agg(details.bitmap) IS NOT NULL AS bitmap_and_ok,
        bitmap_or_agg(details.bitmap) IS NOT NULL AS bitmap_or_ok,
        approx_count_distinct(details.payload) AS approx_distinct
      FROM parquet.`{{ location.string }}`
      """
    Then query result
      | hll_agg_ok | theta_agg_ok | count_min_ok | hll_union_agg_ok | theta_union_agg_ok | theta_intersection_agg_ok | bitmap_and_ok | bitmap_or_ok | approx_distinct |
      | true       | true         | true         | true             | true               | true                      | true          | true         | 1               |

  Scenario: Nested Parquet binary fields are accepted by sketch scalar functions
    When query template
      """
      SELECT
        hll_sketch_estimate(details.hll) IS NOT NULL AS hll_estimate_ok,
        hll_union(details.hll, details.hll) IS NOT NULL AS hll_union_ok,
        theta_sketch_estimate(details.theta) IS NOT NULL AS theta_estimate_ok,
        theta_union(details.theta, details.theta) IS NOT NULL AS theta_union_ok,
        theta_intersection(details.theta, details.theta) IS NOT NULL AS theta_intersection_ok,
        theta_difference(details.theta, details.theta) IS NOT NULL AS theta_difference_ok,
        hex(details.payload) IS NOT NULL AS hex_ok,
        to_char(details.payload, 'hex') IS NOT NULL AS to_char_ok,
        to_varchar(details.payload, 'hex') IS NOT NULL AS to_varchar_ok,
        coalesce(details.payload, CAST('fallback' AS BINARY)) IS NOT NULL AS coalesce_ok
      FROM parquet.`{{ location.string }}`
      WHERE row_id = 0
      """
    Then query result
      | hll_estimate_ok | hll_union_ok | theta_estimate_ok | theta_union_ok | theta_intersection_ok | theta_difference_ok | hex_ok | to_char_ok | to_varchar_ok | coalesce_ok |
      | true            | true         | true              | true           | true                  | true                | true   | true       | true          | true        |

  Scenario: Nested Parquet binary fields are accepted by approximate distinct windows
    When query template
      """
      SELECT max(estimate) AS estimate
      FROM (
        SELECT approx_count_distinct(details.payload) OVER () AS estimate
        FROM parquet.`{{ location.string }}`
      )
      """
    Then query result
      | estimate |
      | 1        |

  Scenario: Nested Parquet binary arrays and maps combine with regular literals
    When query template
      """
      SELECT
        array_prepend(details.binary_values, CAST('c' AS BINARY)) IS NOT NULL AS prepend_ok,
        array_append(details.binary_values, CAST('c' AS BINARY)) IS NOT NULL AS append_ok,
        array_insert(details.binary_values, 2, CAST('c' AS BINARY)) IS NOT NULL AS insert_ok,
        array_remove(details.binary_values, CAST('a' AS BINARY)) IS NOT NULL AS remove_ok,
        array_union(details.binary_values, array(CAST('c' AS BINARY))) IS NOT NULL AS union_ok,
        array_contains(details.binary_values, CAST('a' AS BINARY)) AS contains_ok,
        array_except(
          CAST(details.binary_values AS ARRAY<BINARY>),
          array(CAST('a' AS BINARY), CAST(NULL AS BINARY))
        ) IS NOT NULL AS except_ok,
        concat(details.binary_values, array(CAST('c' AS BINARY))) IS NOT NULL AS concat_ok,
        map_concat(
          details.binary_map,
          map(CAST('c' AS BINARY), CAST('d' AS BINARY))
        ) IS NOT NULL AS map_concat_ok,
        map_contains_key(details.binary_map, CAST('a' AS BINARY)) AS map_contains_ok
      FROM parquet.`{{ location.string }}`
      WHERE row_id = 0
      """
    Then query result
      | prepend_ok | append_ok | insert_ok | remove_ok | union_ok | contains_ok | except_ok | concat_ok | map_concat_ok | map_contains_ok |
      | true       | true      | true      | true      | true     | true        | true      | true      | true          | true            |

  Scenario: Nested Parquet binary fields preserve spatial and concat behavior
    When query template
      """
      SELECT
        st_geomfromwkb(details.wkb) IS NOT NULL AS geometry_ok,
        st_geogfromwkb(details.wkb) IS NOT NULL AS geography_ok,
        typeof(concat(details.payload, details.payload)) AS concat_type
      FROM parquet.`{{ location.string }}`
      WHERE row_id = 0
      """
    Then query result
      | geometry_ok | geography_ok | concat_type |
      | true        | true         | binary      |
