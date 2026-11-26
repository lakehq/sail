# SPDX-License-Identifier: Apache-2.0

Feature: Window RANGE frame with nested ORDER BY keys

  Rule: Default RANGE frame treats rows with equal nested keys as peers

    Scenario: sum over struct and array ORDER BY keys with ties and NULLs
      When query
        """
        SELECT
          g,
          v,
          SUM(v) OVER (PARTITION BY g ORDER BY s) AS w_s,
          SUM(v) OVER (PARTITION BY g ORDER BY s DESC NULLS LAST) AS w_s_desc,
          SUM(v) OVER (PARTITION BY g ORDER BY sa) AS w_sa,
          SUM(v) OVER (PARTITION BY g ORDER BY a, s) AS w_a_s,
          SUM(v) OVER (PARTITION BY g ORDER BY a, l) AS w_a_l,
          SUM(v) OVER (PARTITION BY g ORDER BY i, l) AS w_i_l
        FROM VALUES
          (1, 1, 10, named_struct('x', 2, 'y', 'b'), array(2, 1), named_struct('k', array(2, 1)), INTERVAL '1' DAY),
          (1, 1, 20, named_struct('x', 2, 'y', 'b'), array(2, 1), named_struct('k', array(2, 1)), INTERVAL '1' DAY),
          (1, 1, 40, named_struct('x', 3, 'y', 'a'), array(3), named_struct('k', array(3)), INTERVAL '1' DAY),
          (1, 1, 80, named_struct('x', 1, 'y', 'z'), array(1, 9), named_struct('k', array(1, 9)), INTERVAL '1' DAY),
          (1, 2, 160, named_struct('x', 0, 'y', 'a'), array(0), named_struct('k', array(0)), INTERVAL '2' DAY),
          (2, 1, 1, CAST(NULL AS STRUCT<x: INT, y: STRING>), CAST(NULL AS ARRAY<INT>), CAST(NULL AS STRUCT<k: ARRAY<INT>>), INTERVAL '1' DAY),
          (2, 1, 2, named_struct('x', 1, 'y', 'a'), array(1), named_struct('k', array(1)), INTERVAL '1' DAY),
          (2, 1, 4, named_struct('x', 1, 'y', CAST(NULL AS STRING)), array(1, NULL), named_struct('k', CAST(NULL AS ARRAY<INT>)), INTERVAL '1' DAY)
          AS t(g, a, v, s, l, sa, i)
        ORDER BY g, v
        """
      Then query result ordered
        | g | v   | w_s | w_s_desc | w_sa | w_a_s | w_a_l | w_i_l |
        | 1 | 10  | 270 | 70       | 270  | 110   | 110   | 110   |
        | 1 | 20  | 270 | 70       | 270  | 110   | 110   | 110   |
        | 1 | 40  | 310 | 40       | 310  | 150   | 150   | 150   |
        | 1 | 80  | 240 | 150      | 240  | 80    | 80    | 80    |
        | 1 | 160 | 160 | 310      | 160  | 310   | 310   | 310   |
        | 2 | 1   | 1   | 7        | 1    | 1     | 1     | 1     |
        | 2 | 2   | 7   | 2        | 7    | 7     | 3     | 3     |
        | 2 | 4   | 5   | 6        | 5    | 5     | 7     | 7     |
