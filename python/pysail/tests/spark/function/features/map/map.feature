Feature: map output schema

  Rule: Duplicate key policy

    Scenario: duplicate keys raise an error under the default EXCEPTION policy
      When query
        """
        SELECT map(1, 'a', 1, 'b') AS result
        """
      Then query error .*\[DUPLICATED_MAP_KEY\].*

    Scenario: LAST_WIN keeps the final value at the first key position
      Given config spark.sql.mapKeyDedupPolicy = LAST_WIN
      When query
        """
        SELECT map(1, 'a', 2, 'b', 1, 'c') AS result
        """
      Then query result
        | result           |
        | {1 -> c, 2 -> b} |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to map yields the schema Spark declares
      When query
        """
        SELECT map(1.0, '2', 3.0, '4') AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: decimal(2,1)
         |    |-- value: string (valueContainsNull = false)
        """

    Scenario: a nullable column input to map stays nullable
      When query
        """
        SELECT map(c, '2', 3.0, '4') AS result FROM VALUES (1.0), (CAST(NULL AS DECIMAL(2,1))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: decimal(2,1)
         |    |-- value: string (valueContainsNull = false)
        """

  Rule: A key that mixes a string with another type widens the way the mode says

    Scenario: without ANSI mode the string wins, so the two keys stay apart
      # `TypeCoercion.stringPromotion`: the string is the wider type, and `'01'` and `'1'` are
      # two keys.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT size(map('01', 2, 1, 4)) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: with ANSI mode the number wins, so the two keys are one
      # `AnsiStringPromotionTypeCoercion.findWiderTypeForString`: the string is cast to the other
      # type, and `'01'` and `1` are then the same key, which the dedup policy keeps once.
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.mapKeyDedupPolicy = LAST_WIN
      When query
        """
        SELECT size(map('01', 2, 1, 4)) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario Outline: a string has no type in common with a <type> key
      # `TypeCoercion.stringPromotion` leaves a boolean and a binary value out, so the keys have
      # no one type and `CreateMap` refuses them.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT map(<value>, 1, 'x', 2) AS result
        """
      Then query error DATATYPE_MISMATCH.CREATE_MAP_KEY_DIFF_TYPES

      Examples:
        | type    | value |
        | boolean | true  |
        | binary  | X'01' |

    Scenario: a string has no type in common with a boolean value either
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT map(1, true, 2, 'x') AS result
        """
      Then query error DATATYPE_MISMATCH.CREATE_MAP_VALUE_DIFF_TYPES
