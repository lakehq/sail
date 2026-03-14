Feature: VARIANT data type support

  Rule: VARIANT as SQL data type

    Scenario: CAST NULL to VARIANT
      When query
      """
      SELECT CAST(NULL AS VARIANT) AS result
      """
      Then query schema
      """
      root
       |-- result: variant (nullable = true)
      """

    Scenario: CAST NULL to VARIANT returns NULL value
      When query
      """
      SELECT CAST(NULL AS VARIANT) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: VARIANT in DDL schema

    Scenario: create and query table with VARIANT column
      Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW variant_view AS
      SELECT CAST(NULL AS VARIANT) AS v
      """
      When query
      """
      SELECT * FROM variant_view
      """
      Then query schema
      """
      root
       |-- v: variant (nullable = true)
      """

  Rule: VARIANT function errors

    Scenario: parse_json is not yet supported
      When query
      """
      SELECT parse_json('{"a": 1}') AS result
      """
      Then query error function: parse_json

    Scenario: try_parse_json is not yet supported
      When query
      """
      SELECT try_parse_json('{"a": 1}') AS result
      """
      Then query error function: try_parse_json

    Scenario: variant_get is not yet supported
      When query
      """
      SELECT variant_get(CAST(NULL AS VARIANT), '$.key', 'string') AS result
      """
      Then query error function: variant_get

    Scenario: is_variant_null is not yet supported
      When query
      """
      SELECT is_variant_null(CAST(NULL AS VARIANT)) AS result
      """
      Then query error function: is_variant_null

    Scenario: schema_of_variant is not yet supported
      When query
      """
      SELECT schema_of_variant(CAST(NULL AS VARIANT)) AS result
      """
      Then query error function: schema_of_variant
