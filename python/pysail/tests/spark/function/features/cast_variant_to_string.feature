@spark-4 @cast_variant_to_string
Feature: CAST(variant AS STRING)

  Spark's `CAST(variant AS STRING)` is not the same as `to_json`: it unquotes
  scalar strings and maps a VOID (JSON `null`) value to SQL NULL, while objects,
  arrays, numbers and booleans render as JSON.

  Rule: Scalar string variants cast to the raw (unquoted) string

    Scenario: a variant string casts without JSON quotes
      When query
        """
        SELECT CAST(parse_json('"hi"') AS STRING) AS result
        """
      Then query result
        | result |
        | hi     |

    Scenario: a variant string with spaces preserves the content
      When query
        """
        SELECT CAST(parse_json('"a b"') AS STRING) AS result
        """
      Then query result
        | result |
        | a b    |

  Rule: A VOID (JSON null) variant casts to SQL NULL

    Scenario: JSON null casts to SQL NULL, not the literal 'null'
      When query
        """
        SELECT CAST(parse_json('null') AS STRING) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Non-string variants render as JSON

    Scenario: a variant object casts to its JSON form
      When query
        """
        SELECT CAST(parse_json('{"a": 1}') AS STRING) AS result
        """
      Then query result
        | result  |
        | {"a":1} |

    Scenario: a variant array casts to its JSON form
      When query
        """
        SELECT CAST(parse_json('[1, 2, 3]') AS STRING) AS result
        """
      Then query result
        | result  |
        | [1,2,3] |

    Scenario: a variant integer casts to its decimal text
      When query
        """
        SELECT CAST(parse_json('5') AS STRING) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: a variant boolean casts to its lowercase text
      When query
        """
        SELECT CAST(parse_json('true') AS STRING) AS result
        """
      Then query result
        | result |
        | true   |

  Rule: The unquote and VOID-to-NULL rules hold across a column

    Scenario: a multi-row column mixes raw strings, SQL NULL, and JSON
      When query
        """
        SELECT CAST(parse_json(v) AS STRING) AS result
        FROM VALUES ('"hi"'), ('null'), ('{"a": 1}'), ('5') AS t(v)
        """
      Then query result ordered
        | result  |
        | hi      |
        | NULL    |
        | {"a":1} |
        | 5       |
