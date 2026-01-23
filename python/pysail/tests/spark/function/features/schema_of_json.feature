Feature: schema_of_json() returns the schema of a JSON string as DDL

  Scenario: schema_of_json with simple types
    When query
    """
    SELECT schema_of_json('{"name":"Alice","age":30,"active":true}') AS result
    """
    Then query result ordered
    | result                                            |
    | STRUCT<active: BOOLEAN, age: BIGINT, name: STRING> |

  Scenario: schema_of_json with numeric types
    When query
    """
    SELECT schema_of_json('{"id":100,"price":29.99,"count":5}') AS result
    """
    Then query result ordered
    | result                                           |
    | STRUCT<count: BIGINT, id: BIGINT, price: DOUBLE> |

  Scenario: schema_of_json with nested object
    When query
    """
    SELECT schema_of_json('{"user":{"name":"Bob","age":25},"active":true}') AS result
    """
    Then query result ordered
    | result                                                        |
    | STRUCT<active: BOOLEAN, user: STRUCT<age: BIGINT, name: STRING>> |

  Scenario: schema_of_json with array of primitives
    When query
    """
    SELECT schema_of_json('{"tags":["a","b","c"],"count":3}') AS result
    """
    Then query result ordered
    | result                                      |
    | STRUCT<count: BIGINT, tags: ARRAY<STRING>>  |

  Scenario: schema_of_json with array of objects
    When query
    """
    SELECT schema_of_json('{"items":[{"id":1,"name":"x"},{"id":2,"name":"y"}]}') AS result
    """
    Then query result ordered
    | result                                                    |
    | STRUCT<items: ARRAY<STRUCT<id: BIGINT, name: STRING>>>   |

  Scenario: schema_of_json with null values
    When query
    """
    SELECT schema_of_json('{"name":"Alice","age":null}') AS result
    """
    Then query result ordered
    | result                                  |
    | STRUCT<age: STRING, name: STRING>       |

  Scenario: schema_of_json with mixed types in array
    When query
    """
    SELECT schema_of_json('{"data":[1,2,3],"meta":{"count":3}}') AS result
    """
    Then query result ordered
    | result                                                  |
    | STRUCT<data: ARRAY<BIGINT>, meta: STRUCT<count: BIGINT>> |

  Scenario: schema_of_json with empty object
    When query
    """
    SELECT schema_of_json('{}') AS result
    """
    Then query result ordered
    | result     |
    | STRUCT<>   |

  Scenario: schema_of_json with empty array
    When query
    """
    SELECT schema_of_json('{"items":[]}') AS result
    """
    Then query result ordered
    | result                       |
    | STRUCT<items: ARRAY<STRING>> |

  Scenario: schema_of_json with deeply nested structure
    When query
    """
    SELECT schema_of_json('{"a":{"b":{"c":{"d":1}}}}') AS result
    """
    Then query result ordered
    | result                                               |
    | STRUCT<a: STRUCT<b: STRUCT<c: STRUCT<d: BIGINT>>>>  |

  Scenario: schema_of_json with boolean values
    When query
    """
    SELECT schema_of_json('{"isActive":true,"isValid":false}') AS result
    """
    Then query result ordered
    | result                                         |
    | STRUCT<isActive: BOOLEAN, isValid: BOOLEAN>    |

  Scenario: schema_of_json with string containing numbers
    When query
    """
    SELECT schema_of_json('{"id":"123","value":"456.78"}') AS result
    """
    Then query result ordered
    | result                               |
    | STRUCT<id: STRING, value: STRING>    |
