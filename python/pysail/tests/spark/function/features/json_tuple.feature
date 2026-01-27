Feature: json_tuple() extracts multiple values from JSON strings

  Rule: Basic value extraction

    Scenario: json_tuple extracts simple string values
      When query
      """
      SELECT c0, c1 FROM (SELECT json_tuple('{"name":"Alice","age":"30"}', 'name', 'age') AS (c0, c1))
      """
      Then query result ordered
      | c0    | c1 |
      | Alice | 30 |

    Scenario: json_tuple extracts numeric values as strings
      When query
      """
      SELECT c0, c1, c2 FROM (SELECT json_tuple('{"a":1,"b":2,"c":3}', 'a', 'b', 'c') AS (c0, c1, c2))
      """
      Then query result ordered
      | c0 | c1 | c2 |
      | 1  | 2  | 3  |

    Scenario: json_tuple extracts boolean values
      When query
      """
      SELECT c0, c1 FROM (SELECT json_tuple('{"active":true,"verified":false}', 'active', 'verified') AS (c0, c1))
      """
      Then query result ordered
      | c0   | c1    |
      | true | false |

    Scenario: json_tuple with single key
      When query
      """
      SELECT c0 FROM (SELECT json_tuple('{"x":100}', 'x') AS (c0))
      """
      Then query result ordered
      | c0  |
      | 100 |

  Rule: Complex types extraction

    Scenario: json_tuple extracts nested objects as strings
      When query
      """
      SELECT c0, c1 FROM (SELECT json_tuple('{"user":{"name":"Bob"},"count":5}', 'user', 'count') AS (c0, c1))
      """
      Then query result ordered
      | c0               | c1 |
      | {"name":"Bob"}   | 5  |

    Scenario: json_tuple extracts array values as strings
      When query
      """
      SELECT c0, c1 FROM (SELECT json_tuple('{"items":[1,2,3],"total":6}', 'items', 'total') AS (c0, c1))
      """
      Then query result ordered
      | c0        | c1 |
      | [1,2,3]   | 6  |

  Rule: NULL and missing value handling

    Scenario: json_tuple returns null for missing keys
      When query
      """
      SELECT c0, c1, c2 FROM (SELECT json_tuple('{"a":1,"b":2}', 'a', 'missing', 'b') AS (c0, c1, c2))
      """
      Then query result ordered
      | c0 | c1   | c2 |
      | 1  | NULL | 2  |

    Scenario: json_tuple handles null value in JSON
      When query
      """
      SELECT c0, c1 FROM (SELECT json_tuple('{"a":null,"b":"value"}', 'a', 'b') AS (c0, c1))
      """
      Then query result ordered
      | c0   | c1    |
      | NULL | value |

  Rule: Edge cases and error handling

    Scenario: json_tuple handles invalid JSON
      When query
      """
      SELECT c0 FROM (SELECT json_tuple('not a json', 'a') AS (c0))
      """
      Then query result ordered
      | c0   |
      | NULL |

    Scenario: json_tuple handles empty JSON object
      When query
      """
      SELECT c0, c1 FROM (SELECT json_tuple('{}', 'a', 'b') AS (c0, c1))
      """
      Then query result ordered
      | c0   | c1   |
      | NULL | NULL |

    Scenario: json_tuple handles empty string values
      When query
      """
      SELECT c0, c1 FROM (SELECT json_tuple('{"a":"","b":"text"}', 'a', 'b') AS (c0, c1))
      """
      Then query result ordered
      | c0 | c1   |
      |    | text |

    Scenario: json_tuple handles escaped characters
      When query
      """
      SELECT c0 FROM (SELECT json_tuple('{"a":"hello\\"world"}', 'a') AS (c0))
      """
      Then query result ordered
      | c0           |
      | hello"world  |

  Rule: Direct usage without explicit alias

    Scenario: json_tuple direct select without subquery
      When query
      """
      SELECT json_tuple('{"a":1, "b":2}', 'a', 'b')
      """
      Then query result ordered
      | c0 | c1 |
      | 1  | 2  |

    Scenario: json_tuple direct select single key
      When query
      """
      SELECT c0 FROM (SELECT json_tuple('{"x":"hello"}', 'x') AS (c0))
      """
      Then query result ordered
      | c0    |
      | hello |

    Scenario: json_tuple direct select with missing key
      When query
      """
      SELECT json_tuple('{"a":1}', 'a', 'b', 'c')
      """
      Then query result ordered
      | c0 | c1   | c2   |
      | 1  | NULL | NULL |
