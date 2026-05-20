Feature: to_csv converts a struct value to a CSV string

  Rule: Basic serialization

    Scenario: Convert a struct with two integer fields
      When query
        """
        SELECT to_csv(named_struct('a', 1, 'b', 2))
        """
      Then query result
        | to_csv(named_struct(a, 1, b, 2)) |
        | 1,2                               |

    Scenario: Convert a struct with mixed types
      When query
        """
        SELECT to_csv(named_struct('a', 1, 'b', 2.5, 'c', true))
        """
      Then query result
        | to_csv(named_struct(a, 1, b, 2.5, c, true)) |
        | 1,2.5,true                                    |

    Scenario: Convert a struct from a table column
      When query
        """
        SELECT to_csv(named_struct('a', value, 'b', value))
        FROM VALUES (1), (2), (3) AS t(value)
        ORDER BY value
        """
      Then query result
        | to_csv(named_struct(a, value, b, value)) |
        | 1,1                                       |
        | 2,2                                       |
        | 3,3                                       |

  Rule: NULL handling

    Scenario: NULL struct returns NULL
      When query
        """
        SELECT to_csv(CAST(NULL AS STRUCT<a:INT, b:INT>))
        """
      Then query result
        | to_csv(CAST(NULL AS STRUCT<A:INT,B:INT>)) |
        | NULL         |

    Scenario: Struct with NULL field serializes null field as empty string
      When query
        """
        SELECT to_csv(named_struct('a', 1, 'b', CAST(NULL AS INT)))
        """
      Then query result
        | to_csv(named_struct(a, 1, b, CAST(NULL AS INT))) |
        | 1,                                   |

  Rule: Separator options

    Scenario: Custom separator via sep option
      When query
        """
        SELECT to_csv(named_struct('a', 1, 'b', 2), map('sep', '|'))
        """
      Then query result
        | to_csv(named_struct(a, 1, b, 2), map(sep, \|)) |
        | 1\|2                               |

    Scenario: Custom separator via delimiter option
      When query
        """
        SELECT to_csv(named_struct('a', 1, 'b', 2), map('delimiter', '|'))
        """
      Then query result
        | to_csv(named_struct(a, 1, b, 2), map(delimiter, \|)) |
        | 1\|2                               |

  Rule: Timestamp formatting

    Scenario: Timestamp field uses default ISO 8601 UTC format
      When query
        """
        SELECT to_csv(named_struct('ts', to_timestamp('2015-08-26', 'yyyy-MM-dd')))
        """
      Then query result
        | to_csv(named_struct(ts, to_timestamp(2015-08-26, yyyy-MM-dd))) |
        | 2015-08-26T00:00:00.000Z                                        |

    Scenario: Pre-epoch timestamp is formatted correctly
      When query
        """
        SELECT to_csv(named_struct('ts', to_timestamp('1969-12-31', 'yyyy-MM-dd')))
        """
      Then query result
        | to_csv(named_struct(ts, to_timestamp(1969-12-31, yyyy-MM-dd))) |
        | 1969-12-31T00:00:00.000Z                                        |

    Scenario: Custom timestampFormat option changes the output format
      When query
        """
        SELECT to_csv(named_struct('time', to_timestamp('2015-08-26', 'yyyy-MM-dd')), map('timestampFormat', 'dd/MM/yyyy'))
        """
      Then query result
        | to_csv(named_struct(time, to_timestamp(2015-08-26, yyyy-MM-dd)), map(timestampFormat, dd/MM/yyyy)) |
        | 26/08/2015                                                         |

  Rule: Date formatting

    Scenario: Date field uses default yyyy-MM-dd format
      When query
        """
        SELECT to_csv(named_struct('d', DATE '2015-08-26'))
        """
      Then query result
        | to_csv(named_struct(d, DATE '2015-08-26')) |
        | 2015-08-26                                  |

    Scenario: Pre-epoch date is formatted correctly
      When query
        """
        SELECT to_csv(named_struct('d', DATE '1969-12-31'))
        """
      Then query result
        | to_csv(named_struct(d, DATE '1969-12-31')) |
        | 1969-12-31                                  |

    Scenario: Custom dateFormat option changes the output format
      When query
        """
        SELECT to_csv(named_struct('d', DATE '2015-08-26'), map('dateFormat', 'dd/MM/yyyy'))
        """
      Then query result
        | to_csv(named_struct(d, DATE '2015-08-26'), map(dateFormat, dd/MM/yyyy)) |
        | 26/08/2015                                  |

  Rule: Decimal formatting

    Scenario: Decimal field is formatted as fixed-point string
      When query
        """
        SELECT to_csv(named_struct('price', CAST(9.99 AS DECIMAL(5,2))))
        """
      Then query result
        | to_csv(named_struct(price, CAST(9.99 AS DECIMAL(5,2)))) |
        | 9.99                                                      |

    Scenario: Negative decimal preserves sign including fractional-only values
      When query
        """
        SELECT to_csv(named_struct('price', CAST(-0.99 AS DECIMAL(5,2))))
        """
      Then query result
        | to_csv(named_struct(price, CAST((- 0.99) AS DECIMAL(5,2)))) |
        | -0.99                                                      |
