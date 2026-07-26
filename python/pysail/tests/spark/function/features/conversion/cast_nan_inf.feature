@cast_nan_inf
Feature: CAST and type constructors with NaN and Infinity (issue #630)

  Rule: FLOAT type constructor

    Scenario Outline: FLOAT constructor: <case>
      When query
        """
        SELECT FLOAT(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                     | arg         | result    |
        | FLOAT NaN                | 'NAN'       | NaN       |
        | FLOAT NaN lowercase      | 'nan'       | NaN       |
        | FLOAT NaN mixed case     | 'Nan'       | NaN       |
        | FLOAT negative NaN       | '-NaN'      | NaN       |
        | FLOAT Infinity           | 'Infinity'  | Infinity  |
        | FLOAT negative Infinity  | '-Infinity' | -Infinity |
        | FLOAT Infinity lowercase | 'infinity'  | Infinity  |
        | FLOAT normal value       | '42'        | 42.0      |

    @sail-bug
    # Sail does not trim spaces before parsing NaN
    Scenario: FLOAT NaN with spaces
      When query
        """
        SELECT FLOAT(' NaN ') AS result
        """
      Then query result
        | result |
        | NaN    |

  Rule: DOUBLE type constructor

    Scenario Outline: DOUBLE constructor: <case>
      When query
        """
        SELECT DOUBLE(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                               | arg         | result    |
        | DOUBLE NaN                         | 'NAN'       | NaN       |
        | DOUBLE Infinity uppercase          | 'INFINITY'  | Infinity  |
        | DOUBLE negative Infinity uppercase | '-INFINITY' | -Infinity |
        | DOUBLE normal value                | '3.14'      | 3.14      |

    @sail-bug
    # Sail does not trim spaces before parsing Infinity
    Scenario: DOUBLE Infinity with spaces
      When query
        """
        SELECT DOUBLE(' Infinity ') AS result
        """
      Then query result
        | result   |
        | Infinity |

  Rule: CAST to FLOAT/DOUBLE

    Scenario Outline: CAST: <case>
      When query
        """
        SELECT CAST('NaN' AS <type>) AS result
        """
      Then query result
        | result |
        | NaN    |

      Examples:
        | case               | type   |
        | CAST NaN to FLOAT  | FLOAT  |
        | CAST NaN to DOUBLE | DOUBLE |

  Rule: Integer types reject NaN and Infinity

    Scenario Outline: Integer rejects: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query error .*

      Examples:
        | case                      | expr                         |
        | INT NaN errors            | INT('NAN')                   |
        | CAST NaN to INT errors    | CAST('NaN' AS INT)           |
        | INT Infinity errors       | INT('Infinity')              |
        | BIGINT NaN errors         | BIGINT('NaN')                |
        | SMALLINT NaN errors       | SMALLINT('NaN')              |
        | TINYINT NaN errors        | TINYINT('NaN')               |
        | DECIMAL NaN errors        | CAST('NaN' AS DECIMAL(10,2)) |
        | INT invalid string errors | INT('hello')                 |

  Rule: TRY_CAST with NaN

    Scenario Outline: TRY_CAST: <case>
      When query
        """
        SELECT TRY_CAST('NaN' AS <type>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                              | type  | result |
        | TRY_CAST NaN to INT returns NULL  | INT   | NULL   |
        | TRY_CAST NaN to FLOAT returns NaN | FLOAT | NaN    |

  Rule: NaN arithmetic

    Scenario Outline: NaN arithmetic: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                   | expr                        | result |
        | NaN plus number is NaN                 | FLOAT('NaN') + 1            | NaN    |
        | NaN equals NaN is true in Spark        | FLOAT('NaN') = FLOAT('NaN') | true   |
        | NaN greater than zero is true in Spark | FLOAT('NaN') > 0            | true   |

  Rule: Multi-row with NaN and Infinity

    Scenario: VALUES with FLOAT NaN Infinity and NULL
      When query
        """
        SELECT * FROM VALUES (FLOAT('NaN')), (FLOAT('Infinity')), (FLOAT('-Infinity')), (NULL), (0.0), (1.5) AS t(v)
        """
      Then query result
        | v         |
        | NaN       |
        | Infinity  |
        | -Infinity |
        | NULL      |
        | 0.0       |
        | 1.5       |

    Scenario: VALUES with DOUBLE NaN Infinity and NULL
      When query
        """
        SELECT * FROM VALUES (DOUBLE('NaN')), (DOUBLE('Infinity')), (DOUBLE('-Infinity')), (NULL), (0.0) AS t(v)
        """
      Then query result
        | v         |
        | NaN       |
        | Infinity  |
        | -Infinity |
        | NULL      |
        | 0.0       |
