Feature: rint

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to rint yields the schema Spark declares
      When query
        """
        SELECT rint(12.3456) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to rint stays nullable
      When query
        """
        SELECT rint(c) AS result FROM VALUES (12.3456), (CAST(NULL AS DECIMAL(6,4))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario Outline: rint output schema is a nullable double (<form>)
      When query
        """
        <query>
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

      Examples:
        | form            | query                                                |
        | literal         | SELECT rint(1) AS result                             |
        | non-null column | SELECT rint(CAST(id AS INT)) AS result FROM range(2) |

  Rule: rint rounds half to even

    Scenario Outline: rint(<input>) = <result> (<what>)
      When query
        """
        SELECT rint(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | what                   | input                              | result    |
        | tie down               | 2.5D                               | 2.0       |
        | tie up                 | 3.5D                               | 4.0       |
        | tie up (odd)           | 1.5D                               | 2.0       |
        | tie negative down      | -2.5D                              | -2.0      |
        | tie negative up        | -3.5D                              | -4.0      |
        | tie at zero            | 0.5D                               | 0.0       |
        | tie at negative zero   | -0.5D                              | -0.0      |
        | below a tie            | 2.4D                               | 2.0       |
        | above a tie            | 2.6D                               | 3.0       |
        | above a tie, negative  | -2.6D                              | -3.0      |
        | just below 0.5         | CAST(0.49999999999999994 AS DOUBLE) | 0.0       |
        | negative below 0.5     | CAST(-0.4 AS DOUBLE)               | -0.0      |
        | tie just below 2^52    | 4503599627370495.5D                | 4.503599627370496E15 |
        | tie below 2^51, even   | 2251799813685248.5D                | 2.251799813685248E15 |
        | tie below 2^51, odd    | 2251799813685249.5D                | 2.25179981368525E15  |
        | float tie               | CAST(2.5 AS FLOAT)                 | 2.0       |
        | float negative tie      | CAST(-0.5 AS FLOAT)                | -0.0      |
        | bigint                 | CAST(3 AS BIGINT)                  | 3.0       |
        | negative zero          | -CAST(0.0 AS DOUBLE)               | -0.0      |
        | NaN                    | CAST('NaN' AS DOUBLE)              | NaN       |
        | infinity                | CAST('Infinity' AS DOUBLE)          | Infinity   |
        | negative infinity       | CAST('-Infinity' AS DOUBLE)         | -Infinity  |
        | decimal tie            | CAST(2.5 AS DECIMAL(10,2))         | 2.0       |
        | string tie             | '2.5'                              | 2.0       |

  Rule: rint evaluates its argument once

    # Spark rounds each drawn value once, and a tie `k + 0.5` can only round to an even number.
    # If the argument were evaluated several times, the draw that is tested and the draw that is
    # rounded would differ and odd results would appear.
    Scenario: rint of a volatile argument
      When query
        """
        SELECT count(*) AS odd
        FROM (SELECT rint(floor(rand() * 10) + 0.5) AS r FROM range(10000))
        WHERE r % 2 <> 0
        """
      Then query result
        | odd |
        | 0   |

    Scenario: rint of a seeded volatile argument
      When query
        """
        SELECT count(*) AS odd
        FROM (SELECT rint(floor(rand(42) * 10) + 0.5) AS r FROM range(10000))
        WHERE r % 2 <> 0
        """
      Then query result
        | odd |
        | 0   |
