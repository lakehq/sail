# SPDX-License-Identifier: Apache-2.0

Feature: rlike

  Rule: A literal backslash in a regular expression matches a backslash

    Scenario Outline: <function> keeps a literal backslash when matching a column
      When query
        """
        SELECT id, <function>(x, r'a\\b') AS result
        FROM VALUES (1, r'a\b'), (2, 'ab'), (3, CAST(NULL AS STRING)) AS t(id, x)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | true   |
        | 2  | false  |
        | 3  | NULL   |

      Examples:
        | function    |
        | rlike       |
        | regexp      |
        | regexp_like |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to rlike yields the schema Spark declares
      When query
        """
        SELECT rlike('%SystemDrive%\Users\John', '%SystemDrive%\\Users.*') AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a non-null column input to rlike yields the schema Spark declares
      When query
        """
        SELECT rlike(CAST(id AS STRING), '%SystemDrive%\\Users.*') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a nullable column input to rlike stays nullable
      When query
        """
        SELECT rlike(c, '%SystemDrive%\\Users.*') AS result FROM VALUES ('%SystemDrive%\Users\John'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """
