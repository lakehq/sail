Feature: typeof output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to typeof yields the schema Spark declares
      When query
        """
        SELECT typeof(1) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a non-null column input to typeof yields the schema Spark declares
      When query
        """
        SELECT typeof(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to typeof stays nullable
      When query
        """
        SELECT typeof(c) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

  Rule: Type names

    # `typeof` renders `DataType.catalogString`, which for most types is `simpleString`
    # (`sql/api/.../types/DataType.scala:76-79`). This is the SQL-visible half of Sail's type
    # naming; `StructType.toDDL` renders `.sql` through a SEPARATE path in Sail
    # (`sail-spark-connect/src/schema.rs`), and the two do not agree — see
    # `python/pysail/tests/spark/dataframe/test_toddl.py`.

    Scenario Outline: typeof names the type the way Spark's catalogString does
      When query
        """
        SELECT typeof(CAST(NULL AS <sql_type>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case          | sql_type      | result        |
        | void          | VOID          | void          |
        | boolean       | BOOLEAN       | boolean       |
        | tinyint       | TINYINT       | tinyint       |
        | smallint      | SMALLINT      | smallint      |
        | int           | INT           | int           |
        | bigint        | BIGINT        | bigint        |
        | float         | FLOAT         | float         |
        | double        | DOUBLE        | double        |
        | decimal       | DECIMAL(10,3) | decimal(10,3) |
        | string        | STRING        | string        |
        | binary        | BINARY        | binary        |
        | date          | DATE          | date          |
        | timestamp     | TIMESTAMP     | timestamp     |
        | timestamp_ntz | TIMESTAMP_NTZ | timestamp_ntz |

    Scenario Outline: typeof names a container by its element types
      When query
        """
        SELECT typeof(CAST(NULL AS <sql_type>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case         | sql_type              | result                |
        | array        | ARRAY<INT>            | array<int>            |
        | nested array | ARRAY<ARRAY<STRING>>  | array<array<string>>  |
        | map          | MAP<STRING,INT>       | map<string,int>       |
        | struct       | STRUCT<a:INT>         | struct<a:int>         |
        | nested       | STRUCT<s:STRUCT<a:INT>> | struct<s:struct<a:int>> |

    # A char/varchar column is erased to `string` before `typeof` sees it.
    Scenario Outline: typeof erases the length-bounded string types
      When query
        """
        SELECT typeof(CAST(NULL AS <sql_type>)) AS result
        """
      Then query result
        | result |
        | string |

      Examples:
        | case    | sql_type   |
        | char    | CHAR(5)    |
        | varchar | VARCHAR(5) |

    @sail-bug
    Scenario Outline: typeof keeps the interval fields the type was declared with
      # Sail widens every year-month interval to `interval year to month` and every day-time one
      # to `interval day to second`, losing the start/end fields. `toDDL` keeps them, so the loss
      # is on the Arrow side of the type, not in the Connect protocol.
      When query
        """
        SELECT typeof(CAST(NULL AS <sql_type>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case             | sql_type                 | result                   |
        | year             | INTERVAL YEAR            | interval year            |
        | month            | INTERVAL MONTH           | interval month           |
        | day              | INTERVAL DAY             | interval day             |
        | hour to minute   | INTERVAL HOUR TO MINUTE  | interval hour to minute  |

    @sail-bug
    Scenario: typeof names a variant by its type, not by how it is stored
      # Sail leaks the storage representation `struct<value:binary,metadata:binary>`.
      When query
        """
        SELECT typeof(CAST(NULL AS VARIANT)) AS result
        """
      Then query result
        | result  |
        | variant |
