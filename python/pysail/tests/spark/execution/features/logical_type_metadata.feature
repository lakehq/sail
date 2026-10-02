Feature: Spark logical types carried in field metadata survive a distributed plan

  Sail lowers VARIANT, GEOMETRY and GEOGRAPHY to a storage type plus field metadata, so a
  distributed plan only keeps the logical type if that metadata crosses the wire.

  Rule: A scalar function keeps the metadata of its result across stages

    # `datafusion-proto` rebuilds a scalar UDF's result as `Field::new(name, return_type, true)`
    # (`physical_plan/from_proto.rs:309-318`), so only the `DataType` travels. Sail asks the
    # function for its field again while decoding the plan, which restores the metadata; without
    # that the worker builds an array whose element field no longer matches the stage schema
    # serialized by the driver, and the query fails.
    @spark-4.2
    @sail-bug
    Scenario: the top-k form of max_by keeps a VARIANT value across stages
      When query
        """
        SELECT to_json(max_by(parse_json(CAST(i AS STRING)), i, 2)[0]) AS result
        FROM VALUES (1), (2) AS t(i)
        """
      Then query result
        | result |
        | 2      |

    @spark-4.2
    @sail-bug
    Scenario: the top-k form of min_by keeps a VARIANT value across stages
      When query
        """
        SELECT to_json(min_by(parse_json(CAST(i AS STRING)), i, 2)[0]) AS result
        FROM VALUES (1), (2) AS t(i)
        """
      Then query result
        | result |
        | 1      |

    # GEOMETRY is carried in field metadata alone, with plain BINARY as its type, so it depends on
    # the restored metadata even more than VARIANT does. `size` keeps the result collectable, and
    # it still makes the worker build the array whose element field has to match the stage schema.
    @spark-4.2
    @sail-bug
    Scenario: the top-k form of max_by keeps a GEOMETRY value across stages
      When query
        """
        SELECT size(max_by(st_geomfromwkb(w), i, 2)) AS result
        FROM VALUES (1, X'0101000000000000000000F03F0000000000000040'),
                    (2, X'010100000000000000000000400000000000000040') AS t(i, w)
        """
      Then query result
        | result |
        | 2      |

    @spark-4.2
    @sail-bug
    Scenario: the top-k form of min_by keeps a GEOMETRY value across stages
      When query
        """
        SELECT size(min_by(st_geomfromwkb(w), i, 2)) AS result
        FROM VALUES (1, X'0101000000000000000000F03F0000000000000040'),
                    (2, X'010100000000000000000000400000000000000040') AS t(i, w)
        """
      Then query result
        | result |
        | 2      |
