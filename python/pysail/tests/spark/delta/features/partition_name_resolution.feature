Feature: Delta Lake partition column name resolution

  Rule: Partition columns use Spark-compatible case-insensitive resolution

    Scenario: Greek final sigma resolves to the canonical schema column
      Given variable location for temporary directory delta_partition_greek_sigma
      Given final statement
        """
        DROP TABLE IF EXISTS delta_partition_greek_sigma_test
        """
      Given statement template
        """
        CREATE TABLE delta_partition_greek_sigma_test (
          id INT,
          `ΣDate` STRING
        )
        USING DELTA
        PARTITIONED BY (`ςdate`)
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_partition_greek_sigma_test VALUES (1, '2024-01-01')
        """
      When query
        """
        SELECT id, `ΣDate` FROM delta_partition_greek_sigma_test
        """
      Then query result ordered
        | id | ΣDate      |
        | 1  | 2024-01-01 |
      Then delta log latest effective protocol and metadata contains
        | path                      | value     |
        | metaData.partitionColumns | ["ΣDate"] |

    Scenario: JDK 17 keeps newer Vithkuqi case pairs distinct
      Given variable location for temporary directory delta_partition_vithkuqi
      Given final statement
        """
        DROP TABLE IF EXISTS delta_partition_vithkuqi_test
        """
      Given statement template
        """
        CREATE TABLE delta_partition_vithkuqi_test (
          id INT,
          `𐕰Date` STRING,
          `𐖗Date` STRING
        )
        USING DELTA
        PARTITIONED BY (`𐖗Date`)
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_partition_vithkuqi_test VALUES (1, 'upper', 'partition')
        """
      When query
        """
        SELECT id, `𐕰Date`, `𐖗Date` FROM delta_partition_vithkuqi_test
        """
      Then query result ordered
        | id | 𐕰Date | 𐖗Date     |
        | 1  | upper | partition |
      Then delta log latest effective protocol and metadata contains
        | path                      | value     |
        | metaData.partitionColumns | ["𐖗Date"] |
