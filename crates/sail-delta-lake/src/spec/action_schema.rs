use crate::spec::schema::{ArrayType, DataType, MapType, StructField, StructType};

fn string_map_type(value_contains_null: bool) -> DataType {
    MapType::new(DataType::STRING, DataType::STRING, value_contains_null).into()
}

fn string_list_type() -> DataType {
    ArrayType::new(DataType::STRING, true).into()
}

fn deletion_vector_data_type() -> DataType {
    deletion_vector_struct_type().into()
}

fn metadata_format_data_type() -> DataType {
    StructType::new_unchecked([
        StructField::not_null("provider", DataType::STRING),
        StructField::not_null("options", string_map_type(true)),
    ])
    .into()
}

pub fn deletion_vector_struct_type() -> StructType {
    StructType::new_unchecked([
        StructField::not_null("storageType", DataType::STRING),
        StructField::not_null("pathOrInlineDv", DataType::STRING),
        StructField::nullable("offset", DataType::INTEGER),
        StructField::not_null("sizeInBytes", DataType::INTEGER),
        StructField::not_null("cardinality", DataType::LONG),
    ])
}

pub fn add_struct_type() -> StructType {
    StructType::new_unchecked([
        StructField::not_null("path", DataType::STRING),
        StructField::not_null("partitionValues", string_map_type(true)),
        StructField::not_null("size", DataType::LONG),
        StructField::not_null("modificationTime", DataType::LONG),
        StructField::not_null("dataChange", DataType::BOOLEAN),
        StructField::nullable("stats", DataType::STRING),
        StructField::nullable("tags", string_map_type(true)),
        StructField::nullable("deletionVector", deletion_vector_data_type()),
        StructField::nullable("baseRowId", DataType::LONG),
        StructField::nullable("defaultRowCommitVersion", DataType::LONG),
        StructField::nullable("clusteringProvider", DataType::STRING),
    ])
}

pub fn remove_struct_type() -> StructType {
    StructType::new_unchecked([
        StructField::not_null("path", DataType::STRING),
        StructField::not_null("dataChange", DataType::BOOLEAN),
        StructField::nullable("deletionTimestamp", DataType::LONG),
        StructField::nullable("extendedFileMetadata", DataType::BOOLEAN),
        StructField::nullable("partitionValues", string_map_type(true)),
        StructField::nullable("size", DataType::LONG),
        StructField::nullable("stats", DataType::STRING),
        StructField::nullable("tags", string_map_type(true)),
        StructField::nullable("deletionVector", deletion_vector_data_type()),
        StructField::nullable("baseRowId", DataType::LONG),
        StructField::nullable("defaultRowCommitVersion", DataType::LONG),
    ])
}

pub fn protocol_struct_type() -> StructType {
    StructType::new_unchecked([
        StructField::not_null("minReaderVersion", DataType::INTEGER),
        StructField::not_null("minWriterVersion", DataType::INTEGER),
        StructField::nullable("readerFeatures", string_list_type()),
        StructField::nullable("writerFeatures", string_list_type()),
    ])
}

pub fn domain_metadata_struct_type() -> StructType {
    StructType::new_unchecked([
        StructField::not_null("domain", DataType::STRING),
        StructField::not_null("configuration", DataType::STRING),
        StructField::not_null("removed", DataType::BOOLEAN),
    ])
}

pub fn checkpoint_metadata_struct_type() -> StructType {
    StructType::new_unchecked([
        StructField::not_null("version", DataType::LONG),
        StructField::nullable("tags", string_map_type(true)),
    ])
}

pub fn metadata_struct_type() -> StructType {
    StructType::new_unchecked([
        StructField::not_null("id", DataType::STRING),
        StructField::nullable("name", DataType::STRING),
        StructField::nullable("description", DataType::STRING),
        StructField::not_null("format", metadata_format_data_type()),
        StructField::not_null("schemaString", DataType::STRING),
        StructField::not_null("partitionColumns", string_list_type()),
        StructField::nullable("createdTime", DataType::LONG),
        StructField::not_null("configuration", string_map_type(true)),
    ])
}

pub fn transaction_struct_type() -> StructType {
    StructType::new_unchecked([
        StructField::not_null("appId", DataType::STRING),
        StructField::not_null("version", DataType::LONG),
        StructField::nullable("lastUpdated", DataType::LONG),
    ])
}

pub fn sidecar_struct_type() -> StructType {
    StructType::new_unchecked([
        StructField::not_null("path", DataType::STRING),
        StructField::not_null("sizeInBytes", DataType::LONG),
        StructField::not_null("modificationTime", DataType::LONG),
        StructField::nullable("tags", string_map_type(true)),
    ])
}

#[cfg(test)]
mod tests {
    use super::{
        add_struct_type, checkpoint_metadata_struct_type, metadata_struct_type, remove_struct_type,
        sidecar_struct_type,
    };

    #[test]
    fn add_schema_keeps_extended_fields() {
        let add = add_struct_type();
        assert!(add.field("stats").is_some());
        assert!(add.field("tags").is_some());
        assert!(add.field("deletionVector").is_some());
        assert!(add.field("clusteringProvider").is_some());
    }

    #[test]
    fn remove_schema_keeps_optional_partition_values() {
        let remove = remove_struct_type();
        #[expect(clippy::expect_used)]
        let partition_values = remove
            .field("partitionValues")
            .expect("remove schema should contain partitionValues");
        assert!(partition_values.is_nullable());
    }

    #[test]
    fn metadata_schema_keeps_configuration_field() {
        let metadata = metadata_struct_type();
        assert!(metadata.field("configuration").is_some());
        assert!(metadata.field("schemaString").is_some());
    }

    #[test]
    fn sidecar_schema_keeps_required_protocol_fields() {
        let sidecar = sidecar_struct_type();
        assert!(sidecar.field("path").is_some());
        assert!(sidecar.field("sizeInBytes").is_some());
        assert!(sidecar.field("modificationTime").is_some());
        assert!(sidecar.field("tags").is_some());
    }

    #[test]
    fn checkpoint_metadata_schema_matches_protocol_fields() {
        let checkpoint_metadata = checkpoint_metadata_struct_type();
        assert!(checkpoint_metadata.field("version").is_some());
        assert!(checkpoint_metadata.field("tags").is_some());
    }
}
