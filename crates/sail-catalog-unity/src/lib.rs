// SPDX-License-Identifier: Apache-2.0

mod config;
mod credential;
mod data_type;
mod legacy_commit;
mod provider;
mod token;

#[expect(unused_imports)]
#[expect(clippy::enum_variant_names)]
pub mod r#gen {
    include!(concat!(env!("OUT_DIR"), "/unity_catalog_gen.rs"));

    // The column type name is defined manually to allow for custom aliases.
    /// Name of type (INT, STRUCT, MAP, etc.).
    #[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
    pub enum ColumnTypeName {
        #[serde(rename = "BOOLEAN", alias = "boolean")]
        Boolean,
        #[serde(rename = "BYTE", alias = "byte")]
        Byte,
        #[serde(rename = "SHORT", alias = "short")]
        Short,
        #[serde(rename = "INT", alias = "int")]
        Int,
        #[serde(rename = "LONG", alias = "long")]
        Long,
        #[serde(rename = "FLOAT", alias = "float")]
        Float,
        #[serde(rename = "DOUBLE", alias = "double")]
        Double,
        #[serde(rename = "DATE", alias = "date")]
        Date,
        #[serde(rename = "TIMESTAMP", alias = "timestamp")]
        Timestamp,
        #[serde(rename = "TIMESTAMP_NTZ", alias = "timestamp_ntz")]
        TimestampNtz,
        #[serde(rename = "STRING", alias = "string")]
        String,
        #[serde(rename = "BINARY", alias = "binary")]
        Binary,
        #[serde(rename = "DECIMAL", alias = "decimal")]
        Decimal,
        #[serde(rename = "INTERVAL", alias = "interval")]
        Interval,
        #[serde(rename = "ARRAY", alias = "array")]
        Array,
        #[serde(rename = "STRUCT", alias = "struct")]
        Struct,
        #[serde(rename = "MAP", alias = "map")]
        Map,
        #[serde(rename = "VARIANT", alias = "variant")]
        Variant,
        #[serde(rename = "CHAR", alias = "char")]
        Char,
        #[serde(rename = "NULL", alias = "null")]
        Null,
        #[serde(rename = "USER_DEFINED_TYPE", alias = "user_defined_type")]
        UserDefinedType,
        #[serde(rename = "TABLE_TYPE", alias = "table_type")]
        TableType,
        /// A type name that is not known to the client (e.g. `GEOGRAPHY`), as it is on the wire.
        #[serde(untagged)]
        Unknown(String),
    }

    impl std::fmt::Display for ColumnTypeName {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let value = match self {
                Self::Boolean => "BOOLEAN",
                Self::Byte => "BYTE",
                Self::Short => "SHORT",
                Self::Int => "INT",
                Self::Long => "LONG",
                Self::Float => "FLOAT",
                Self::Double => "DOUBLE",
                Self::Date => "DATE",
                Self::Timestamp => "TIMESTAMP",
                Self::TimestampNtz => "TIMESTAMP_NTZ",
                Self::String => "STRING",
                Self::Binary => "BINARY",
                Self::Decimal => "DECIMAL",
                Self::Interval => "INTERVAL",
                Self::Array => "ARRAY",
                Self::Struct => "STRUCT",
                Self::Map => "MAP",
                Self::Variant => "VARIANT",
                Self::Char => "CHAR",
                Self::Null => "NULL",
                Self::UserDefinedType => "USER_DEFINED_TYPE",
                Self::TableType => "TABLE_TYPE",
                Self::Unknown(name) => name,
            };
            f.write_str(value)
        }
    }
}

/// The client for the Unity Catalog Delta REST API (`delta/v1`).
#[expect(unused_imports)]
#[expect(clippy::enum_variant_names)]
#[expect(clippy::vec_init_then_push)]
pub mod delta_gen {
    include!(concat!(env!("OUT_DIR"), "/unity_catalog_delta_gen.rs"));

    // The column data type is defined manually since primitive types are bare JSON strings
    // (e.g. `"long"`) on the wire while complex types are JSON objects.
    /// Column data type.
    pub type DeltaDataType = serde_json::Value;

    // The struct type is defined manually so that the field types and the field metadata
    // are sent as they are written in the Delta log.
    /// Struct type containing named fields.
    pub type DeltaStructType = serde_json::Value;

    // The response of creating a staging table is defined manually with only the fields
    // used for creating the table, so that the other fields are not validated.
    #[derive(Clone, Debug, serde::Deserialize)]
    pub struct DeltaStagingTableResponse {
        #[serde(rename = "table-id")]
        pub table_id: String,
        pub location: String,
        #[serde(rename = "required-protocol", default)]
        pub required_protocol: Option<DeltaProtocol>,
        #[serde(rename = "required-properties", default)]
        pub required_properties: Option<std::collections::HashMap<String, Option<String>>>,
    }

    // The response of loading a table is defined manually with only the fields used for
    // commit discovery, so that the other fields are not validated.
    #[derive(Clone, Debug, serde::Deserialize)]
    pub struct DeltaLoadTableResponse {
        pub metadata: DeltaLoadTableMetadata,
        #[serde(default)]
        pub commits: Option<Vec<DeltaLoadTableCommit>>,
        #[serde(rename = "latest-table-version", default)]
        pub latest_table_version: Option<i64>,
    }

    // The storage credentials are defined manually so that servers that issue no
    // credentials for a location are accepted, and so that the secret values are not
    // shown when the response is formatted.
    #[derive(Clone, Debug, serde::Deserialize)]
    pub struct DeltaCredentialsResponse {
        #[serde(rename = "storage-credentials", default)]
        pub storage_credentials: Option<Vec<DeltaStorageCredential>>,
    }

    #[derive(Clone, serde::Deserialize)]
    pub struct DeltaStorageCredential {
        #[serde(default)]
        pub prefix: Option<String>,
        /// The operation that the credential is issued for, such as `READ` or `READ_WRITE`.
        #[serde(default)]
        pub operation: Option<String>,
        #[serde(default)]
        pub config: Option<std::collections::HashMap<String, Option<String>>>,
        #[serde(rename = "expiration-time-ms", default)]
        pub expiration_time_ms: Option<i64>,
    }

    impl std::fmt::Debug for DeltaStorageCredential {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("DeltaStorageCredential")
                .field("prefix", &self.prefix)
                .field("operation", &self.operation)
                .field("expiration_time_ms", &self.expiration_time_ms)
                .finish_non_exhaustive()
        }
    }

    #[derive(Clone, Debug, serde::Deserialize)]
    pub struct DeltaLoadTableMetadata {
        #[serde(rename = "table-uuid")]
        pub table_uuid: String,
        /// The property values are not validated.
        #[serde(default)]
        pub properties: Option<std::collections::HashMap<String, serde_json::Value>>,
    }

    #[derive(Clone, Debug, serde::Deserialize)]
    pub struct DeltaLoadTableCommit {
        pub version: i64,
        #[serde(rename = "file-name")]
        pub file_name: String,
        #[serde(default)]
        pub timestamp: i64,
        #[serde(rename = "file-size", default)]
        pub file_size: i64,
        #[serde(rename = "file-modification-timestamp", default)]
        pub file_modification_timestamp: i64,
    }
}

pub use config::UnityCatalogConfig;
pub use provider::{UnityCatalogOptions, UnityCatalogProvider};
