// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
mod config;
mod credential;
mod data_type;
mod provider;
mod token;

pub mod unity {
    #![expect(clippy::allow_attributes)]
    include!(concat!(env!("OUT_DIR"), "/unity_catalog.rs"));
}

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
        #[serde(rename = "CHAR", alias = "char")]
        Char,
        #[serde(rename = "NULL", alias = "null")]
        Null,
        #[serde(rename = "USER_DEFINED_TYPE", alias = "user_defined_type")]
        UserDefinedType,
        #[serde(rename = "TABLE_TYPE", alias = "table_type")]
        TableType,
    }
}

pub use config::UnityCatalogConfig;
pub use provider::{UnityCatalogOptions, UnityCatalogProvider};
