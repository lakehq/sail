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

//! Shared test utilities for Glue catalog table and view integration tests.

use arrow::datatypes::DataType;
use sail_catalog::provider::{
    CatalogProvider, CreateTableColumnOptions, CreateTableOptions, Namespace,
};
use sail_catalog_glue::GlueCatalogProvider;
use testcontainers::{ContainerAsync, GenericImage};

use crate::common::{setup_glue_catalog, simple_database_options};

/// Helper to create a column with default options (nullable=true, no comment).
pub fn col(name: &str, data_type: DataType) -> CreateTableColumnOptions {
    CreateTableColumnOptions {
        name: name.to_string(),
        data_type,
        nullable: true,
        comment: None,
        default: None,
        generated_always_as: None,
    }
}

/// Helper to create table options with sensible defaults (parquet format, no partitioning).
pub fn simple_table_options(columns: Vec<CreateTableColumnOptions>) -> CreateTableOptions {
    CreateTableOptions {
        columns,
        comment: None,
        constraints: vec![],
        location: Some("s3://bucket/default".to_string()),
        format: "parquet".to_string(),
        partition_by: vec![],
        sort_by: vec![],
        bucket_by: None,
        if_not_exists: false,
        replace: false,
        properties: vec![],
    }
}

/// Sets up a Glue catalog with a pre-created database for table tests.
/// Returns the provider, container, and namespace.
pub async fn setup_with_database(
    test_name: &str,
) -> (GlueCatalogProvider, ContainerAsync<GenericImage>, Namespace) {
    let (catalog, container) = setup_glue_catalog(test_name).await;
    let namespace = Namespace::try_from(vec![format!("{test_name}_db")]).unwrap();
    catalog
        .create_database(&namespace, simple_database_options())
        .await
        .unwrap();
    (catalog, container, namespace)
}
