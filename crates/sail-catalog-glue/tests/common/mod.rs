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

//! Shared test utilities for Glue catalog integration tests.

use std::time::Duration;

use arrow::datatypes::DataType;
use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseOptions, CreateTableColumnOptions, CreateTableOptions, Namespace,
};
use sail_catalog_glue::{GlueCatalogConfig, GlueCatalogProvider};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

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

/// Helper to create database options with sensible defaults.
pub fn simple_database_options() -> CreateDatabaseOptions {
    CreateDatabaseOptions {
        if_not_exists: false,
        comment: None,
        location: None,
        properties: vec![],
    }
}

/// Helper to create table options with sensible defaults (parquet format, no partitioning).
pub fn simple_table_options(columns: Vec<CreateTableColumnOptions>) -> CreateTableOptions {
    CreateTableOptions {
        columns,
        comment: None,
        constraints: vec![],
        location: None,
        format: "parquet".to_string(),
        partition_by: vec![],
        sort_by: vec![],
        bucket_by: None,
        if_not_exists: false,
        replace: false,
        options: vec![],
        properties: vec![],
    }
}

/// Sets up a Glue catalog provider backed by a Moto container for testing.
pub async fn setup_glue_catalog(
    test_name: &str,
) -> (GlueCatalogProvider, ContainerAsync<GenericImage>) {
    let moto = GenericImage::new("motoserver/moto", "latest")
        .with_wait_for(WaitFor::message_on_stderr("Running on"))
        .with_exposed_port(ContainerPort::Tcp(5000))
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .expect("Failed to start Moto");

    let host = moto.get_host().await.expect("get host");
    let port = moto.get_host_port_ipv4(5000).await.expect("get port");
    let endpoint = format!("http://{host}:{port}");

    let config = GlueCatalogConfig {
        region: Some("us-east-1".to_string()),
        endpoint_url: Some(endpoint),
        access_key_id: Some("testing".to_string()),
        secret_access_key: Some("testing".to_string()),
    };

    let provider = GlueCatalogProvider::new(test_name.to_string(), config);

    (provider, moto)
}

/// Sets up a Glue catalog with a pre-created database for table tests.
pub async fn setup_with_database(
    test_name: &str,
) -> (
    GlueCatalogProvider,
    ContainerAsync<GenericImage>,
    Namespace,
) {
    let (catalog, container) = setup_glue_catalog(test_name).await;
    let namespace = Namespace::try_from(vec![format!("{test_name}_db")]).unwrap();
    catalog
        .create_database(&namespace, simple_database_options())
        .await
        .unwrap();
    (catalog, container, namespace)
}
