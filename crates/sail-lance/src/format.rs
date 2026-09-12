// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! The Lance implementation of Sail's [`TableFormat`].
//!
//! This is the type Sail's table format registry is given; everything Sail
//! needs from Lance goes through it.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::provider_as_source;
use datafusion_common::{Result, not_impl_err, plan_err};
use datafusion_expr::{Extension, LogicalPlan, TableSource};
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat, TableFormatRegistry};

use crate::options::{LanceReadOptions, LanceWriteMode, LanceWriteOptions};
use crate::provider::LanceTableProvider;
use crate::uri;
use crate::write::LanceWriteNode;

/// The `lance` table format.
///
/// Register it with Sail's table format registry to make
/// `spark.read.format("lance")`, `df.write.format("lance")` and
/// `CREATE TABLE ... USING lance` work:
///
/// ```
/// # use sail_common_datafusion::datasource::TableFormatRegistry;
/// # use sail_lance::LanceTableFormat;
/// # fn register(registry: &TableFormatRegistry) -> datafusion_common::Result<()> {
/// LanceTableFormat::register(registry)?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Default, Clone, Copy)]
pub struct LanceTableFormat;

impl LanceTableFormat {
    /// Adds this format to a Sail table format registry.
    pub fn register(registry: &TableFormatRegistry) -> Result<()> {
        registry.register(Arc::new(Self))
    }
}

#[async_trait]
impl TableFormat for LanceTableFormat {
    fn name(&self) -> &str {
        "lance"
    }

    async fn create_source(
        &self,
        _ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        let SourceInfo {
            paths,
            lakehouse_table: _,
            schema,
            constraints: _,
            partition_by,
            bucket_by,
            sort_order: _,
            options,
            read_case_sensitive: _,
        } = info;
        if !partition_by.is_empty() {
            return not_impl_err!(
                "partition columns for the Lance table format; \
                 a Lance dataset is not partitioned by directory"
            );
        }
        if bucket_by.is_some() {
            return not_impl_err!("bucketing for the Lance table format");
        }
        let uri = uri::resolve(&paths, &options)?;
        let read_options = LanceReadOptions::resolve(&options)?;
        let provider = LanceTableProvider::try_open(&uri, read_options).await?;
        if let Some(schema) = schema.filter(|schema| !schema.fields().is_empty()) {
            check_user_schema(&schema.into(), &provider.schema(), &uri)?;
        }
        Ok(provider_as_source(Arc::new(provider)))
    }

    async fn create_writer(&self, _ctx: &dyn Session, info: SinkInfo) -> Result<LogicalPlan> {
        let SinkInfo {
            input,
            mode,
            partition_by,
            bucket_by,
            sort_order,
            options,
            lakehouse_table: _,
        } = info;
        if !partition_by.is_empty() {
            return not_impl_err!("partition columns for the Lance table format");
        }
        if bucket_by.is_some() {
            return not_impl_err!("bucketing for the Lance table format");
        }
        if !sort_order.is_empty() {
            return not_impl_err!("write sort order for the Lance table format");
        }
        let uri = uri::resolve(&[], &options)?;
        let write_mode = LanceWriteMode::resolve(&mode)?;
        let write_options = LanceWriteOptions::resolve(&options)?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(LanceWriteNode::new(
                Arc::new(input),
                uri,
                write_mode,
                write_options,
            )),
        }))
    }
}

/// Rejects a user supplied schema that does not describe the dataset.
///
/// A Lance dataset carries its own schema, so the schema a user passes can only
/// confirm it.
fn check_user_schema(user: &SchemaRef, dataset: &SchemaRef, uri: &str) -> Result<()> {
    let mismatched =
        user.fields()
            .iter()
            .find(|field| match dataset.field_with_name(field.name()) {
                Ok(actual) => actual.data_type() != field.data_type(),
                Err(_) => true,
            });
    match mismatched {
        None => Ok(()),
        Some(field) => plan_err!(
            "the schema given for the Lance dataset at {uri} does not match the dataset: \
             field '{}' of type {} is not in the dataset schema [{}]",
            field.name(),
            field.data_type(),
            dataset
                .fields()
                .iter()
                .map(|field| format!("{}: {}", field.name(), field.data_type()))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}
