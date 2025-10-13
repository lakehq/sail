use std::collections::HashSet;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::stats::Statistics;
use datafusion::datasource::object_store::ObjectStoreUrl;
use deltalake::errors::{DeltaResult, DeltaTableError};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::kernel::snapshot::LogDataHandler;
use crate::table::DeltaTableState;
/// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/mod.rs>
pub(crate) const PATH_COLUMN: &str = "__delta_rs_path";

pub mod actions;
pub mod error;
pub mod expressions;
pub mod provider;
pub mod pruning;
pub mod scan;
pub mod schema;
pub(crate) mod schema_rewriter;
pub mod type_converter;

// Re-exports
pub use actions::{
    adds_to_remove_actions, get_path_column, join_batches_with_add_actions,
    partitioned_file_from_action, to_correct_scalar_value,
};
pub use error::{datafusion_to_delta_error, delta_to_datafusion_error};
pub use expressions::{
    collect_physical_columns, get_pushdown_filters, parse_predicate_expression, simplify_expr,
    DeltaContextProvider, PredicateProperties,
};
pub use provider::DeltaTableProvider;
pub use pruning::{prune_files, PruningResult};
pub use scan::build_file_scan_config;
pub use schema::{arrow_schema_from_struct_type, df_logical_schema, DataFusionMixins};

pub(crate) fn create_object_store_url(location: &Url) -> DeltaResult<ObjectStoreUrl> {
    ObjectStoreUrl::parse(&location[..url::Position::BeforePath]).map_err(datafusion_to_delta_error)
}

// Extension trait to add datafusion_table_statistics method to DeltaTableState
pub(crate) trait DeltaTableStateExt {
    fn datafusion_table_statistics(&self, mask: Option<&[bool]>) -> Option<Statistics>;
}

impl DeltaTableStateExt for DeltaTableState {
    fn datafusion_table_statistics(&self, mask: Option<&[bool]>) -> Option<Statistics> {
        if let Some(mask) = mask {
            let es = self.snapshot();
            let boolean_array = BooleanArray::from(mask.to_vec());
            let pruned_files = filter_record_batch(&es.files, &boolean_array).ok()?;
            LogDataHandler::new(&pruned_files, es.table_configuration()).statistics()
        } else {
            self.snapshot().log_data().statistics()
        }
    }
}

#[derive(Debug, Clone)]
/// Used to specify if additional metadata columns are exposed to the user
pub struct DeltaScanConfigBuilder {
    /// Include the source path for each record. The name of this column is determined by `file_column_name`
    include_file_column: bool,
    /// Column name that contains the source path.
    ///
    /// If include_file_column is true and the name is None then it will be auto-generated
    /// Otherwise the user provided name will be used
    file_column_name: Option<String>,
    /// Whether to wrap partition values in a dictionary encoding to potentially save space
    wrap_partition_values: bool,
    /// Whether to push down filter in end result or just prune the files
    enable_parquet_pushdown: bool,
    /// Schema to scan table with
    schema: Option<SchemaRef>,
}

impl Default for DeltaScanConfigBuilder {
    fn default() -> Self {
        DeltaScanConfigBuilder {
            include_file_column: false,
            file_column_name: None,
            wrap_partition_values: true,
            enable_parquet_pushdown: true,
            schema: None,
        }
    }
}

impl DeltaScanConfigBuilder {
    /// Construct a new instance of `DeltaScanConfigBuilder`
    pub fn new() -> Self {
        Self::default()
    }

    /// Indicate that a column containing a records file path is included.
    /// Column name is generated and can be determined once this Config is built
    pub fn with_file_column(mut self, include: bool) -> Self {
        self.include_file_column = include;
        self.file_column_name = None;
        self
    }

    /// Use the provided [SchemaRef] for the [DeltaScan]
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Build a DeltaScanConfig and ensure no column name conflicts occur during downstream processing
    pub fn build(&self, snapshot: &DeltaTableState) -> DeltaResult<DeltaScanConfig> {
        let file_column_name = if self.include_file_column {
            let input_schema = snapshot.input_schema()?;
            let mut column_names: HashSet<&String> = HashSet::new();
            for field in input_schema.fields.iter() {
                column_names.insert(field.name());
            }

            match &self.file_column_name {
                Some(name) => {
                    if column_names.contains(name) {
                        return Err(DeltaTableError::Generic(format!(
                            "Unable to add file path column since column with name {name} exits"
                        )));
                    }

                    Some(name.to_owned())
                }
                None => {
                    let prefix = PATH_COLUMN;
                    let mut idx = 0;
                    let mut name = prefix.to_owned();

                    while column_names.contains(&name) {
                        idx += 1;
                        name = format!("{prefix}_{idx}");
                    }

                    Some(name)
                }
            }
        } else {
            None
        };

        Ok(DeltaScanConfig {
            file_column_name,
            wrap_partition_values: self.wrap_partition_values,
            enable_parquet_pushdown: self.enable_parquet_pushdown,
            schema: self.schema.clone(),
        })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Include additional metadata columns during a [`DeltaScan`]
pub struct DeltaScanConfig {
    /// Include the source path for each record
    pub file_column_name: Option<String>,
    /// Wrap partition values in a dictionary encoding
    pub wrap_partition_values: bool,
    /// Allow pushdown of the scan filter
    pub enable_parquet_pushdown: bool,
    /// Schema to read as
    pub schema: Option<SchemaRef>,
}
