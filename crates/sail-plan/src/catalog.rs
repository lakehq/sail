use std::cmp::Ordering;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::catalog::MemTable;
use datafusion::common::{DFSchemaRef, Result};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::prelude::SessionContext;
use datafusion_common::{exec_datafusion_err, internal_datafusion_err, DFSchema};
use datafusion_expr::{TableScan, UNNAMED_TABLE};
use sail_catalog::command::CatalogCommand;
use sail_catalog::manager::CatalogManager;
use sail_catalog::utils::quote_names_if_needed;
use sail_common_datafusion::catalog::display::CatalogObjectDisplay;
use sail_common_datafusion::catalog::{DatabaseStatus, TableColumnStatus, TableKind, TableStatus};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::PlanFormatter;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::formatter::SparkPlanFormatter;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct CatalogCommandNode {
    name: String,
    schema: DFSchemaRef,
    command: CatalogCommand,
}

#[derive(PartialEq, PartialOrd)]
struct CatalogCommandNodeOrd<'a> {
    name: &'a String,
    command: &'a CatalogCommand,
}

impl<'a> From<&'a CatalogCommandNode> for CatalogCommandNodeOrd<'a> {
    fn from(node: &'a CatalogCommandNode) -> Self {
        Self {
            name: &node.name,
            command: &node.command,
        }
    }
}

impl PartialOrd for CatalogCommandNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        CatalogCommandNodeOrd::from(self).partial_cmp(&other.into())
    }
}

impl CatalogCommandNode {
    pub(crate) fn try_new(ctx: &SessionContext, command: CatalogCommand) -> Result<Self> {
        let schema = command
            .schema(ctx)
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        Ok(Self {
            name: format!("CatalogCommand: {}", command.name()),
            schema: DFSchemaRef::new(DFSchema::try_from(schema)?),
            command,
        })
    }
}

impl CatalogCommandNode {
    pub(crate) async fn execute(&self, ctx: &SessionContext) -> Result<LogicalPlan> {
        let manager = ctx.extension::<CatalogManager>()?;
        let batch = self
            .command
            .clone()
            .execute(ctx, manager.as_ref())
            .await
            .map_err(|e| exec_datafusion_err!("{e}"))?;
        let provider = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        Ok(LogicalPlan::TableScan(TableScan::try_new(
            UNNAMED_TABLE,
            provider_as_source(Arc::new(provider)),
            None,
            vec![],
            None,
        )?))
    }
}

impl UserDefinedLogicalNodeCore for CatalogCommandNode {
    fn name(&self) -> &str {
        &self.name
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        inputs.zero()?;
        Ok(self.clone())
    }
}

mod display {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SparkCatalog {
        pub name: String,
        pub description: Option<String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SparkDatabase {
        pub name: String,
        pub catalog: Option<String>,
        pub description: Option<String>,
        pub location_uri: Option<String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SparkTable {
        pub name: String,
        pub catalog: Option<String>,
        pub namespace: Vec<String>,
        pub description: Option<String>,
        pub table_type: String,
        pub is_temporary: bool,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SparkTableColumn {
        pub name: String,
        pub description: Option<String>,
        pub data_type: String,
        pub nullable: bool,
        pub is_partition: bool,
        pub is_bucket: bool,
        pub is_cluster: bool, // new in Spark 4.0
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SparkFunction {
        pub name: String,
        pub catalog: Option<String>,
        pub namespace: Option<Vec<String>>,
        pub description: Option<String>,
        pub class_name: String,
        pub is_temporary: bool,
    }
}

#[derive(Default)]
pub struct SparkCatalogObjectDisplay;

impl CatalogObjectDisplay for SparkCatalogObjectDisplay {
    type Catalog = display::SparkCatalog;
    type Database = display::SparkDatabase;
    type Table = display::SparkTable;
    type TableColumn = display::SparkTableColumn;
    type Function = display::SparkFunction;

    fn catalog(name: String) -> Self::Catalog {
        Self::Catalog {
            name,
            description: None, // Spark code sets all descriptions to None
        }
    }

    fn database(status: DatabaseStatus) -> Self::Database {
        Self::Database {
            name: quote_names_if_needed(&status.database),
            catalog: Some(status.catalog),
            description: status.comment,
            location_uri: status.location,
        }
    }

    fn table(status: TableStatus) -> Self::Table {
        let table_type = match status.kind {
            TableKind::Table { .. } => "MANAGED",
            TableKind::View { .. } => "VIEW",
            TableKind::TemporaryView { .. } => "TEMPORARY",
            TableKind::GlobalTemporaryView { .. } => "TEMPORARY",
        };
        let is_temporary = match status.kind {
            TableKind::Table { .. } | TableKind::View { .. } => false,
            TableKind::TemporaryView { .. } | TableKind::GlobalTemporaryView { .. } => true,
        };
        Self::Table {
            name: status.name,
            catalog: status.kind.catalog(),
            namespace: status.kind.database(),
            description: status.kind.comment(),
            table_type: table_type.to_string(),
            is_temporary,
        }
    }

    fn table_column(status: TableColumnStatus) -> Self::TableColumn {
        let data_type = SparkPlanFormatter
            .data_type_to_simple_string(&status.data_type)
            .unwrap_or("invalid".to_string());
        Self::TableColumn {
            name: status.name,
            description: status.comment,
            data_type,
            nullable: status.nullable,
            is_partition: status.is_partition,
            is_bucket: status.is_bucket,
            is_cluster: status.is_cluster,
        }
    }

    fn function(name: String) -> Self::Function {
        Self::Function {
            name,
            catalog: None,
            namespace: None,
            description: None,
            class_name: "".to_string(),
            is_temporary: false,
        }
    }
}
