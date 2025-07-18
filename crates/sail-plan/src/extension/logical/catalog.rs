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
use sail_catalog::descriptor::DescriptorFactory;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::{NamespaceMetadata, TableColumnMetadata, TableKind, TableMetadata};
use sail_catalog::utils::quote_names_if_needed;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use serde::{Deserialize, Serialize};

use crate::config::PlanConfig;
use crate::formatter::{PlanFormatter, SparkPlanFormatter};
use crate::utils::ItemTaker;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct CatalogCommandNode {
    name: String,
    schema: DFSchemaRef,
    command: CatalogCommand,
    config: Arc<PlanConfig>,
}

#[derive(PartialEq, PartialOrd)]
struct CatalogCommandNodeOrd<'a> {
    name: &'a String,
    command: &'a CatalogCommand,
    config: &'a Arc<PlanConfig>,
}

impl<'a> From<&'a CatalogCommandNode> for CatalogCommandNodeOrd<'a> {
    fn from(node: &'a CatalogCommandNode) -> Self {
        Self {
            name: &node.name,
            command: &node.command,
            config: &node.config,
        }
    }
}

impl PartialOrd for CatalogCommandNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        CatalogCommandNodeOrd::from(self).partial_cmp(&other.into())
    }
}

impl CatalogCommandNode {
    pub(crate) fn try_new(command: CatalogCommand, config: Arc<PlanConfig>) -> Result<Self> {
        let schema = command
            .schema::<SparkDescriptorFactory>()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        Ok(Self {
            name: format!("CatalogCommand: {}", command.name()),
            schema: DFSchemaRef::new(DFSchema::try_from(schema)?),
            command,
            config,
        })
    }
}

impl CatalogCommandNode {
    pub(crate) async fn execute(&self, ctx: &SessionContext) -> Result<LogicalPlan> {
        let manager = ctx.extension::<CatalogManager>()?;
        let batch = self
            .command
            .clone()
            .execute::<SparkDescriptorFactory>(ctx, manager.as_ref())
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CatalogDescriptor {
    name: String,
    description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DatabaseDescriptor {
    pub name: String,
    pub catalog: Option<String>,
    pub description: Option<String>,
    pub location_uri: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDescriptor {
    pub name: String,
    pub catalog: Option<String>,
    pub namespace: Vec<String>,
    pub description: Option<String>,
    pub table_type: String,
    pub is_temporary: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TableColumnDescriptor {
    name: String,
    description: Option<String>,
    data_type: String,
    nullable: bool,
    is_partition: bool,
    is_bucket: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FunctionDescriptor {
    pub name: String,
    pub catalog: Option<String>,
    pub namespace: Option<Vec<String>>,
    pub description: Option<String>,
    pub class_name: String,
    pub is_temporary: bool,
}

struct SparkDescriptorFactory;

impl DescriptorFactory for SparkDescriptorFactory {
    type Catalog = CatalogDescriptor;
    type Database = DatabaseDescriptor;
    type Table = TableDescriptor;
    type TableColumn = TableColumnDescriptor;
    type Function = FunctionDescriptor;

    fn catalog(name: String) -> Self::Catalog {
        Self::Catalog {
            name,
            description: None, // Spark code sets all descriptions to None
        }
    }

    fn database(metadata: NamespaceMetadata) -> Self::Database {
        Self::Database {
            name: quote_names_if_needed(&metadata.namespace),
            catalog: Some(metadata.catalog),
            description: None,
            location_uri: None,
        }
    }

    fn table(metadata: TableMetadata) -> Self::Table {
        let table_type = match metadata.kind {
            TableKind::Table { .. } => "MANAGED",
            TableKind::View { .. } => "VIEW",
            TableKind::TemporaryView { .. } => "TEMPORARY",
            TableKind::GlobalTemporaryView { .. } => "TEMPORARY",
        };
        let is_temporary = match metadata.kind {
            TableKind::Table { .. } | TableKind::View { .. } => false,
            TableKind::TemporaryView { .. } | TableKind::GlobalTemporaryView { .. } => true,
        };
        let catalog = metadata.catalog();
        let namespace = metadata.namespace();
        let description = metadata.description();
        Self::Table {
            name: metadata.name,
            catalog,
            namespace,
            description,
            table_type: table_type.to_string(),
            is_temporary,
        }
    }

    fn table_column(metadata: TableColumnMetadata) -> Self::TableColumn {
        let data_type = SparkPlanFormatter
            .data_type_to_simple_string(&metadata.data_type)
            .unwrap_or("invalid".to_string());
        Self::TableColumn {
            name: metadata.name,
            description: metadata.description,
            data_type,
            nullable: metadata.nullable,
            is_partition: metadata.is_partition,
            is_bucket: metadata.is_bucket,
        }
    }
}
