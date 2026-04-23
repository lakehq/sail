use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::Result;
use datafusion_expr::LogicalPlan;

use crate::catalog::{
    CatalogPartitionField, CatalogTableBucketBy, CatalogTableConstraint, CatalogTableSort,
};
use crate::column_features::ColumnFeaturesBuilder;
use crate::session::plan::PlanFormatter;

/// Metadata key used by Spark Connect's column protocol for generation
/// expressions. This is an input/output boundary value translated to the
/// engine's canonical [`crate::column_features::ColumnFeatureKey`] at the
/// protocol layer.
pub const SPARK_GENERATION_EXPRESSION_METADATA_KEY: &str = "GENERATION_EXPRESSION";

#[derive(Debug, Clone)]
pub struct DatabaseStatus {
    pub catalog: String,
    pub database: Vec<String>,
    pub comment: Option<String>,
    pub location: Option<String>,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct TableStatus {
    pub catalog: Option<String>,
    pub database: Vec<String>,
    pub name: String,
    pub kind: TableKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableKind {
    Table {
        columns: Vec<TableColumnStatus>,
        comment: Option<String>,
        constraints: Vec<CatalogTableConstraint>,
        location: Option<String>,
        format: String,
        partition_by: Vec<CatalogPartitionField>,
        sort_by: Vec<CatalogTableSort>,
        bucket_by: Option<CatalogTableBucketBy>,
        options: Vec<(String, String)>,
        properties: Vec<(String, String)>,
    },
    View {
        definition: String,
        columns: Vec<TableColumnStatus>,
        comment: Option<String>,
        properties: Vec<(String, String)>,
    },
    TemporaryView {
        plan: Arc<LogicalPlan>,
        columns: Vec<TableColumnStatus>,
        comment: Option<String>,
        properties: Vec<(String, String)>,
    },
    GlobalTemporaryView {
        plan: Arc<LogicalPlan>,
        columns: Vec<TableColumnStatus>,
        comment: Option<String>,
        properties: Vec<(String, String)>,
    },
}

impl TableKind {
    pub fn columns(&self) -> Vec<TableColumnStatus> {
        match &self {
            TableKind::Table { columns, .. }
            | TableKind::View { columns, .. }
            | TableKind::TemporaryView { columns, .. }
            | TableKind::GlobalTemporaryView { columns, .. } => columns.clone(),
        }
    }

    pub fn comment(&self) -> Option<String> {
        match &self {
            TableKind::Table { comment, .. }
            | TableKind::View { comment, .. }
            | TableKind::TemporaryView { comment, .. }
            | TableKind::GlobalTemporaryView { comment, .. } => comment.clone(),
        }
    }

    pub fn type_name(&self) -> &str {
        match self {
            TableKind::Table { .. } => "MANAGED",
            TableKind::View { .. } => "VIEW",
            TableKind::TemporaryView { .. } => "TEMPORARY",
            TableKind::GlobalTemporaryView { .. } => "TEMPORARY",
        }
    }

    pub fn is_temporary(&self) -> bool {
        matches!(
            self,
            TableKind::TemporaryView { .. } | TableKind::GlobalTemporaryView { .. }
        )
    }

    pub fn properties(&self) -> &[(String, String)] {
        match self {
            TableKind::Table { properties, .. }
            | TableKind::View { properties, .. }
            | TableKind::TemporaryView { properties, .. }
            | TableKind::GlobalTemporaryView { properties, .. } => properties,
        }
    }

    pub fn partition_columns(&self) -> Vec<&TableColumnStatus> {
        match self {
            TableKind::Table {
                columns,
                partition_by,
                ..
            } if !partition_by.is_empty() => columns.iter().filter(|c| c.is_partition).collect(),
            _ => vec![],
        }
    }

    pub fn format(&self) -> Option<&str> {
        match self {
            TableKind::Table { format, .. } => Some(format),
            _ => None,
        }
    }

    pub fn location(&self) -> Option<&str> {
        match self {
            TableKind::Table {
                location: Some(loc),
                ..
            } => Some(loc),
            _ => None,
        }
    }

    pub fn view_definition(&self) -> Option<&str> {
        match self {
            TableKind::View { definition, .. } if !definition.is_empty() => Some(definition),
            _ => None,
        }
    }
}

impl TableStatus {
    /// Returns metadata key-value pairs for the DESCRIBE EXTENDED output,
    /// following Spark's CatalogTable.toLinkedHashMap row ordering.
    pub fn describe_extended_metadata(&self) -> Vec<(String, String)> {
        let mut rows = Vec::new();

        rows.push(("Database".to_string(), self.database.join(".")));
        rows.push(("Table".to_string(), self.name.clone()));
        rows.push(("Type".to_string(), self.kind.type_name().to_string()));

        if let Some(format) = self.kind.format() {
            rows.push(("Provider".to_string(), format.to_string()));
        }

        if let Some(comment) = self.kind.comment() {
            rows.push(("Comment".to_string(), comment));
        }

        if let Some(definition) = self.kind.view_definition() {
            rows.push(("View Text".to_string(), definition.to_string()));
        }

        let properties = self.kind.properties();
        if !properties.is_empty() {
            let props_str = properties
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(", ");
            rows.push(("Table Properties".to_string(), format!("[{props_str}]")));
        }

        if let Some(loc) = self.kind.location() {
            rows.push(("Location".to_string(), loc.to_string()));
        }

        rows
    }

    pub fn show_table_extended_information(&self, formatter: &dyn PlanFormatter) -> Result<String> {
        let mut output = String::new();

        for (key, value) in self.describe_extended_metadata() {
            output.push_str(&format!("{key}: {value}\n"));
        }

        output.push_str("Schema: root\n");
        for column in self.kind.columns() {
            let data_type = formatter
                .data_type_to_simple_string(&column.data_type)
                .unwrap_or_else(|_| "invalid".to_string());
            let nullable = if column.nullable { "true" } else { "false" };
            output.push_str(&format!(
                " |-- {}: {} (nullable = {})\n",
                column.name, data_type, nullable
            ));
        }

        Ok(output)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableColumnStatus {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub comment: Option<String>,
    pub default: Option<String>,
    pub generated_always_as: Option<String>,
    pub is_partition: bool,
    pub is_bucket: bool,
    pub is_cluster: bool,
}

impl TableColumnStatus {
    pub fn field(&self) -> Field {
        let mut metadata = std::collections::HashMap::new();
        if let Some(expr) = &self.generated_always_as {
            let builder = ColumnFeaturesBuilder::new().with_generation_expression(expr.clone());
            metadata.extend(builder.build());
        }
        if let Some(comment) = &self.comment {
            metadata.insert("comment".to_string(), comment.clone());
        }
        let field = Field::new(self.name.clone(), self.data_type.clone(), self.nullable);
        if metadata.is_empty() {
            field
        } else {
            field.with_metadata(metadata)
        }
    }
}

pub fn identity_partition_fields(columns: &[String]) -> Vec<CatalogPartitionField> {
    columns
        .iter()
        .cloned()
        .map(|column| CatalogPartitionField {
            column,
            transform: None,
        })
        .collect()
}
