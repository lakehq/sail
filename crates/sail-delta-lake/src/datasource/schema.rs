use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, Schema as ArrowSchema, SchemaRef, SchemaRef as ArrowSchemaRef,
};
use datafusion::common::DFSchema;
use datafusion::execution::SessionState;
use datafusion::logical_expr::Expr;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use deltalake::errors::{DeltaResult, DeltaTableError};

use crate::datasource::error::datafusion_to_delta_error;
use crate::datasource::expressions::parse_predicate_expression;
use crate::kernel::snapshot::{EagerSnapshot, LogDataHandler, Snapshot};
use crate::table::DeltaTableState;

/// Convenience trait for calling common methods on snapshot hierarchies
pub trait DataFusionMixins {
    /// The physical datafusion schema of a table
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef>;

    /// Get the table schema as an [`ArrowSchemaRef`]
    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef>;

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr>;
}

impl DataFusionMixins for Snapshot {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        arrow_schema_impl(self, true)
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        arrow_schema_impl(self, false)
    }

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        let schema = DFSchema::try_from(self.arrow_schema()?.as_ref().to_owned())
            .map_err(datafusion_to_delta_error)?;
        parse_predicate_expression(&schema, expr, df_state)
    }
}

impl DataFusionMixins for EagerSnapshot {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        arrow_schema_from_struct_type(self.schema(), self.metadata().partition_columns(), true)
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        arrow_schema_from_struct_type(self.schema(), self.metadata().partition_columns(), false)
    }

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        let schema = DFSchema::try_from(self.arrow_schema()?.as_ref().to_owned())
            .map_err(datafusion_to_delta_error)?;
        parse_predicate_expression(&schema, expr, df_state)
    }
}

impl DataFusionMixins for DeltaTableState {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        Ok(Arc::new(self.schema().try_into_arrow()?))
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        self.arrow_schema()
    }

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        let schema = DFSchema::try_from(self.arrow_schema()?.as_ref().to_owned())
            .map_err(datafusion_to_delta_error)?;
        parse_predicate_expression(&schema, expr, df_state)
    }
}

impl DataFusionMixins for LogDataHandler<'_> {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        unimplemented!("arrow_schema for LogDataHandler");
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        unimplemented!("input_schema for LogDataHandler");
    }

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        let schema = DFSchema::try_from(self.arrow_schema()?.as_ref().to_owned())
            .map_err(datafusion_to_delta_error)?;
        parse_predicate_expression(&schema, expr, df_state)
    }
}

fn arrow_schema_from_snapshot(
    snapshot: &Snapshot,
    wrap_partitions: bool,
) -> DeltaResult<ArrowSchemaRef> {
    let meta = snapshot.metadata();
    let schema = snapshot.schema();

    let fields = schema
        .fields()
        .filter(|f| !meta.partition_columns().contains(&f.name().to_string()))
        .map(|f| {
            let field_name = f.name().to_string();
            let field: Field = f.try_into_arrow()?;
            let field_type = field.data_type().clone();
            Ok(Field::new(field_name, field_type, f.is_nullable()))
        })
        .chain(meta.partition_columns().iter().map(|partition_col| {
            #[allow(clippy::expect_used)]
            let f = schema
                .field(partition_col)
                .expect("Partition column should exist in schema");
            let field: Field = f.try_into_arrow()?;
            let field_name = f.name().to_string();
            let field_type = field.data_type().clone();
            let field = Field::new(field_name, field_type, f.is_nullable());
            let corrected = if wrap_partitions {
                match field.data_type() {
                    ArrowDataType::Utf8
                    | ArrowDataType::LargeUtf8
                    | ArrowDataType::Binary
                    | ArrowDataType::LargeBinary => {
                        datafusion::datasource::physical_plan::wrap_partition_type_in_dict(
                            field.data_type().clone(),
                        )
                    }
                    _ => field.data_type().clone(),
                }
            } else {
                field.data_type().clone()
            };
            Ok(field.with_data_type(corrected))
        }))
        .collect::<Result<Vec<Field>, DeltaTableError>>()?;

    Ok(Arc::new(ArrowSchema::new(fields)))
}

pub fn arrow_schema_from_struct_type(
    schema: &delta_kernel::schema::StructType,
    partition_columns: &[String],
    wrap_partitions: bool,
) -> DeltaResult<ArrowSchemaRef> {
    let fields = schema
        .fields()
        .filter(|f| !partition_columns.contains(&f.name().to_string()))
        .map(|f| {
            let field_name = f.name().to_string();
            let field: Field = f.try_into_arrow()?;
            let field_type = field.data_type().clone();
            Ok(Field::new(field_name, field_type, f.is_nullable()))
        })
        .chain(partition_columns.iter().map(|partition_col| {
            #[allow(clippy::expect_used)]
            let f = schema
                .field(partition_col)
                .expect("Partition column should exist in schema");
            let field: Field = f.try_into_arrow()?;
            let field_name = f.name().to_string();
            let field_type = field.data_type().clone();
            let field = Field::new(field_name, field_type, f.is_nullable());
            let corrected = if wrap_partitions {
                match field.data_type() {
                    ArrowDataType::Utf8
                    | ArrowDataType::LargeUtf8
                    | ArrowDataType::Binary
                    | ArrowDataType::LargeBinary => {
                        datafusion::datasource::physical_plan::wrap_partition_type_in_dict(
                            field.data_type().clone(),
                        )
                    }
                    _ => field.data_type().clone(),
                }
            } else {
                field.data_type().clone()
            };
            Ok(field.with_data_type(corrected))
        }))
        .collect::<Result<Vec<Field>, DeltaTableError>>()?;

    Ok(Arc::new(ArrowSchema::new(fields)))
}

fn arrow_schema_impl(snapshot: &Snapshot, wrap_partitions: bool) -> DeltaResult<ArrowSchemaRef> {
    arrow_schema_from_snapshot(snapshot, wrap_partitions)
}

/// The logical schema for a Deltatable is different from the protocol level schema since partition
/// columns must appear at the end of the schema. This is to align with how partition are handled
/// at the physical level
pub fn df_logical_schema(
    snapshot: &DeltaTableState,
    file_column_name: &Option<String>,
    schema: Option<ArrowSchemaRef>,
) -> DeltaResult<SchemaRef> {
    let input_schema = match schema {
        Some(schema) => schema,
        None => snapshot.input_schema()?,
    };
    let table_partition_cols = &snapshot.metadata().partition_columns();

    let mut fields: Vec<Arc<Field>> = input_schema
        .fields()
        .iter()
        .filter(|f| !table_partition_cols.contains(f.name()))
        .cloned()
        .collect();

    for partition_col in table_partition_cols.iter() {
        #[allow(clippy::expect_used)]
        fields.push(Arc::new(
            input_schema
                .field_with_name(partition_col)
                .expect("Partition column should exist in input schema")
                .to_owned(),
        ));
    }

    if let Some(file_column_name) = file_column_name {
        fields.push(Arc::new(Field::new(
            file_column_name,
            ArrowDataType::Utf8,
            true,
        )));
    }

    Ok(Arc::new(ArrowSchema::new(fields)))
}
