use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::schema_rewriter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};
use datafusion::physical_expr::PhysicalExpr;

use crate::delta_datafusion::type_converter::DeltaTypeConverter;

/// A Physical Expression Adapter Factory which provides casting and rewriting of physical expressions
/// for Delta Lake conventions.
#[derive(Debug)]
pub struct DeltaPhysicalExprAdapterFactory {}

impl PhysicalExprAdapterFactory for DeltaPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Arc<dyn PhysicalExprAdapter> {
        let (column_mapping, default_values) =
            Self::create_column_mapping(&logical_file_schema, &physical_file_schema);

        Arc::new(DeltaPhysicalExprAdapter {
            logical_file_schema,
            physical_file_schema,
            partition_values: Vec::new(),
            column_mapping,
            default_values,
        })
    }
}

impl DeltaPhysicalExprAdapterFactory {
    /// Create column mapping and default values for schema evolution
    fn create_column_mapping(
        logical_schema: &Schema,
        physical_schema: &Schema,
    ) -> (Vec<Option<usize>>, Vec<Option<ScalarValue>>) {
        let mut column_mapping = Vec::with_capacity(logical_schema.fields().len());
        let mut default_values = Vec::with_capacity(logical_schema.fields().len());

        for logical_field in logical_schema.fields() {
            match physical_schema.index_of(logical_field.name()) {
                Ok(physical_index) => {
                    column_mapping.push(Some(physical_index));
                    default_values.push(None);
                }
                Err(_) => {
                    column_mapping.push(None);
                    let default_value = if logical_field.is_nullable() {
                        Some(
                            ScalarValue::try_from(logical_field.data_type())
                                .unwrap_or(ScalarValue::Null),
                        )
                    } else {
                        Some(Self::create_default_value(logical_field.data_type()))
                    };
                    default_values.push(default_value);
                }
            }
        }

        (column_mapping, default_values)
    }

    fn create_default_value(data_type: &DataType) -> ScalarValue {
        match data_type {
            DataType::Boolean => ScalarValue::Boolean(Some(false)),
            DataType::Int8 => ScalarValue::Int8(Some(0)),
            DataType::Int16 => ScalarValue::Int16(Some(0)),
            DataType::Int32 => ScalarValue::Int32(Some(0)),
            DataType::Int64 => ScalarValue::Int64(Some(0)),
            DataType::UInt8 => ScalarValue::UInt8(Some(0)),
            DataType::UInt16 => ScalarValue::UInt16(Some(0)),
            DataType::UInt32 => ScalarValue::UInt32(Some(0)),
            DataType::UInt64 => ScalarValue::UInt64(Some(0)),
            DataType::Float16 => ScalarValue::Float32(Some(0.0)),
            DataType::Float32 => ScalarValue::Float32(Some(0.0)),
            DataType::Float64 => ScalarValue::Float64(Some(0.0)),
            DataType::Utf8 => ScalarValue::Utf8(Some(String::new())),
            DataType::LargeUtf8 => ScalarValue::LargeUtf8(Some(String::new())),
            DataType::Binary => ScalarValue::Binary(Some(Vec::new())),
            DataType::LargeBinary => ScalarValue::LargeBinary(Some(Vec::new())),
            DataType::Date32 => ScalarValue::Date32(Some(0)),
            DataType::Date64 => ScalarValue::Date64(Some(0)),
            DataType::Time32(_) => ScalarValue::Time32Second(Some(0)),
            DataType::Time64(_) => ScalarValue::Time64Nanosecond(Some(0)),
            DataType::Timestamp(unit, tz) => match unit {
                datafusion::arrow::datatypes::TimeUnit::Second => {
                    ScalarValue::TimestampSecond(Some(0), tz.clone())
                }
                datafusion::arrow::datatypes::TimeUnit::Millisecond => {
                    ScalarValue::TimestampMillisecond(Some(0), tz.clone())
                }
                datafusion::arrow::datatypes::TimeUnit::Microsecond => {
                    ScalarValue::TimestampMicrosecond(Some(0), tz.clone())
                }
                datafusion::arrow::datatypes::TimeUnit::Nanosecond => {
                    ScalarValue::TimestampNanosecond(Some(0), tz.clone())
                }
            },
            _ => ScalarValue::Null,
        }
    }
}

/// A Physical Expression Adapter that handles Delta Lake specific expression rewriting
/// with schema evolution support
#[derive(Debug)]
pub(crate) struct DeltaPhysicalExprAdapter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
    partition_values: Vec<(FieldRef, ScalarValue)>,
    /// Column mapping from logical to physical schema indices
    column_mapping: Vec<Option<usize>>,
    /// Default values for missing columns in physical schema
    default_values: Vec<Option<ScalarValue>>,
}

impl PhysicalExprAdapter for DeltaPhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let rewriter = DeltaPhysicalExprRewriter {
            logical_file_schema: &self.logical_file_schema,
            physical_file_schema: &self.physical_file_schema,
            partition_values: &self.partition_values,
            column_mapping: &self.column_mapping,
            default_values: &self.default_values,
        };
        expr.transform(|expr| rewriter.rewrite_expr(Arc::clone(&expr)))
            .data()
    }

    fn with_partition_values(
        &self,
        partition_values: Vec<(FieldRef, ScalarValue)>,
    ) -> Arc<dyn PhysicalExprAdapter> {
        Arc::new(DeltaPhysicalExprAdapter {
            logical_file_schema: Arc::clone(&self.logical_file_schema),
            physical_file_schema: Arc::clone(&self.physical_file_schema),
            partition_values,
            column_mapping: self.column_mapping.clone(),
            default_values: self.default_values.clone(),
        })
    }
}

impl Clone for DeltaPhysicalExprAdapter {
    fn clone(&self) -> Self {
        Self {
            logical_file_schema: Arc::clone(&self.logical_file_schema),
            physical_file_schema: Arc::clone(&self.physical_file_schema),
            partition_values: self.partition_values.clone(),
            column_mapping: self.column_mapping.clone(),
            default_values: self.default_values.clone(),
        }
    }
}

struct DeltaPhysicalExprRewriter<'a> {
    logical_file_schema: &'a Schema,
    physical_file_schema: &'a Schema,
    partition_values: &'a [(FieldRef, ScalarValue)],
    column_mapping: &'a [Option<usize>],
    default_values: &'a [Option<ScalarValue>],
}

impl<'a> DeltaPhysicalExprRewriter<'a> {
    fn rewrite_expr(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        // Handle column references with schema evolution
        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            return self.rewrite_column(Arc::clone(&expr), column);
        }

        // For non-column expressions, continue with default behavior
        Ok(Transformed::no(expr))
    }

    fn rewrite_column(
        &self,
        expr: Arc<dyn PhysicalExpr>,
        column: &Column,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        // First check if this is a partition column
        if let Some(partition_value) = self.get_partition_value(column.name()) {
            return Ok(Transformed::yes(Arc::new(Literal::new(partition_value))));
        }

        // Get the logical field for this column
        let logical_field_index = match self.logical_file_schema.index_of(column.name()) {
            Ok(index) => index,
            Err(_) => {
                if let Ok(_physical_field) =
                    self.physical_file_schema.field_with_name(column.name())
                {
                    return Ok(Transformed::no(expr));
                } else {
                    return exec_err!(
                        "Column '{}' not found in either logical or physical schema",
                        column.name()
                    );
                }
            }
        };

        let logical_field = self.logical_file_schema.field(logical_field_index);

        // Check column mapping to see if this column exists in the physical schema
        match self.column_mapping.get(logical_field_index) {
            Some(Some(physical_index)) => {
                let physical_field = self.physical_file_schema.field(*physical_index);
                self.handle_existing_column(
                    expr,
                    column,
                    logical_field,
                    physical_field,
                    *physical_index,
                )
            }
            Some(None) => {
                if let Some(Some(default_value)) = self.default_values.get(logical_field_index) {
                    Ok(Transformed::yes(Arc::new(Literal::new(
                        default_value.clone(),
                    ))))
                } else if logical_field.is_nullable() {
                    let null_value = ScalarValue::Null.cast_to(logical_field.data_type())?;
                    Ok(Transformed::yes(Arc::new(Literal::new(null_value))))
                } else {
                    exec_err!("Non-nullable column '{}' is missing from physical schema and no default value provided", column.name())
                }
            }
            None => {
                exec_err!(
                    "Column mapping not found for logical field index {}",
                    logical_field_index
                )
            }
        }
    }

    fn handle_existing_column(
        &self,
        expr: Arc<dyn PhysicalExpr>,
        column: &Column,
        logical_field: &Field,
        physical_field: &Field,
        physical_index: usize,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        let needs_index_update = column.index() != physical_index;
        let needs_type_cast = logical_field.data_type() != physical_field.data_type();

        match (needs_index_update, needs_type_cast) {
            (false, false) => {
                // No changes needed
                Ok(Transformed::no(expr))
            }
            (true, false) => {
                // Only index needs updating
                let new_column =
                    Column::new_with_schema(logical_field.name(), self.physical_file_schema)?;
                Ok(Transformed::yes(Arc::new(new_column)))
            }
            (false, true) => {
                // Only type casting needed
                self.apply_type_cast(expr, logical_field, physical_field)
            }
            (true, true) => {
                // Both index update and type casting needed
                let new_column =
                    Column::new_with_schema(logical_field.name(), self.physical_file_schema)?;
                self.apply_type_cast(Arc::new(new_column), logical_field, physical_field)
            }
        }
    }

    fn apply_type_cast(
        &self,
        column_expr: Arc<dyn PhysicalExpr>,
        logical_field: &Field,
        physical_field: &Field,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        // Check if the cast is possible
        if !can_cast_types(physical_field.data_type(), logical_field.data_type()) {
            return exec_err!(
                "Cannot cast column '{}' from '{}' (physical) to '{}' (logical)",
                logical_field.name(),
                physical_field.data_type(),
                logical_field.data_type()
            );
        }

        let cast_expr = self.create_delta_cast(column_expr, logical_field.data_type())?;
        Ok(Transformed::yes(cast_expr))
    }

    fn create_delta_cast(
        &self,
        expr: Arc<dyn PhysicalExpr>,
        target_type: &DataType,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        DeltaTypeConverter::create_cast_expr(expr, target_type)
    }

    fn get_partition_value(&self, column_name: &str) -> Option<ScalarValue> {
        self.partition_values
            .iter()
            .find(|(field, _)| field.name() == column_name)
            .map(|(_, value)| value.clone())
    }
}
