use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray,
    new_null_array,
};
use datafusion::arrow::compute::{CastOptions, can_cast_types, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use datafusion::physical_expr::expressions::{self, Column, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::format::DEFAULT_CAST_OPTIONS;
use parquet_variant_compute::{VariantArray, unshred_variant};

use crate::variant::{is_binary_variant_field, is_variant_arrow_field, is_variant_storage_type};

#[derive(Debug)]
pub struct SchemaEvolutionPhysicalExprAdapterFactory {}

impl PhysicalExprAdapterFactory for SchemaEvolutionPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        let (column_mapping, default_values) =
            create_column_mapping(&logical_file_schema, &physical_file_schema);

        Ok(Arc::new(SchemaEvolutionPhysicalExprAdapter {
            logical_file_schema,
            physical_file_schema,
            column_mapping,
            default_values,
        }))
    }
}

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
                    Some(
                        ScalarValue::new_zero(logical_field.data_type())
                            .unwrap_or(ScalarValue::Null),
                    )
                };
                default_values.push(default_value);
            }
        }
    }

    (column_mapping, default_values)
}

#[derive(Debug, Clone)]
struct SchemaEvolutionPhysicalExprAdapter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
    column_mapping: Vec<Option<usize>>,
    default_values: Vec<Option<ScalarValue>>,
}

impl PhysicalExprAdapter for SchemaEvolutionPhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let rewriter = SchemaEvolutionPhysicalExprRewriter {
            logical_file_schema: &self.logical_file_schema,
            physical_file_schema: &self.physical_file_schema,
            column_mapping: &self.column_mapping,
            default_values: &self.default_values,
        };
        expr.transform(|expr| rewriter.rewrite_expr(Arc::clone(&expr)))
            .data()
    }
}

struct SchemaEvolutionPhysicalExprRewriter<'a> {
    logical_file_schema: &'a Schema,
    physical_file_schema: &'a Schema,
    column_mapping: &'a [Option<usize>],
    default_values: &'a [Option<ScalarValue>],
}

impl<'a> SchemaEvolutionPhysicalExprRewriter<'a> {
    fn rewrite_expr(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        if let Some(transformed) = self.try_rewrite_struct_field_access(&expr)? {
            return Ok(Transformed::yes(transformed));
        }
        if let Some(column) = expr.downcast_ref::<Column>() {
            return self.rewrite_column(Arc::clone(&expr), column);
        }
        Ok(Transformed::no(expr))
    }

    fn try_rewrite_struct_field_access(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        let get_field_expr =
            match ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(expr.as_ref()) {
                Some(expr) => expr,
                None => return Ok(None),
            };

        let source_expr = match get_field_expr.args().first() {
            Some(expr) => expr,
            None => return Ok(None),
        };
        let field_name_expr = match get_field_expr.args().get(1) {
            Some(expr) => expr,
            None => return Ok(None),
        };

        let lit = match field_name_expr.downcast_ref::<expressions::Literal>() {
            Some(lit) => lit,
            None => return Ok(None),
        };
        let field_name = match lit.value().try_as_str().flatten() {
            Some(name) => name,
            None => return Ok(None),
        };

        let column = match source_expr.downcast_ref::<Column>() {
            Some(column) => column,
            None => return Ok(None),
        };

        let physical_field = match self.physical_file_schema.field_with_name(column.name()) {
            Ok(field) => field,
            Err(_) => return Ok(None),
        };
        let physical_struct_fields = match physical_field.data_type() {
            DataType::Struct(fields) => fields,
            _ => return Ok(None),
        };
        let logical_field = match self.logical_file_schema.field_with_name(column.name()) {
            Ok(field) => field,
            Err(_) => return Ok(None),
        };
        let logical_struct_fields = match logical_field.data_type() {
            DataType::Struct(fields) => fields,
            _ => return Ok(None),
        };
        let logical_struct_field = match logical_struct_fields
            .iter()
            .find(|f| f.name() == field_name)
        {
            Some(field) => field,
            None => return Ok(None),
        };
        if find_matching_struct_field(physical_struct_fields, logical_struct_field).is_some() {
            return Ok(None);
        }

        let null_value = ScalarValue::Null.cast_to(logical_struct_field.data_type())?;
        Ok(Some(Arc::new(Literal::new(null_value))))
    }

    fn rewrite_column(
        &self,
        expr: Arc<dyn PhysicalExpr>,
        column: &Column,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        let logical_field_index = match self.logical_file_schema.index_of(column.name()) {
            Ok(index) => index,
            Err(_) => {
                if self
                    .physical_file_schema
                    .field_with_name(column.name())
                    .is_ok()
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
                    exec_err!(
                        "Non-nullable column '{}' is missing from physical schema and no default value provided",
                        column.name()
                    )
                }
            }
            None => exec_err!(
                "Column mapping not found for logical field index {}",
                logical_field_index
            ),
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
            (false, false) => Ok(Transformed::no(expr)),
            (true, false) => {
                let new_column =
                    Column::new_with_schema(logical_field.name(), self.physical_file_schema)?;
                Ok(Transformed::yes(Arc::new(new_column)))
            }
            (false, true) => self.apply_type_cast(expr, logical_field, physical_field),
            (true, true) => {
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
        if !can_cast_field_with_schema_evolution(physical_field, logical_field)? {
            return exec_err!(
                "Cannot cast column '{}' from '{}' (physical) to '{}' (logical)",
                logical_field.name(),
                physical_field.data_type(),
                logical_field.data_type()
            );
        }

        Ok(Transformed::yes(Arc::new(
            SchemaEvolutionCastColumnExpr::new(
                column_expr,
                Arc::new(physical_field.clone()),
                Arc::new(logical_field.clone()),
                None,
            ),
        )))
    }
}

fn can_cast_field_with_schema_evolution(source: &Field, target: &Field) -> Result<bool> {
    if source.data_type() == &DataType::Null {
        return Ok(target.is_nullable());
    }
    if source.is_nullable() && !target.is_nullable() {
        return Ok(false);
    }
    if source.data_type() == target.data_type() {
        return Ok(true);
    }
    if is_binary_variant_field(source) && is_binary_variant_field(target) {
        return Ok(true);
    }
    if is_variant_arrow_field(target) && is_variant_storage_type(source.data_type()) {
        return Ok(true);
    }

    match (source.data_type(), target.data_type()) {
        (DataType::Struct(from_fields), DataType::Struct(to_fields)) => {
            validate_struct_compatibility_with_variant(from_fields, to_fields)?;
            Ok(true)
        }
        (DataType::List(from_elem), DataType::List(to_elem)) => {
            can_cast_field_with_schema_evolution(from_elem, to_elem)
        }
        (DataType::LargeList(from_elem), DataType::LargeList(to_elem)) => {
            can_cast_field_with_schema_evolution(from_elem, to_elem)
        }
        (
            DataType::FixedSizeList(from_elem, from_len),
            DataType::FixedSizeList(to_elem, to_len),
        ) => {
            if from_len != to_len {
                return Ok(false);
            }
            can_cast_field_with_schema_evolution(from_elem, to_elem)
        }
        (DataType::Map(from_entries, _), DataType::Map(to_entries, _)) => {
            can_cast_field_with_schema_evolution(from_entries, to_entries)
        }
        _ => Ok(can_cast_types(source.data_type(), target.data_type())),
    }
}

const DELTA_COLUMN_MAPPING_PHYSICAL_NAME_METADATA_KEY: &str = "delta.columnMapping.physicalName";
const STRUCT_FIELD_ID_METADATA_KEYS: [&str; 3] = [
    "delta.columnMapping.id",
    PARQUET_FIELD_ID_META_KEY,
    "parquet.field.id",
];

fn struct_field_id(field: &Field) -> Option<&str> {
    STRUCT_FIELD_ID_METADATA_KEYS
        .iter()
        .find_map(|key| field.metadata().get(*key).map(String::as_str))
}

fn find_matching_struct_field<'a>(
    source_fields: &'a [Arc<Field>],
    target_field: &Field,
) -> Option<(usize, &'a Arc<Field>)> {
    if let Some(physical_name) = target_field
        .metadata()
        .get(DELTA_COLUMN_MAPPING_PHYSICAL_NAME_METADATA_KEY)
        && let Some(matched) = source_fields
            .iter()
            .enumerate()
            .find(|(_, source)| source.name() == physical_name)
    {
        return Some(matched);
    }

    if let Some(target_id) = struct_field_id(target_field)
        && let Some(matched) = source_fields
            .iter()
            .enumerate()
            .find(|(_, source)| struct_field_id(source) == Some(target_id))
    {
        return Some(matched);
    }

    source_fields
        .iter()
        .enumerate()
        .find(|(_, source)| source.name() == target_field.name())
}

fn validate_struct_compatibility_with_variant(
    source_fields: &[Arc<Field>],
    target_fields: &[Arc<Field>],
) -> Result<()> {
    if !target_fields
        .iter()
        .any(|target| find_matching_struct_field(source_fields, target).is_some())
    {
        return exec_err!(
            "Cannot cast struct with {} fields to {} fields because there is no field name overlap",
            source_fields.len(),
            target_fields.len()
        );
    }

    for target_field in target_fields {
        match find_matching_struct_field(source_fields, target_field) {
            Some((_, source_field)) => {
                if !can_cast_field_with_schema_evolution(source_field, target_field)? {
                    return exec_err!(
                        "Cannot cast struct field '{}' from type {} to type {}",
                        target_field.name(),
                        source_field.data_type(),
                        target_field.data_type()
                    );
                }
            }
            None if target_field.is_nullable() => {}
            None => {
                return exec_err!(
                    "Cannot cast struct: target field '{}' is non-nullable but missing from source. \
                     Cannot fill with NULL.",
                    target_field.name()
                );
            }
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Eq)]
pub struct SchemaEvolutionCastColumnExpr {
    expr: Arc<dyn PhysicalExpr>,
    input_field: Arc<Field>,
    target_field: Arc<Field>,
    cast_options: CastOptions<'static>,
}

impl PartialEq for SchemaEvolutionCastColumnExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
            && self.input_field.eq(&other.input_field)
            && self.target_field.eq(&other.target_field)
            && self.cast_options.eq(&other.cast_options)
    }
}

impl std::hash::Hash for SchemaEvolutionCastColumnExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.input_field.hash(state);
        self.target_field.hash(state);
        self.cast_options.hash(state);
    }
}

impl std::fmt::Display for SchemaEvolutionCastColumnExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SCHEMA_EVOLUTION_CAST_COLUMN({} AS {:?})",
            self.expr,
            self.target_field.data_type()
        )
    }
}

impl SchemaEvolutionCastColumnExpr {
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        input_field: Arc<Field>,
        target_field: Arc<Field>,
        cast_options: Option<CastOptions<'static>>,
    ) -> Self {
        Self {
            expr,
            input_field,
            target_field,
            cast_options: cast_options.unwrap_or(DEFAULT_CAST_OPTIONS),
        }
    }

    pub fn input_field(&self) -> &Arc<Field> {
        &self.input_field
    }

    pub fn target_field(&self) -> &Arc<Field> {
        &self.target_field
    }
}

impl PhysicalExpr for SchemaEvolutionCastColumnExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.target_field.data_type().clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.target_field.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let value = self.expr.evaluate(batch)?;
        match value {
            ColumnarValue::Array(array) => {
                Ok(ColumnarValue::Array(cast_array_with_schema_evolution(
                    &array,
                    self.target_field.as_ref(),
                    &self.cast_options,
                )?))
            }
            ColumnarValue::Scalar(scalar) => {
                let as_array = scalar.to_array_of_size(1)?;
                let casted = cast_array_with_schema_evolution(
                    &as_array,
                    self.target_field.as_ref(),
                    &self.cast_options,
                )?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    casted.as_ref(),
                    0,
                )?))
            }
        }
    }

    fn return_field(&self, _input_schema: &Schema) -> Result<Arc<Field>> {
        Ok(Arc::clone(&self.target_field))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        assert_eq!(children.len(), 1);
        let child = children.pop().ok_or_else(|| {
            DataFusionError::Plan("SchemaEvolutionCastColumnExpr requires a child".to_string())
        })?;
        Ok(Arc::new(Self::new(
            child,
            Arc::clone(&self.input_field),
            Arc::clone(&self.target_field),
            Some(self.cast_options.clone()),
        )))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

fn cast_array_with_schema_evolution(
    source: &ArrayRef,
    target_field: &Field,
    cast_options: &CastOptions,
) -> Result<ArrayRef> {
    if is_variant_arrow_field(target_field) && is_variant_storage_type(source.data_type()) {
        return cast_variant_array_with_schema_evolution(source, target_field, cast_options);
    }

    match target_field.data_type() {
        DataType::Struct(target_fields) => {
            cast_struct_array_with_schema_evolution(source, target_fields, cast_options)
        }
        DataType::List(target_elem) => {
            let Some(source_list) = source.as_any().downcast_ref::<ListArray>() else {
                return exec_err!(
                    "Cannot cast column of type {} to list type. Source must be a list to cast to list.",
                    source.data_type()
                );
            };
            let casted_values = cast_array_with_schema_evolution(
                source_list.values(),
                target_elem.as_ref(),
                cast_options,
            )?;
            Ok(Arc::new(ListArray::new(
                Arc::clone(target_elem),
                source_list.offsets().clone(),
                casted_values,
                source_list.nulls().cloned(),
            )))
        }
        DataType::LargeList(target_elem) => {
            let Some(source_list) = source.as_any().downcast_ref::<LargeListArray>() else {
                return exec_err!(
                    "Cannot cast column of type {} to large list type. Source must be a large list to cast to large list.",
                    source.data_type()
                );
            };
            let casted_values = cast_array_with_schema_evolution(
                source_list.values(),
                target_elem.as_ref(),
                cast_options,
            )?;
            Ok(Arc::new(LargeListArray::new(
                Arc::clone(target_elem),
                source_list.offsets().clone(),
                casted_values,
                source_list.nulls().cloned(),
            )))
        }
        DataType::FixedSizeList(target_elem, target_len) => {
            let Some(source_list) = source.as_any().downcast_ref::<FixedSizeListArray>() else {
                return exec_err!(
                    "Cannot cast column of type {} to fixed size list type. Source must be a fixed size list to cast to fixed size list.",
                    source.data_type()
                );
            };
            let source_len = source_list.value_length();
            if &source_len != target_len {
                return exec_err!(
                    "Cannot cast fixed size list with length {} to length {}",
                    source_len,
                    target_len
                );
            }
            let casted_values = cast_array_with_schema_evolution(
                source_list.values(),
                target_elem.as_ref(),
                cast_options,
            )?;
            Ok(Arc::new(FixedSizeListArray::new(
                Arc::clone(target_elem),
                *target_len,
                casted_values,
                source_list.nulls().cloned(),
            )))
        }
        DataType::Map(target_entries, ordered) => {
            let Some(source_map) = source.as_any().downcast_ref::<MapArray>() else {
                return exec_err!(
                    "Cannot cast column of type {} to map type. Source must be a map to cast to map.",
                    source.data_type()
                );
            };

            let DataType::Struct(target_kv_fields) = target_entries.data_type() else {
                return exec_err!(
                    "Invalid map entries type {}, expected struct",
                    target_entries.data_type()
                );
            };

            let num_entries = source_map.entries().len();
            let mut kv_arrays: Vec<ArrayRef> = Vec::with_capacity(target_kv_fields.len());
            let mut kv_fields: Vec<Arc<Field>> = Vec::with_capacity(target_kv_fields.len());

            for target_child in target_kv_fields {
                kv_fields.push(Arc::clone(target_child));
                match source_map.entries().column_by_name(target_child.name()) {
                    Some(source_child) => kv_arrays.push(cast_array_with_schema_evolution(
                        source_child,
                        target_child.as_ref(),
                        cast_options,
                    )?),
                    None => kv_arrays.push(new_null_array(target_child.data_type(), num_entries)),
                }
            }

            let new_entries = StructArray::new(kv_fields.into(), kv_arrays, None);
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(target_entries),
                source_map.offsets().clone(),
                new_entries,
                source_map.nulls().cloned(),
                *ordered,
            )?))
        }
        _ => Ok(cast_with_options(
            source,
            target_field.data_type(),
            cast_options,
        )?),
    }
}

fn cast_variant_array_with_schema_evolution(
    source: &ArrayRef,
    target_field: &Field,
    cast_options: &CastOptions,
) -> Result<ArrayRef> {
    let DataType::Struct(target_fields) = target_field.data_type() else {
        return exec_err!(
            "Cannot cast variant column to non-struct type {}",
            target_field.data_type()
        );
    };
    let variant = VariantArray::try_new(source.as_ref())?;
    let unshredded = unshred_variant(&variant)?;
    let unshredded = Arc::new(unshredded.into_inner()) as ArrayRef;
    cast_struct_array_to_fields(&unshredded, target_fields, cast_options)
}

fn cast_struct_array_with_schema_evolution(
    source: &ArrayRef,
    target_fields: &[Arc<Field>],
    cast_options: &CastOptions,
) -> Result<ArrayRef> {
    if source.data_type() == &DataType::Null
        || (!source.is_empty() && source.null_count() == source.len())
    {
        return Ok(new_null_array(
            &DataType::Struct(target_fields.to_vec().into()),
            source.len(),
        ));
    }

    let Some(source_struct) = source.as_any().downcast_ref::<StructArray>() else {
        return exec_err!(
            "Cannot cast column of type {} to struct type. Source must be a struct to cast to struct.",
            source.data_type()
        );
    };
    validate_struct_compatibility_with_variant(source_struct.fields(), target_fields)?;
    cast_struct_array_to_fields(source, target_fields, cast_options)
}

fn cast_struct_array_to_fields(
    source: &ArrayRef,
    target_fields: &[Arc<Field>],
    cast_options: &CastOptions,
) -> Result<ArrayRef> {
    let Some(source_struct) = source.as_any().downcast_ref::<StructArray>() else {
        return exec_err!(
            "Cannot cast column of type {} to struct type. Source must be a struct to cast to struct.",
            source.data_type()
        );
    };
    let num_rows = source.len();
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(target_fields.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(target_fields.len());

    for target_child in target_fields {
        fields.push(Arc::clone(target_child));
        match find_matching_struct_field(source_struct.fields(), target_child) {
            Some((source_index, _)) => {
                arrays.push(cast_array_with_schema_evolution(
                    source_struct.column(source_index),
                    target_child.as_ref(),
                    cast_options,
                )?);
            }
            None => arrays.push(new_null_array(target_child.data_type(), num_rows)),
        }
    }

    Ok(Arc::new(StructArray::new(
        fields.into(),
        arrays,
        source_struct.nulls().cloned(),
    )))
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::array::{BinaryViewArray, Int64Array, StringArray};
    use datafusion::arrow::buffer::OffsetBuffer;
    use parquet_variant_compute::{VariantType, json_to_variant, shred_variant, variant_to_json};

    use super::*;

    fn variant_field(name: &str) -> Field {
        Field::new(
            name,
            DataType::Struct(
                vec![
                    Field::new("metadata", DataType::Binary, false),
                    Field::new("value", DataType::Binary, false),
                ]
                .into(),
            ),
            true,
        )
        .with_extension_type(VariantType)
    }

    fn field_with_id(name: &str, metadata_key: &str, id: i64) -> Arc<Field> {
        Arc::new(
            Field::new(name, DataType::Int64, true)
                .with_metadata(HashMap::from([(metadata_key.to_string(), id.to_string())])),
        )
    }

    #[test]
    fn cast_struct_fields_by_column_id() -> Result<()> {
        let source_fields = vec![
            field_with_id("col-a", PARQUET_FIELD_ID_META_KEY, 1),
            field_with_id("col-b", PARQUET_FIELD_ID_META_KEY, 2),
        ];
        let source = Arc::new(StructArray::new(
            source_fields.into(),
            vec![
                Arc::new(Int64Array::from(vec![Some(10)])),
                Arc::new(Int64Array::from(vec![Some(20)])),
            ],
            None,
        )) as ArrayRef;
        let target_fields = vec![
            field_with_id("logical_b", "delta.columnMapping.id", 2),
            field_with_id("logical_a", "delta.columnMapping.id", 1),
        ];
        let target_field = Field::new("root", DataType::Struct(target_fields.clone().into()), true);

        assert!(can_cast_field_with_schema_evolution(
            &Field::new("root", source.data_type().clone(), true),
            &target_field,
        )?);
        let casted =
            cast_array_with_schema_evolution(&source, &target_field, &DEFAULT_CAST_OPTIONS)?;
        let casted = casted
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct array");

        assert!(casted.fields().iter().eq(target_fields.iter()));
        assert_eq!(
            casted
                .column_by_name("logical_b")
                .expect("logical_b")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64")
                .value(0),
            20,
        );
        assert_eq!(
            casted
                .column_by_name("logical_a")
                .expect("logical_a")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64")
                .value(0),
            10,
        );
        Ok(())
    }

    #[test]
    fn cast_struct_field_by_physical_name_before_logical_name() -> Result<()> {
        let source = Arc::new(StructArray::new(
            vec![
                Arc::new(Field::new("rate", DataType::Int64, true)),
                Arc::new(Field::new("col-rate", DataType::Int64, true)),
            ]
            .into(),
            vec![
                Arc::new(Int64Array::from(vec![Some(10)])),
                Arc::new(Int64Array::from(vec![Some(20)])),
            ],
            None,
        )) as ArrayRef;
        let target_child = Arc::new(Field::new("rate", DataType::Int64, true).with_metadata(
            HashMap::from([(
                DELTA_COLUMN_MAPPING_PHYSICAL_NAME_METADATA_KEY.to_string(),
                "col-rate".to_string(),
            )]),
        ));
        let target_field = Field::new("root", DataType::Struct(vec![target_child].into()), true);

        let casted =
            cast_array_with_schema_evolution(&source, &target_field, &DEFAULT_CAST_OPTIONS)?;
        let casted = casted
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct array");

        assert_eq!(
            casted
                .column_by_name("rate")
                .expect("rate")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64")
                .value(0),
            20,
        );
        Ok(())
    }

    #[test]
    fn cast_array_unshreds_variant_without_value_field() -> Result<()> {
        let metadata = BinaryViewArray::from_iter_values(vec![&[0x01, 0x00, 0x00][..]]);
        let typed_value = Int64Array::from(vec![Some(42)]);
        let source = Arc::new(StructArray::new(
            vec![
                Arc::new(Field::new("metadata", DataType::BinaryView, false)),
                Arc::new(Field::new("typed_value", DataType::Int64, true)),
            ]
            .into(),
            vec![Arc::new(metadata), Arc::new(typed_value)],
            None,
        )) as ArrayRef;
        let source_field = Field::new("payload", source.data_type().clone(), true);
        let target_field = variant_field("payload");

        assert!(can_cast_field_with_schema_evolution(
            &source_field,
            &target_field
        )?);
        let casted =
            cast_array_with_schema_evolution(&source, &target_field, &DEFAULT_CAST_OPTIONS)?;

        assert_eq!(casted.data_type(), target_field.data_type());
        let json = variant_to_json(&casted)?;
        assert_eq!(json.value(0), "42");
        Ok(())
    }

    #[test]
    fn cast_array_unshreds_nested_variant() -> Result<()> {
        let json =
            Arc::new(StringArray::from(vec![Some(r#"{"id":1,"name":"alice"}"#)])) as ArrayRef;
        let variant = json_to_variant(&json)?;
        let shredding_type = DataType::Struct(
            vec![
                Field::new("id", DataType::Int64, true),
                Field::new("name", DataType::Utf8, true),
            ]
            .into(),
        );
        let shredded = shred_variant(&variant, &shredding_type)?;
        let source_payload = Arc::new(shredded.into_inner()) as ArrayRef;
        let source = Arc::new(StructArray::new(
            vec![Arc::new(Field::new(
                "payload",
                source_payload.data_type().clone(),
                true,
            ))]
            .into(),
            vec![source_payload],
            None,
        )) as ArrayRef;
        let target_payload = Arc::new(variant_field("payload"));
        let target_field = Field::new("root", DataType::Struct(vec![target_payload].into()), true);

        let casted =
            cast_array_with_schema_evolution(&source, &target_field, &DEFAULT_CAST_OPTIONS)?;
        let casted_struct = casted
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct array");
        let payload = casted_struct
            .column_by_name("payload")
            .expect("payload column");
        let json = variant_to_json(payload)?;
        let value: serde_json::Value = serde_json::from_str(json.value(0)).unwrap();

        assert_eq!(value, serde_json::json!({"id": 1, "name": "alice"}));
        Ok(())
    }

    #[test]
    fn cast_array_unshreds_list_element_variant() -> Result<()> {
        let json = Arc::new(StringArray::from(vec![Some("42")])) as ArrayRef;
        let variant = json_to_variant(&json)?;
        let shredded = shred_variant(&variant, &DataType::Int64)?;
        let source_values = Arc::new(shredded.into_inner()) as ArrayRef;
        let source = Arc::new(ListArray::new(
            Arc::new(Field::new(
                "element",
                source_values.data_type().clone(),
                true,
            )),
            OffsetBuffer::new(vec![0_i32, 1].into()),
            source_values,
            None,
        )) as ArrayRef;
        let target_field = Field::new(
            "items",
            DataType::List(Arc::new(variant_field("element"))),
            true,
        );

        let casted =
            cast_array_with_schema_evolution(&source, &target_field, &DEFAULT_CAST_OPTIONS)?;
        let casted_list = casted
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list array");
        let json = variant_to_json(casted_list.values())?;

        assert_eq!(json.value(0), "42");
        Ok(())
    }
}
