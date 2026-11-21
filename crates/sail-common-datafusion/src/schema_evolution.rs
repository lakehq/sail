// SPDX-License-Identifier: Apache-2.0

use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray,
    new_null_array,
};
use datafusion::arrow::compute::{CastOptions, can_cast_types, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion::parquet::arrow::{PARQUET_FIELD_ID_META_KEY, RowNumber};
use datafusion::physical_expr::expressions::{self, CastExpr, Column, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::format::DEFAULT_CAST_OPTIONS;
use parquet_variant_compute::{VariantArray, unshred_variant};

use crate::array::record_batch::cast_array_recursively;
use crate::parquet_schema::{
    cast_parquet_integer, is_parquet_integer_target, parquet_integer_cast_wraps,
    parquet_temporal_storage_cast, parquet_unscaled_integer, validate_parquet_field,
};
use crate::variant::{is_binary_variant_field, is_variant_arrow_field, is_variant_storage_type};

pub const FIELD_DEFAULT_METADATA_KEY: &str = "sail.schema_evolution.default";
pub const FIELD_ALIASES_METADATA_KEY: &str = "sail.schema_evolution.aliases";

/// Preserve the Arrow type and value of a missing-field default across plan serialization.
pub fn encode_field_default(value: &ScalarValue) -> Result<String> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        value.data_type(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![value.to_array_of_size(1)?])?;
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    serde_json::to_string(&bytes).map_err(|error| DataFusionError::External(Box::new(error)))
}

pub fn field_default_array(field: &Field, rows: usize) -> Result<Option<ArrayRef>> {
    let Some(encoded) = field.metadata().get(FIELD_DEFAULT_METADATA_KEY) else {
        return Ok(None);
    };
    let value = decode_field_default_array(encoded)?;
    let value = cast_array_with_schema_evolution(
        &value,
        field,
        &DEFAULT_CAST_OPTIONS,
        StructFieldMatching::Name,
    )?;
    let indices = datafusion::arrow::array::UInt32Array::from(vec![0; rows]);
    Ok(Some(datafusion::arrow::compute::take(
        value.as_ref(),
        &indices,
        None,
    )?))
}

fn decode_field_default_array(encoded: &str) -> Result<ArrayRef> {
    let bytes: Vec<u8> = serde_json::from_str(encoded)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    let batch = reader
        .next()
        .transpose()?
        .ok_or_else(|| exec_datafusion_err!("Missing encoded field default"))?;
    if batch.num_rows() != 1 || batch.num_columns() != 1 || reader.next().is_some() {
        return exec_err!("Invalid encoded field default");
    }
    Ok(batch.column(0).clone())
}

pub fn decode_field_default(encoded: &str) -> Result<ScalarValue> {
    default_array_to_scalar(decode_field_default_array(encoded)?)
}

pub fn field_default(field: &Field) -> Result<Option<ScalarValue>> {
    let Some(value) = field_default_array(field, 1)? else {
        return Ok(None);
    };
    default_array_to_scalar(value).map(Some)
}

fn default_array_to_scalar(value: ArrayRef) -> Result<ScalarValue> {
    // ScalarValue::try_from_array reconstructs list fields without their metadata.
    let scalar = match value.data_type() {
        DataType::List(_) => ScalarValue::List(Arc::new(
            value
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| exec_datafusion_err!("Expected list default"))?
                .clone(),
        )),
        DataType::LargeList(_) => ScalarValue::LargeList(Arc::new(
            value
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| exec_datafusion_err!("Expected large-list default"))?
                .clone(),
        )),
        DataType::FixedSizeList(_, _) => ScalarValue::FixedSizeList(Arc::new(
            value
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| exec_datafusion_err!("Expected fixed-list default"))?
                .clone(),
        )),
        _ => ScalarValue::try_from_array(&value, 0)?,
    };
    Ok(scalar)
}

/// A typed missing-field default whose nested field metadata survives constant folding.
#[derive(Debug, Clone)]
pub struct SchemaEvolutionDefaultExpr {
    field: Arc<Field>,
    value: ArrayRef,
}

impl SchemaEvolutionDefaultExpr {
    pub fn try_new(field: Arc<Field>) -> Result<Self> {
        let value = field_default_array(&field, 1)?
            .ok_or_else(|| exec_datafusion_err!("Missing default for '{}'", field.name()))?;
        Ok(Self { field, value })
    }
    pub fn field(&self) -> &Arc<Field> {
        &self.field
    }
}

impl PartialEq for SchemaEvolutionDefaultExpr {
    fn eq(&self, other: &Self) -> bool {
        self.field == other.field
    }
}
impl Eq for SchemaEvolutionDefaultExpr {}
impl std::hash::Hash for SchemaEvolutionDefaultExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.field.hash(state);
    }
}
impl std::fmt::Display for SchemaEvolutionDefaultExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SCHEMA_EVOLUTION_DEFAULT({})", self.field.name())
    }
}
impl PhysicalExpr for SchemaEvolutionDefaultExpr {
    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
    fn data_type(&self, _input: &Schema) -> Result<DataType> {
        Ok(self.field.data_type().clone())
    }
    fn nullable(&self, _input: &Schema) -> Result<bool> {
        Ok(self.field.is_nullable())
    }
    fn return_field(&self, _input: &Schema) -> Result<Arc<Field>> {
        Ok(self.field.clone())
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if !children.is_empty() {
            return exec_err!("SchemaEvolutionDefaultExpr has no children");
        }
        Ok(self)
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let indices = datafusion::arrow::array::UInt32Array::from(vec![0; batch.num_rows()]);
        Ok(ColumnarValue::Array(datafusion::arrow::compute::take(
            self.value.as_ref(),
            &indices,
            None,
        )?))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum StructFieldMatching {
    #[default]
    Name,
    CaseInsensitiveName,
    PhysicalName,
    FieldId,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum SchemaEvolutionCastMode {
    #[default]
    Strict,
    RelaxedTimezone,
    Parquet,
}

#[derive(Debug)]
pub struct SchemaEvolutionPhysicalExprAdapterFactory {}

impl PhysicalExprAdapterFactory for SchemaEvolutionPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        create_schema_evolution_adapter(
            logical_file_schema,
            physical_file_schema,
            StructFieldMatching::Name,
            SchemaEvolutionCastMode::Strict,
        )
    }
}

#[derive(Debug)]
pub struct SchemaEvolutionPhysicalExprAdapterFactoryWithMatching {
    matching: StructFieldMatching,
    cast_mode: SchemaEvolutionCastMode,
}

impl SchemaEvolutionPhysicalExprAdapterFactoryWithMatching {
    pub fn with_cast_mode(
        matching: StructFieldMatching,
        cast_mode: SchemaEvolutionCastMode,
    ) -> Self {
        Self {
            matching,
            cast_mode,
        }
    }

    pub fn new(matching: StructFieldMatching) -> Self {
        Self {
            matching,
            cast_mode: SchemaEvolutionCastMode::Strict,
        }
    }

    pub fn new_relaxed_timezone(matching: StructFieldMatching) -> Self {
        Self {
            matching,
            cast_mode: SchemaEvolutionCastMode::RelaxedTimezone,
        }
    }
}

impl PhysicalExprAdapterFactory for SchemaEvolutionPhysicalExprAdapterFactoryWithMatching {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        create_schema_evolution_adapter(
            logical_file_schema,
            physical_file_schema,
            self.matching,
            self.cast_mode,
        )
    }
}

fn create_schema_evolution_adapter(
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
    matching: StructFieldMatching,
    cast_mode: SchemaEvolutionCastMode,
) -> Result<Arc<dyn PhysicalExprAdapter>> {
    let column_mapping = create_column_mapping(
        &logical_file_schema,
        &physical_file_schema,
        matching,
        cast_mode,
    )?;

    Ok(Arc::new(SchemaEvolutionPhysicalExprAdapter {
        logical_file_schema,
        physical_file_schema,
        column_mapping,
        matching,
        cast_mode,
    }))
}

#[derive(Debug, Clone)]
enum ColumnMapping {
    Physical(usize),
    Missing(Option<ScalarValue>),
    Ambiguous(Arc<DataFusionError>),
}

fn create_column_mapping(
    logical_schema: &Schema,
    physical_schema: &Schema,
    matching: StructFieldMatching,
    cast_mode: SchemaEvolutionCastMode,
) -> Result<Vec<ColumnMapping>> {
    let mut column_mapping = Vec::with_capacity(logical_schema.fields().len());

    for logical_field in logical_schema.fields() {
        // Reader-generated positions have no Delta physical name or field ID.
        // Match their typed virtual fields before applying persisted-column rules.
        let physical_field = if logical_field.try_extension_type::<RowNumber>().is_ok() {
            physical_schema
                .fields()
                .iter()
                .enumerate()
                .find(|(_, field)| {
                    field.name() == logical_field.name()
                        && field.try_extension_type::<RowNumber>().is_ok()
                })
        } else {
            match find_matching_struct_field(physical_schema.fields(), logical_field, matching) {
                Ok(field) => field,
                Err(error)
                    if cast_mode == SchemaEvolutionCastMode::Parquet
                        && matching == StructFieldMatching::CaseInsensitiveName =>
                {
                    // Spark resolves case-insensitive Parquet names only for requested fields.
                    column_mapping.push(ColumnMapping::Ambiguous(Arc::new(error)));
                    continue;
                }
                Err(error) => return Err(error),
            }
        };
        match physical_field {
            Some((physical_index, _)) => {
                column_mapping.push(ColumnMapping::Physical(physical_index))
            }
            None => {
                let default_value = if let Some(value) = field_default(logical_field)? {
                    Some(value)
                } else if logical_field.is_nullable() {
                    Some(
                        ScalarValue::try_from(logical_field.data_type())
                            .unwrap_or(ScalarValue::Null),
                    )
                } else if matching == StructFieldMatching::FieldId {
                    None
                } else {
                    Some(
                        ScalarValue::new_zero(logical_field.data_type())
                            .unwrap_or(ScalarValue::Null),
                    )
                };
                column_mapping.push(ColumnMapping::Missing(default_value));
            }
        }
    }

    Ok(column_mapping)
}

#[derive(Debug, Clone)]
struct SchemaEvolutionPhysicalExprAdapter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
    column_mapping: Vec<ColumnMapping>,
    matching: StructFieldMatching,
    cast_mode: SchemaEvolutionCastMode,
}

impl PhysicalExprAdapter for SchemaEvolutionPhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let rewriter = SchemaEvolutionPhysicalExprRewriter {
            logical_file_schema: &self.logical_file_schema,
            physical_file_schema: &self.physical_file_schema,
            column_mapping: &self.column_mapping,
            matching: self.matching,
            cast_mode: self.cast_mode,
        };
        if self.cast_mode == SchemaEvolutionCastMode::Parquet {
            return expr
                .transform_down(|expr| {
                    if let Some(rewritten) = rewriter.rewrite_parquet_struct_access(&expr)? {
                        return Ok(Transformed::new(rewritten, true, TreeNodeRecursion::Jump));
                    }
                    if let Some(column) = expr.downcast_ref::<Column>() {
                        let mut rewritten = rewriter.rewrite_column(Arc::clone(&expr), column)?;
                        rewritten.tnr = TreeNodeRecursion::Jump;
                        return Ok(rewritten);
                    }
                    Ok(Transformed::no(expr))
                })
                .data();
        }
        expr.transform(|expr| rewriter.rewrite_expr(Arc::clone(&expr)))
            .data()
    }
}

struct SchemaEvolutionPhysicalExprRewriter<'a> {
    logical_file_schema: &'a Schema,
    physical_file_schema: &'a Schema,
    column_mapping: &'a [ColumnMapping],
    matching: StructFieldMatching,
    cast_mode: SchemaEvolutionCastMode,
}

impl<'a> SchemaEvolutionPhysicalExprRewriter<'a> {
    /// Resolve a projected struct path directly against the decoder schema. Casting the
    /// parent before accessing its child would also read and validate its unused siblings.
    fn rewrite_parquet_struct_access(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        let Some(function) = ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(expr.as_ref())
        else {
            return Ok(None);
        };
        let mut path = Vec::new();
        let Some(column) = parquet_struct_access(expr, &mut path) else {
            return Ok(None);
        };
        let Ok(index) = self.logical_file_schema.index_of(column.name()) else {
            return Ok(None);
        };
        let Some(ColumnMapping::Physical(physical_index)) = self.column_mapping.get(index) else {
            return Ok(None);
        };
        let mut logical = &self.logical_file_schema.fields()[index];
        let mut physical = &self.physical_file_schema.fields()[*physical_index];
        let mut arguments =
            vec![Arc::new(Column::new(physical.name(), *physical_index)) as Arc<dyn PhysicalExpr>];
        for name in path {
            let (DataType::Struct(fields), DataType::Struct(actual)) =
                (logical.data_type(), physical.data_type())
            else {
                return Ok(None);
            };
            let Some((_, field)) = fields.find(name) else {
                return Ok(None);
            };
            let Some((_, source)) = find_matching_struct_field(actual, field, self.matching)?
            else {
                if field_default(field)?.is_some() {
                    return Ok(None);
                }
                return Ok(Some(Arc::new(Literal::new(ScalarValue::try_from(
                    function.return_type(),
                )?))));
            };
            arguments.push(Arc::new(Literal::new(ScalarValue::Utf8(Some(
                source.name().clone(),
            )))));
            logical = field;
            physical = source;
        }
        validate_parquet_field(logical.name(), logical, physical, self.matching)?;
        let leaf: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::try_new(
            Arc::new(function.fun().clone()),
            arguments,
            self.physical_file_schema,
            Arc::new(function.config_options().clone()),
        )?);
        if logical.data_type() == physical.data_type() {
            return Ok(Some(leaf));
        }
        Ok(Some(self.apply_type_cast(leaf, logical, physical)?.data))
    }

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

        let logical_field_index = match self.logical_file_schema.index_of(column.name()) {
            Ok(index) => index,
            Err(_) => return Ok(None),
        };
        let physical_field_index = match self.column_mapping.get(logical_field_index) {
            Some(ColumnMapping::Physical(index)) => *index,
            _ => return Ok(None),
        };
        let physical_field = self.physical_file_schema.field(physical_field_index);
        let physical_struct_fields = match physical_field.data_type() {
            DataType::Struct(fields) => fields,
            _ => return Ok(None),
        };
        let logical_field = self.logical_file_schema.field(logical_field_index);
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
        if find_matching_struct_field(physical_struct_fields, logical_struct_field, self.matching)?
            .is_some()
        {
            return Ok(None);
        }

        // The structural cast also preserves null parents when a child has a default.
        if field_default(logical_struct_field)?.is_some() {
            return Ok(None);
        }
        if !logical_struct_field.is_nullable() && self.matching == StructFieldMatching::FieldId {
            return exec_err!(
                "Required field '{}' is missing and has no default",
                logical_struct_field.name()
            );
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

        match &self.column_mapping[logical_field_index] {
            ColumnMapping::Physical(physical_index) => {
                let physical_field = self.physical_file_schema.field(*physical_index);
                self.handle_existing_column(
                    expr,
                    column,
                    logical_field,
                    physical_field,
                    *physical_index,
                )
            }
            ColumnMapping::Missing(default_value) => {
                if let Some(default_value) = default_value {
                    if logical_field.data_type().is_nested()
                        && logical_field
                            .metadata()
                            .contains_key(FIELD_DEFAULT_METADATA_KEY)
                    {
                        return Ok(Transformed::yes(Arc::new(
                            SchemaEvolutionDefaultExpr::try_new(Arc::new(logical_field.clone()))?,
                        )));
                    }
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
            ColumnMapping::Ambiguous(error) => Err(DataFusionError::Shared(Arc::clone(error))),
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
        let needs_column_update =
            column.index() != physical_index || column.name() != physical_field.name();
        let needs_type_cast = logical_field.data_type() != physical_field.data_type();

        match (needs_column_update, needs_type_cast) {
            (false, false) => Ok(Transformed::no(expr)),
            (true, false) => {
                let new_column =
                    Column::new_with_schema(physical_field.name(), self.physical_file_schema)?;
                Ok(Transformed::yes(Arc::new(new_column)))
            }
            (false, true) => self.apply_type_cast(expr, logical_field, physical_field),
            (true, true) => {
                let new_column =
                    Column::new_with_schema(physical_field.name(), self.physical_file_schema)?;
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
        if self.cast_mode == SchemaEvolutionCastMode::Parquet
            && matches!(logical_field.data_type(), DataType::Struct(_))
        {
            validate_parquet_field(
                logical_field.name(),
                logical_field,
                &Arc::new(physical_field.clone()),
                self.matching,
            )?;
        }
        if !can_cast_field_with_schema_evolution(
            physical_field,
            logical_field,
            self.matching,
            self.cast_mode,
        )? {
            return exec_err!(
                "Cannot cast column '{}' from '{}' (physical) to '{}' (logical)",
                logical_field.name(),
                physical_field.data_type(),
                logical_field.data_type()
            );
        }

        // TODO: Expose a pruning target for PhysicalName and FieldId casts so Delta and Iceberg
        // can prune nested Parquet leaves without weakening their field-identity semantics.
        if matches!(
            self.matching,
            StructFieldMatching::Name | StructFieldMatching::CaseInsensitiveName
        ) && is_pure_name_structural_narrowing(physical_field, logical_field)
        {
            return Ok(Transformed::yes(Arc::new(CastExpr::new_with_target_field(
                column_expr,
                Arc::new(logical_field.clone()),
                None,
            ))));
        }

        let input_field = Arc::new(physical_field.clone());
        let target_field = Arc::new(logical_field.clone());
        let cast = SchemaEvolutionCastColumnExpr::from_parts(
            column_expr,
            input_field,
            target_field,
            None,
            self.matching,
            self.cast_mode,
        );
        Ok(Transformed::yes(Arc::new(cast)))
    }
}

/// Collect paths from both variadic get_field and chained get_field expressions.
fn parquet_struct_access<'a>(
    expr: &'a Arc<dyn PhysicalExpr>,
    path: &mut Vec<&'a str>,
) -> Option<&'a Column> {
    if let Some(column) = expr.downcast_ref::<Column>() {
        return Some(column);
    }
    let function = ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(expr.as_ref())?;
    let (root, fields) = function.args().split_first()?;
    let column = parquet_struct_access(root, path)?;
    for field in fields {
        path.push(
            field
                .downcast_ref::<Literal>()?
                .value()
                .try_as_str()
                .flatten()?,
        );
    }
    Some(column)
}

/// Returns whether DataFusion's name-based nested cast is equivalent to Sail's cast.
/// Keeping the native cast for these shapes lets Parquet derive a leaf-level projection.
fn is_pure_name_structural_narrowing(source: &Field, target: &Field) -> bool {
    if is_variant_arrow_field(source)
        || is_variant_arrow_field(target)
        || is_variant_storage_type(source.data_type())
        || is_variant_storage_type(target.data_type())
    {
        return false;
    }

    match (source.data_type(), target.data_type()) {
        (DataType::Struct(source_fields), DataType::Struct(target_fields)) => {
            target_fields.iter().all(|target_field| {
                source_fields
                    .iter()
                    .find(|source_field| source_field.name() == target_field.name())
                    .is_some_and(|source_field| {
                        is_pure_name_structural_narrowing(source_field, target_field)
                    })
            })
        }
        (DataType::List(source_item), DataType::List(target_item))
        | (DataType::LargeList(source_item), DataType::LargeList(target_item)) => {
            is_pure_name_structural_narrowing(source_item, target_item)
        }
        _ => {
            source.data_type() == target.data_type()
                && !source.data_type().is_nested()
                && !target.data_type().is_nested()
        }
    }
}

fn can_cast_field_with_schema_evolution(
    source: &Field,
    target: &Field,
    matching: StructFieldMatching,
    cast_mode: SchemaEvolutionCastMode,
) -> Result<bool> {
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

    if cast_mode == SchemaEvolutionCastMode::Parquet
        && (parquet_temporal_storage_cast(source.data_type(), target.data_type()).is_some()
            || (source.data_type().is_decimal() && is_parquet_integer_target(target.data_type())))
    {
        return Ok(true);
    }

    match (source.data_type(), target.data_type()) {
        (DataType::Struct(from_fields), DataType::Struct(to_fields)) => {
            validate_struct_compatibility_with_variant(
                from_fields,
                to_fields,
                matching,
                cast_mode,
            )?;
            Ok(true)
        }
        (
            DataType::List(from_elem)
            | DataType::LargeList(from_elem)
            | DataType::FixedSizeList(from_elem, _),
            DataType::List(to_elem) | DataType::LargeList(to_elem),
        ) => can_cast_field_with_schema_evolution(from_elem, to_elem, matching, cast_mode),
        (
            DataType::FixedSizeList(from_elem, from_len),
            DataType::FixedSizeList(to_elem, to_len),
        ) => {
            if from_len != to_len {
                return Ok(false);
            }
            can_cast_field_with_schema_evolution(from_elem, to_elem, matching, cast_mode)
        }
        (DataType::Map(from_entries, _), DataType::Map(to_entries, _)) => {
            validate_map_entries_compatibility(from_entries, to_entries, matching, cast_mode)?;
            Ok(true)
        }
        _ => Ok(can_cast_types(source.data_type(), target.data_type())),
    }
}

const DELTA_COLUMN_MAPPING_PHYSICAL_NAME_METADATA_KEY: &str = "delta.columnMapping.physicalName";
// Delta kernel schemas, Arrow Parquet readers, and Delta's physical schema builder use
// different field-ID metadata spellings, all of which represent the same identity.
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

pub fn find_matching_struct_field<'a>(
    source_fields: &'a [Arc<Field>],
    target_field: &Field,
    matching: StructFieldMatching,
) -> Result<Option<(usize, &'a Arc<Field>)>> {
    Ok(match matching {
        StructFieldMatching::Name => source_fields
            .iter()
            .enumerate()
            .find(|(_, source)| source.name() == target_field.name()),
        StructFieldMatching::CaseInsensitiveName => {
            let name = target_field.name().to_lowercase();
            let mut fields = source_fields
                .iter()
                .enumerate()
                .filter(|(_, source)| source.name().to_lowercase() == name);
            let field = fields.next();
            if fields.next().is_some() {
                return exec_err!("Multiple physical fields match '{}'", target_field.name());
            }
            field
        }
        StructFieldMatching::PhysicalName => {
            // Synthetic list/map wrappers are matched by their container handlers; only
            // actual Delta struct fields with physicalName metadata reach this branch.
            let physical_name = target_field
                .metadata()
                .get(DELTA_COLUMN_MAPPING_PHYSICAL_NAME_METADATA_KEY);
            let Some(physical_name) = physical_name else {
                return Ok(None);
            };
            source_fields
                .iter()
                .enumerate()
                .find(|(_, source)| source.name() == physical_name)
        }
        StructFieldMatching::FieldId => {
            if let Some(target_id) = struct_field_id(target_field) {
                let mut identified = source_fields
                    .iter()
                    .enumerate()
                    .filter(|(_, source)| struct_field_id(source) == Some(target_id));
                if let Some(found) = identified.next() {
                    if identified.next().is_some() {
                        return exec_err!("Multiple physical fields match field ID {target_id}");
                    }
                    return Ok(Some(found));
                }
            }
            let aliases: Vec<String> = target_field
                .metadata()
                .get(FIELD_ALIASES_METADATA_KEY)
                .map(|value| serde_json::from_str(value))
                .transpose()
                .map_err(|error| DataFusionError::External(Box::new(error)))?
                .unwrap_or_default();
            let mut matches = source_fields.iter().enumerate().filter(|(_, source)| {
                match struct_field_id(source) {
                    Some(_) => false,
                    None => aliases.contains(source.name()),
                }
            });
            let found = matches.next();
            if matches.next().is_some() {
                return exec_err!(
                    "Multiple physical fields match aliases of {}",
                    target_field.name()
                );
            }
            found
        }
    })
}

fn validate_struct_compatibility_with_variant(
    source_fields: &[Arc<Field>],
    target_fields: &[Arc<Field>],
    matching: StructFieldMatching,
    cast_mode: SchemaEvolutionCastMode,
) -> Result<()> {
    if cast_mode != SchemaEvolutionCastMode::Parquet
        && matching == StructFieldMatching::Name
        && !target_fields.iter().any(|target| {
            find_matching_struct_field(source_fields, target, matching)
                .is_ok_and(|field| field.is_some())
        })
    {
        return exec_err!(
            "Cannot cast struct with {} fields to {} fields because there is no field name overlap",
            source_fields.len(),
            target_fields.len()
        );
    }

    for target_field in target_fields {
        match find_matching_struct_field(source_fields, target_field, matching)? {
            Some((_, source_field)) => {
                if !can_cast_field_with_schema_evolution(
                    source_field,
                    target_field,
                    matching,
                    cast_mode,
                )? {
                    return exec_err!(
                        "Cannot cast struct field '{}' from type {} to type {}",
                        target_field.name(),
                        source_field.data_type(),
                        target_field.data_type()
                    );
                }
            }
            None if target_field.is_nullable() => {}
            None if field_default(target_field)?.is_some_and(|value| !value.is_null()) => {}
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

fn map_entry_index(
    source_fields: &Fields,
    target_index: usize,
    target_field: &Field,
    cast_mode: SchemaEvolutionCastMode,
) -> Option<usize> {
    if cast_mode == SchemaEvolutionCastMode::Parquet {
        // Legacy Parquet maps identify the key and value by their position.
        (target_index < source_fields.len()).then_some(target_index)
    } else {
        source_fields.find(target_field.name()).map(|(i, _)| i)
    }
}

fn validate_map_entries_compatibility(
    source_entries: &Field,
    target_entries: &Field,
    matching: StructFieldMatching,
    cast_mode: SchemaEvolutionCastMode,
) -> Result<()> {
    let (DataType::Struct(source_fields), DataType::Struct(target_fields)) =
        (source_entries.data_type(), target_entries.data_type())
    else {
        return exec_err!("map entries must use struct types");
    };

    for (index, target_field) in target_fields.iter().enumerate() {
        match map_entry_index(source_fields, index, target_field, cast_mode)
            .map(|i| &source_fields[i])
        {
            Some(source_field)
                if can_cast_field_with_schema_evolution(
                    source_field,
                    target_field,
                    matching,
                    cast_mode,
                )? => {}
            Some(source_field) => {
                return exec_err!(
                    "Cannot cast map entry '{}' from type {} to type {}",
                    target_field.name(),
                    source_field.data_type(),
                    target_field.data_type()
                );
            }
            None if target_field.is_nullable() => {}
            None => {
                return exec_err!(
                    "Cannot cast map: target entry '{}' is non-nullable but missing from source. \
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
    matching: StructFieldMatching,
    cast_mode: SchemaEvolutionCastMode,
}

impl PartialEq for SchemaEvolutionCastColumnExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
            && self.input_field.eq(&other.input_field)
            && self.target_field.eq(&other.target_field)
            && self.cast_options.eq(&other.cast_options)
            && self.matching.eq(&other.matching)
            && self.cast_mode.eq(&other.cast_mode)
    }
}

impl std::hash::Hash for SchemaEvolutionCastColumnExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.input_field.hash(state);
        self.target_field.hash(state);
        self.cast_options.hash(state);
        self.matching.hash(state);
        self.cast_mode.hash(state);
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
        Self::new_with_matching(
            expr,
            input_field,
            target_field,
            cast_options,
            StructFieldMatching::Name,
        )
    }

    pub fn new_with_matching(
        expr: Arc<dyn PhysicalExpr>,
        input_field: Arc<Field>,
        target_field: Arc<Field>,
        cast_options: Option<CastOptions<'static>>,
        matching: StructFieldMatching,
    ) -> Self {
        Self::from_parts(
            expr,
            input_field,
            target_field,
            cast_options,
            matching,
            SchemaEvolutionCastMode::Strict,
        )
    }

    pub fn new_relaxed_timezone(
        expr: Arc<dyn PhysicalExpr>,
        input_field: Arc<Field>,
        target_field: Arc<Field>,
        cast_options: Option<CastOptions<'static>>,
        matching: StructFieldMatching,
    ) -> Self {
        Self::from_parts(
            expr,
            input_field,
            target_field,
            cast_options,
            matching,
            SchemaEvolutionCastMode::RelaxedTimezone,
        )
    }

    pub fn from_parts(
        expr: Arc<dyn PhysicalExpr>,
        input_field: Arc<Field>,
        target_field: Arc<Field>,
        cast_options: Option<CastOptions<'static>>,
        matching: StructFieldMatching,
        cast_mode: SchemaEvolutionCastMode,
    ) -> Self {
        Self {
            expr,
            input_field,
            target_field,
            cast_options: cast_options.unwrap_or(DEFAULT_CAST_OPTIONS),
            matching,
            cast_mode,
        }
    }

    pub fn input_field(&self) -> &Arc<Field> {
        &self.input_field
    }

    pub fn target_field(&self) -> &Arc<Field> {
        &self.target_field
    }

    pub const fn matching(&self) -> StructFieldMatching {
        self.matching
    }

    pub const fn cast_mode(&self) -> SchemaEvolutionCastMode {
        self.cast_mode
    }

    fn cast_array(&self, array: &ArrayRef) -> Result<ArrayRef> {
        cast_array_with_schema_evolution_inner(
            array,
            self.target_field.as_ref(),
            &self.cast_options,
            self.matching,
            self.cast_mode,
        )
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
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(self.cast_array(&array)?)),
            ColumnarValue::Scalar(scalar)
                if scalar.data_type() == *self.target_field.data_type() =>
            {
                Ok(ColumnarValue::Scalar(scalar))
            }
            ColumnarValue::Scalar(scalar) => {
                let as_array = scalar.to_array_of_size(1)?;
                let casted = self.cast_array(&as_array)?;
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
        Ok(Arc::new(Self::from_parts(
            child,
            Arc::clone(&self.input_field),
            Arc::clone(&self.target_field),
            Some(self.cast_options.clone()),
            self.matching,
            self.cast_mode,
        )))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

pub fn cast_array_with_schema_evolution(
    source: &ArrayRef,
    target_field: &Field,
    cast_options: &CastOptions,
    matching: StructFieldMatching,
) -> Result<ArrayRef> {
    cast_array_with_schema_evolution_inner(
        source,
        target_field,
        cast_options,
        matching,
        SchemaEvolutionCastMode::Strict,
    )
}

pub fn cast_array_with_schema_evolution_relaxed_tz(
    source: &ArrayRef,
    target_field: &Field,
    cast_options: &CastOptions,
    matching: StructFieldMatching,
) -> Result<ArrayRef> {
    cast_array_with_schema_evolution_inner(
        source,
        target_field,
        cast_options,
        matching,
        SchemaEvolutionCastMode::RelaxedTimezone,
    )
}

fn cast_array_with_schema_evolution_inner(
    source: &ArrayRef,
    target_field: &Field,
    cast_options: &CastOptions,
    matching: StructFieldMatching,
    cast_mode: SchemaEvolutionCastMode,
) -> Result<ArrayRef> {
    if is_variant_arrow_field(target_field) && is_variant_storage_type(source.data_type()) {
        return cast_variant_array_with_schema_evolution(source, target_field, cast_options);
    }

    if source.data_type() == target_field.data_type() {
        return Ok(Arc::clone(source));
    }

    if cast_mode == SchemaEvolutionCastMode::Parquet
        && source.data_type().is_decimal()
        && is_parquet_integer_target(target_field.data_type())
    {
        let integer = parquet_unscaled_integer(source, target_field.data_type())?;
        return cast_array_with_schema_evolution_inner(
            &integer,
            target_field,
            cast_options,
            matching,
            cast_mode,
        );
    }

    let storage;
    let source = if cast_mode == SchemaEvolutionCastMode::Parquet
        && let Some(ty) =
            parquet_temporal_storage_cast(source.data_type(), target_field.data_type())
    {
        storage = if parquet_integer_cast_wraps(source.data_type(), &ty) {
            cast_parquet_integer(source.as_ref(), &ty)?
        } else {
            cast_with_options(source, &ty, cast_options)?
        };
        &storage
    } else {
        source
    };

    if cast_mode == SchemaEvolutionCastMode::Parquet
        && parquet_integer_cast_wraps(source.data_type(), target_field.data_type())
    {
        return cast_parquet_integer(source.as_ref(), target_field.data_type());
    }

    if matches!(
        cast_mode,
        SchemaEvolutionCastMode::RelaxedTimezone | SchemaEvolutionCastMode::Parquet
    ) && let (DataType::Timestamp(source_unit, _), DataType::Timestamp(target_unit, _)) =
        (source.data_type(), target_field.data_type())
    {
        // Parquet timestamp readers preserve the stored epoch ticks across timezone families.
        let source_without_timezone = DataType::Timestamp(*source_unit, None);
        let source = cast_array_recursively(source, &source_without_timezone)?;
        let target_without_timezone = DataType::Timestamp(*target_unit, None);
        let scaled = cast_with_options(&source, &target_without_timezone, cast_options)?;
        return cast_array_recursively(&scaled, target_field.data_type());
    }

    // Convert only offsets here so nested values retain schema-evolution field matching.
    let offset_cast = match (source.data_type(), target_field.data_type()) {
        (DataType::LargeList(element) | DataType::FixedSizeList(element, _), DataType::List(_)) => {
            Some(cast_with_options(
                source,
                &DataType::List(Arc::clone(element)),
                cast_options,
            )?)
        }
        (DataType::List(element) | DataType::FixedSizeList(element, _), DataType::LargeList(_)) => {
            Some(cast_with_options(
                source,
                &DataType::LargeList(Arc::clone(element)),
                cast_options,
            )?)
        }
        _ => None,
    };
    let source = offset_cast.as_ref().unwrap_or(source);

    match target_field.data_type() {
        DataType::Struct(target_fields) => cast_struct_array_with_schema_evolution(
            source,
            target_fields,
            cast_options,
            matching,
            cast_mode,
        ),
        DataType::List(target_elem) => {
            let Some(source_list) = source.as_any().downcast_ref::<ListArray>() else {
                return exec_err!(
                    "Cannot cast column of type {} to list type. Source must be a list to cast to list.",
                    source.data_type()
                );
            };
            let casted_values = cast_array_with_schema_evolution_inner(
                source_list.values(),
                target_elem.as_ref(),
                cast_options,
                matching,
                cast_mode,
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
            let casted_values = cast_array_with_schema_evolution_inner(
                source_list.values(),
                target_elem.as_ref(),
                cast_options,
                matching,
                cast_mode,
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
            let casted_values = cast_array_with_schema_evolution_inner(
                source_list.values(),
                target_elem.as_ref(),
                cast_options,
                matching,
                cast_mode,
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

            for (index, target_child) in target_kv_fields.iter().enumerate() {
                kv_fields.push(Arc::clone(target_child));
                match map_entry_index(
                    source_map.entries().fields(),
                    index,
                    target_child,
                    cast_mode,
                ) {
                    Some(i) => kv_arrays.push(cast_array_with_schema_evolution_inner(
                        source_map.entries().column(i),
                        target_child.as_ref(),
                        cast_options,
                        matching,
                        cast_mode,
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
    cast_struct_array_to_fields(
        &unshredded,
        target_fields,
        cast_options,
        StructFieldMatching::Name,
        SchemaEvolutionCastMode::Strict,
    )
}

fn cast_struct_array_with_schema_evolution(
    source: &ArrayRef,
    target_fields: &[Arc<Field>],
    cast_options: &CastOptions,
    matching: StructFieldMatching,
    cast_mode: SchemaEvolutionCastMode,
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
    validate_struct_compatibility_with_variant(
        source_struct.fields(),
        target_fields,
        matching,
        cast_mode,
    )?;
    cast_struct_array_to_fields(source, target_fields, cast_options, matching, cast_mode)
}

fn cast_struct_array_to_fields(
    source: &ArrayRef,
    target_fields: &[Arc<Field>],
    cast_options: &CastOptions,
    matching: StructFieldMatching,
    cast_mode: SchemaEvolutionCastMode,
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
        match find_matching_struct_field(source_struct.fields(), target_child, matching)? {
            Some((source_index, _)) => {
                arrays.push(cast_array_with_schema_evolution_inner(
                    source_struct.column(source_index),
                    target_child.as_ref(),
                    cast_options,
                    matching,
                    cast_mode,
                )?);
            }
            None => arrays.push(match field_default_array(target_child, num_rows)? {
                Some(value) => value,
                None => new_null_array(target_child.data_type(), num_rows),
            }),
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

    use datafusion::arrow::array::{
        BinaryViewArray, Int64Array, StringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray,
    };
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
    use datafusion::arrow::datatypes::TimeUnit;
    use parquet_variant_compute::{VariantType, json_to_variant, shred_variant, variant_to_json};

    use super::*;

    #[test]
    fn explicit_aliases_match_unidentified_fields_only() -> Result<()> {
        let field = Field::new("reserved", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                FIELD_ALIASES_METADATA_KEY.to_string(),
                "[\"reserved\"]".to_string(),
            )]),
        );
        let plain = vec![Arc::new(Field::new("reserved", DataType::Utf8, true))];
        assert!(
            find_matching_struct_field(&plain, &field, StructFieldMatching::FieldId)?.is_some()
        );
        let reused = vec![field_with_id("reserved", PARQUET_FIELD_ID_META_KEY, 1)];
        assert!(
            find_matching_struct_field(&reused, &field, StructFieldMatching::FieldId)?.is_none()
        );
        let unaliased = Field::new("reserved", DataType::Utf8, true);
        assert!(
            find_matching_struct_field(&plain, &unaliased, StructFieldMatching::FieldId)?.is_none()
        );
        assert!(
            find_matching_struct_field(
                &[plain[0].clone(), plain[0].clone()],
                &field,
                StructFieldMatching::FieldId
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn missing_defaults_and_aliases_preserve_field_identity() -> Result<()> {
        let mut metadata = std::collections::HashMap::from([
            (PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string()),
            (
                FIELD_ALIASES_METADATA_KEY.to_string(),
                "[\"legacy\"]".to_string(),
            ),
            (
                FIELD_DEFAULT_METADATA_KEY.to_string(),
                encode_field_default(&ScalarValue::Int64(Some(42)))?,
            ),
        ]);
        let logical = Arc::new(Schema::new(vec![
            Field::new("current", DataType::Int64, false).with_metadata(metadata.clone()),
        ]));
        let legacy = Arc::new(Schema::new(vec![Field::new(
            "legacy",
            DataType::Int64,
            true,
        )]));
        let adapter = create_schema_evolution_adapter(
            logical.clone(),
            legacy,
            StructFieldMatching::FieldId,
            SchemaEvolutionCastMode::Strict,
        )?;
        assert!(
            adapter
                .rewrite(Arc::new(Column::new("current", 0)))?
                .downcast_ref::<Column>()
                .is_some()
        );
        let identified = Arc::new(Schema::new(vec![
            Arc::new(Field::new("legacy", DataType::Int64, true)),
            field_with_id("renamed", PARQUET_FIELD_ID_META_KEY, 1),
        ]));
        let adapter = create_schema_evolution_adapter(
            logical.clone(),
            identified,
            StructFieldMatching::FieldId,
            SchemaEvolutionCastMode::Strict,
        )?;
        assert_eq!(
            adapter
                .rewrite(Arc::new(Column::new("current", 0)))?
                .downcast_ref::<Column>()
                .unwrap()
                .index(),
            1
        );
        let reused = Arc::new(Schema::new(vec![field_with_id(
            "legacy",
            PARQUET_FIELD_ID_META_KEY,
            2,
        )]));
        let adapter = create_schema_evolution_adapter(
            logical,
            reused,
            StructFieldMatching::FieldId,
            SchemaEvolutionCastMode::Strict,
        )?;
        assert_eq!(
            adapter
                .rewrite(Arc::new(Column::new("current", 0)))?
                .downcast_ref::<Literal>()
                .unwrap()
                .value(),
            &ScalarValue::Int64(Some(42))
        );
        metadata.remove(FIELD_DEFAULT_METADATA_KEY);
        let required = Arc::new(Schema::new(vec![
            Field::new("current", DataType::Int64, false).with_metadata(metadata),
        ]));
        let adapter = create_schema_evolution_adapter(
            required,
            Arc::new(Schema::empty()),
            StructFieldMatching::FieldId,
            SchemaEvolutionCastMode::Strict,
        )?;
        assert!(
            adapter
                .rewrite(Arc::new(Column::new("current", 0)))
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn list_default_projection_preserves_element_metadata() -> Result<()> {
        let element = field_with_id("item", PARQUET_FIELD_ID_META_KEY, 2);
        let values = Arc::new(Int64Array::from(vec![7, 9])) as ArrayRef;
        let array = Arc::new(ListArray::new(
            element.clone(),
            OffsetBuffer::new(vec![0i32, 2].into()),
            values,
            None,
        ));
        let default = ScalarValue::List(array);
        let target = Field::new("values", DataType::List(element), true).with_metadata(
            std::collections::HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string()),
                (
                    FIELD_DEFAULT_METADATA_KEY.to_string(),
                    encode_field_default(&default)?,
                ),
            ]),
        );
        let logical = Arc::new(Schema::new(vec![target.clone()]));
        let physical = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let adapter = create_schema_evolution_adapter(
            logical,
            physical.clone(),
            StructFieldMatching::FieldId,
            SchemaEvolutionCastMode::Strict,
        )?;
        let batch = RecordBatch::try_new(physical, vec![Arc::new(Int64Array::from(vec![1, 2]))])?;
        let expression = adapter.rewrite(Arc::new(Column::new("values", 0)))?;
        let expression = datafusion::physical_expr::simplifier::PhysicalExprSimplifier::new(
            batch.schema().as_ref(),
        )
        .simplify(expression)?;
        let values = expression.evaluate(&batch)?.into_array(2)?;
        assert_eq!(values.data_type(), target.data_type());
        assert_eq!(
            values
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[7, 9]
        );
        Ok(())
    }

    #[test]
    fn virtual_row_numbers_survive_column_mapping() -> Result<()> {
        let row_number =
            Arc::new(Field::new("position", DataType::Int64, false).with_extension_type(RowNumber));
        let logical = Arc::new(Schema::new(vec![Arc::clone(&row_number)]));
        let physical = Arc::new(Schema::new(vec![
            Arc::new(Field::new("data", DataType::Int64, true)),
            row_number,
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![10, 100])),
            ],
        )?;
        for matching in [
            StructFieldMatching::PhysicalName,
            StructFieldMatching::FieldId,
        ] {
            let adapter = create_schema_evolution_adapter(
                Arc::clone(&logical),
                Arc::clone(&physical),
                matching,
                SchemaEvolutionCastMode::Strict,
            )?;
            let expr = adapter.rewrite(Arc::new(Column::new("position", 0)))?;
            assert!(
                expr.downcast_ref::<Column>()
                    .is_some_and(|column| column.index() == 1)
            );
            assert_eq!(
                expr.evaluate(&batch)?.into_array(2)?.as_ref(),
                batch.column(1).as_ref()
            );
        }
        Ok(())
    }

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
    fn maps_top_level_columns_by_field_id() -> Result<()> {
        let logical_schema = Arc::new(Schema::new(vec![
            field_with_id("renamed_b", PARQUET_FIELD_ID_META_KEY, 2),
            field_with_id("renamed_a", PARQUET_FIELD_ID_META_KEY, 1),
        ]));
        let physical_schema = Arc::new(Schema::new(vec![
            field_with_id("old_a", PARQUET_FIELD_ID_META_KEY, 1),
            field_with_id("old_b", PARQUET_FIELD_ID_META_KEY, 2),
        ]));
        let adapter = create_schema_evolution_adapter(
            logical_schema,
            Arc::clone(&physical_schema),
            StructFieldMatching::FieldId,
            SchemaEvolutionCastMode::Strict,
        )?;

        let rewritten = adapter.rewrite(Arc::new(Column::new("renamed_b", 0)))?;
        let column = rewritten
            .downcast_ref::<Column>()
            .expect("rewritten column");
        assert_eq!(column.name(), "old_b");
        assert_eq!(column.index(), 1);

        let renamed_in_place = Arc::new(Schema::new(vec![
            field_with_id("renamed_a", PARQUET_FIELD_ID_META_KEY, 1),
            field_with_id("renamed_b", PARQUET_FIELD_ID_META_KEY, 2),
        ]));
        let adapter = create_schema_evolution_adapter(
            renamed_in_place,
            physical_schema,
            StructFieldMatching::FieldId,
            SchemaEvolutionCastMode::Strict,
        )?;
        let rewritten = adapter.rewrite(Arc::new(Column::new("renamed_a", 0)))?;
        let column = rewritten
            .downcast_ref::<Column>()
            .expect("rewritten column");
        assert_eq!(column.name(), "old_a");
        assert_eq!(column.index(), 0);
        Ok(())
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
            StructFieldMatching::FieldId,
            SchemaEvolutionCastMode::Strict,
        )?);
        let casted = cast_array_with_schema_evolution(
            &source,
            &target_field,
            &DEFAULT_CAST_OPTIONS,
            StructFieldMatching::FieldId,
        )?;
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
    fn list_offset_width_cast_preserves_field_ids_and_slices() -> Result<()> {
        let source_fields = vec![
            field_with_id("old_a", PARQUET_FIELD_ID_META_KEY, 1),
            field_with_id("old_b", PARQUET_FIELD_ID_META_KEY, 2),
        ];
        let values: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![0, 10, 20, 30])),
            Arc::new(Int64Array::from(vec![0, 100, 200, 300])),
        ];
        let source_values = Arc::new(StructArray::new(
            source_fields.clone().into(),
            values.clone(),
            None,
        ));
        let source_element = Arc::new(Field::new(
            "element",
            DataType::Struct(source_fields.into()),
            true,
        ));
        let target_fields = vec![
            field_with_id("renamed_b", PARQUET_FIELD_ID_META_KEY, 2),
            field_with_id("renamed_a", PARQUET_FIELD_ID_META_KEY, 1),
        ];
        let target_values = Arc::new(StructArray::new(
            target_fields.clone().into(),
            vec![values[1].clone(), values[0].clone()],
            None,
        ));
        let target_element = Arc::new(Field::new(
            "element",
            DataType::Struct(target_fields.into()),
            true,
        ));
        let offsets = OffsetBuffer::new(vec![0_i32, 1, 3, 3, 3, 4].into());
        let nulls = Some(NullBuffer::from(vec![true, true, true, false, true]));
        let source: ArrayRef = Arc::new(
            ListArray::new(
                source_element.clone(),
                offsets.clone(),
                source_values,
                nulls.clone(),
            )
            .slice(1, 4),
        );
        let expected: ArrayRef = Arc::new(
            ListArray::new(target_element.clone(), offsets, target_values, nulls).slice(1, 4),
        );

        for target_is_large in [false, true] {
            let (source, expected) = if target_is_large {
                (
                    source.clone(),
                    cast_with_options(
                        &expected,
                        &DataType::LargeList(target_element.clone()),
                        &DEFAULT_CAST_OPTIONS,
                    )?,
                )
            } else {
                (
                    cast_with_options(
                        &source,
                        &DataType::LargeList(source_element.clone()),
                        &DEFAULT_CAST_OPTIONS,
                    )?,
                    expected.clone(),
                )
            };
            let target = Field::new("items", expected.data_type().clone(), true);
            assert!(can_cast_field_with_schema_evolution(
                &Field::new("items", source.data_type().clone(), true),
                &target,
                StructFieldMatching::FieldId,
                SchemaEvolutionCastMode::Strict,
            )?);
            let actual = cast_array_with_schema_evolution(
                &source,
                &target,
                &DEFAULT_CAST_OPTIONS,
                StructFieldMatching::FieldId,
            )?;
            assert_eq!(actual.to_data(), expected.to_data());
        }
        Ok(())
    }

    #[test]
    fn list_offset_width_cast_checks_element_nullability() -> Result<()> {
        let optional = Arc::new(Field::new("element", DataType::Int64, true));
        let required = Arc::new(Field::new("element", DataType::Int64, false));
        for (source, target) in [
            (
                DataType::List(optional.clone()),
                DataType::LargeList(required.clone()),
            ),
            (DataType::LargeList(optional), DataType::List(required)),
        ] {
            assert!(!can_cast_field_with_schema_evolution(
                &Field::new("items", source, true),
                &Field::new("items", target, true),
                StructFieldMatching::FieldId,
                SchemaEvolutionCastMode::Strict,
            )?);
        }
        Ok(())
    }

    #[test]
    fn list_offset_width_cast_rejects_overflow() {
        let length = i64::from(i32::MAX) + 1;
        let element = Arc::new(Field::new("element", DataType::Null, true));
        let source: ArrayRef = Arc::new(LargeListArray::new(
            element.clone(),
            OffsetBuffer::new(vec![0, length].into()),
            Arc::new(datafusion::arrow::array::NullArray::new(length as usize)),
            None,
        ));
        let error = cast_array_with_schema_evolution(
            &source,
            &Field::new("items", DataType::List(element), true),
            &DEFAULT_CAST_OPTIONS,
            StructFieldMatching::FieldId,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            DataFusionError::ArrowError(error, _)
                if matches!(*error, datafusion::arrow::error::ArrowError::ComputeError(ref message)
                    if message.starts_with("Offset overflow"))
        ));
    }

    #[test]
    fn generic_default_matches_nested_fields_by_name() -> Result<()> {
        let source_by_name = field_with_id("logical", PARQUET_FIELD_ID_META_KEY, 2);
        let source_by_id = field_with_id("old_name", PARQUET_FIELD_ID_META_KEY, 1);
        let source = Arc::new(StructArray::new(
            vec![source_by_name, source_by_id].into(),
            vec![
                Arc::new(Int64Array::from(vec![Some(20)])),
                Arc::new(Int64Array::from(vec![Some(10)])),
            ],
            None,
        )) as ArrayRef;
        let target_child = field_with_id("logical", "delta.columnMapping.id", 1);
        let target_field = Field::new("root", DataType::Struct(vec![target_child].into()), true);

        let casted = cast_array_with_schema_evolution(
            &source,
            &target_field,
            &DEFAULT_CAST_OPTIONS,
            StructFieldMatching::default(),
        )?;
        let casted = casted
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct array");
        assert_eq!(
            casted
                .column_by_name("logical")
                .expect("logical")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64")
                .value(0),
            20,
        );
        Ok(())
    }

    #[test]
    fn generic_name_mode_rejects_structs_without_field_overlap() {
        let source = Arc::new(StructArray::new(
            vec![Arc::new(Field::new("old", DataType::Int64, true))].into(),
            vec![Arc::new(Int64Array::from(vec![Some(10)]))],
            None,
        )) as ArrayRef;
        let target_field = Field::new(
            "root",
            DataType::Struct(vec![Arc::new(Field::new("new", DataType::Int64, true))].into()),
            true,
        );

        let error = cast_array_with_schema_evolution(
            &source,
            &target_field,
            &DEFAULT_CAST_OPTIONS,
            StructFieldMatching::Name,
        )
        .expect_err("zero-overlap name-based structs must fail");

        assert!(error.to_string().contains("no field name overlap"));
    }

    #[test]
    fn relaxed_timezone_cast_scales_without_shifting_values() -> Result<()> {
        let source = Arc::new(TimestampMillisecondArray::from(vec![Some(1_000)])) as ArrayRef;
        let target = Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("Europe/Amsterdam"))),
            true,
        );

        let casted = cast_array_with_schema_evolution_relaxed_tz(
            &source,
            &target,
            &DEFAULT_CAST_OPTIONS,
            StructFieldMatching::Name,
        )?;
        let timestamp = casted
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp");

        assert_eq!(timestamp.value(0), 1_000_000);
        assert_eq!(timestamp.data_type(), target.data_type());
        Ok(())
    }

    #[test]
    fn identical_schema_evolution_reuses_array() -> Result<()> {
        let source = Arc::new(Int64Array::from(vec![Some(10), None])) as ArrayRef;
        let target = Field::new("value", DataType::Int64, true);

        let casted = cast_array_with_schema_evolution_relaxed_tz(
            &source,
            &target,
            &DEFAULT_CAST_OPTIONS,
            StructFieldMatching::Name,
        )?;

        assert!(Arc::ptr_eq(&source, &casted));
        Ok(())
    }

    #[test]
    fn relaxed_timezone_cast_recurses_into_structs() -> Result<()> {
        let source_child = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
            true,
        ));
        let source = Arc::new(StructArray::new(
            vec![Arc::clone(&source_child)].into(),
            vec![Arc::new(
                TimestampMillisecondArray::from(vec![Some(1_000)]).with_timezone("UTC"),
            )],
            None,
        )) as ArrayRef;
        let target_child = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("Europe/Amsterdam"))),
            true,
        ));
        let target = Field::new(
            "root",
            DataType::Struct(vec![Arc::clone(&target_child)].into()),
            true,
        );

        let casted = cast_array_with_schema_evolution_relaxed_tz(
            &source,
            &target,
            &DEFAULT_CAST_OPTIONS,
            StructFieldMatching::Name,
        )?;
        let casted = casted
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct");
        let timestamp = casted
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp");

        assert_eq!(timestamp.value(0), 1_000_000);
        assert_eq!(timestamp.data_type(), target_child.data_type());
        Ok(())
    }

    #[test]
    fn name_mode_does_not_fallback_to_id_or_logical_name() -> Result<()> {
        let source_child = field_with_id("logical", PARQUET_FIELD_ID_META_KEY, 1);
        let source = Arc::new(StructArray::new(
            vec![source_child].into(),
            vec![Arc::new(Int64Array::from(vec![Some(10)]))],
            None,
        )) as ArrayRef;
        let target_child = Arc::new(Field::new("logical", DataType::Int64, true).with_metadata(
            HashMap::from([
                (
                    DELTA_COLUMN_MAPPING_PHYSICAL_NAME_METADATA_KEY.to_string(),
                    "different-physical-name".to_string(),
                ),
                ("delta.columnMapping.id".to_string(), "1".to_string()),
            ]),
        ));
        let target_field = Field::new("root", DataType::Struct(vec![target_child].into()), true);

        let casted = cast_array_with_schema_evolution(
            &source,
            &target_field,
            &DEFAULT_CAST_OPTIONS,
            StructFieldMatching::PhysicalName,
        )?;
        let casted = casted
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct array");

        assert!(casted.column(0).is_null(0));
        Ok(())
    }

    #[test]
    fn missing_non_nullable_mapped_field_is_rejected() {
        let source = Arc::new(StructArray::new(
            vec![Arc::new(Field::new("other", DataType::Int64, true))].into(),
            vec![Arc::new(Int64Array::from(vec![Some(10)]))],
            None,
        )) as ArrayRef;
        let target_child = Arc::new(
            Field::new("required", DataType::Int64, false).with_metadata(HashMap::from([(
                DELTA_COLUMN_MAPPING_PHYSICAL_NAME_METADATA_KEY.to_string(),
                "required-physical".to_string(),
            )])),
        );
        let target_field = Field::new("root", DataType::Struct(vec![target_child].into()), true);

        let error = cast_array_with_schema_evolution(
            &source,
            &target_field,
            &DEFAULT_CAST_OPTIONS,
            StructFieldMatching::PhysicalName,
        )
        .expect_err("missing non-nullable field must fail");

        assert!(
            error
                .to_string()
                .contains("target field 'required' is non-nullable")
        );
    }

    #[test]
    fn map_entry_wrappers_match_by_name_in_physical_name_mode() -> Result<()> {
        let source_value = Arc::new(Field::new(
            "value",
            DataType::Struct(vec![Arc::new(Field::new("col-a", DataType::Int64, true))].into()),
            true,
        ));
        let source = Field::new(
            "attrs",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", DataType::Utf8, false)),
                            source_value,
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        );

        let target_value_child = Arc::new(Field::new("a", DataType::Int64, true).with_metadata(
            HashMap::from([(
                DELTA_COLUMN_MAPPING_PHYSICAL_NAME_METADATA_KEY.to_string(),
                "col-a".to_string(),
            )]),
        ));
        let target = Field::new(
            "attrs",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", DataType::Utf8, false)),
                            Arc::new(Field::new(
                                "value",
                                DataType::Struct(vec![target_value_child].into()),
                                true,
                            )),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        );

        assert!(can_cast_field_with_schema_evolution(
            &source,
            &target,
            StructFieldMatching::PhysicalName,
            SchemaEvolutionCastMode::Strict,
        )?);
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

        let casted = cast_array_with_schema_evolution(
            &source,
            &target_field,
            &DEFAULT_CAST_OPTIONS,
            StructFieldMatching::PhysicalName,
        )?;
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
            &target_field,
            StructFieldMatching::PhysicalName,
            SchemaEvolutionCastMode::Strict,
        )?);
        let casted = cast_array_with_schema_evolution(
            &source,
            &target_field,
            &DEFAULT_CAST_OPTIONS,
            StructFieldMatching::PhysicalName,
        )?;

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

        let casted = cast_array_with_schema_evolution(
            &source,
            &target_field,
            &DEFAULT_CAST_OPTIONS,
            StructFieldMatching::Name,
        )?;
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

        let casted = cast_array_with_schema_evolution(
            &source,
            &target_field,
            &DEFAULT_CAST_OPTIONS,
            StructFieldMatching::Name,
        )?;
        let casted_list = casted
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list array");
        let json = variant_to_json(casted_list.values())?;

        assert_eq!(json.value(0), "42");
        Ok(())
    }
}
