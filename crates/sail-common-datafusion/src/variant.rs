use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::extension::ExtensionType;
use datafusion::arrow::array::{
    Array, ArrayRef, LargeListArray, ListArray, MapArray, RecordBatch, StructArray,
};
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit,
};
use parquet_variant::Variant;
use parquet_variant_compute::{VariantArray, VariantType, shred_variant, unshred_variant};

const MIN_FIELD_FREQUENCY: f64 = 0.10;
const MAX_SHREDDED_FIELDS: usize = 300;
const MAX_SHREDDING_DEPTH: usize = 50;
const MAX_INTERMEDIATE_FIELDS: usize = 1000;
pub const DEFAULT_VARIANT_INFERENCE_NODE_BUDGET: usize = 100_000;
pub const VARIANT_METADATA_FIELD_NAME: &str = "metadata";
pub const VARIANT_VALUE_FIELD_NAME: &str = "value";
pub const VARIANT_TYPED_VALUE_FIELD_NAME: &str = "typed_value";
pub const VARIANT_METADATA_MARKER_KEY: &str = "variant";
pub const VARIANT_METADATA_MARKER_VALUE: &str = "true";

#[derive(Debug, Clone)]
pub struct VariantShreddingConfig {
    pub enabled: bool,
    pub inference_buffer_size: usize,
    pub inference_node_budget: usize,
}

impl Default for VariantShreddingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            inference_buffer_size: 100,
            inference_node_budget: DEFAULT_VARIANT_INFERENCE_NODE_BUDGET,
        }
    }
}

#[derive(Debug, Clone)]
pub struct VariantShreddingPlan {
    column_types: Vec<Option<DataType>>,
}

impl VariantShreddingPlan {
    pub fn is_noop(&self) -> bool {
        self.column_types.iter().all(Option::is_none)
    }
}

pub fn variant_top_level_columns(schema: &SchemaRef) -> HashSet<String> {
    schema
        .fields()
        .iter()
        .filter(|field| is_variant_arrow_field(field))
        .map(|field| field.name().clone())
        .collect()
}

pub fn build_variant_shredding_plan(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    inference_buffer_size: usize,
    inference_node_budget: usize,
) -> Result<VariantShreddingPlan, String> {
    let mut column_types = vec![None; schema.fields().len()];
    let inference_buffer_size = inference_buffer_size.max(1);
    let inference_node_budget = inference_node_budget.max(1);

    for (index, field) in schema.fields().iter().enumerate() {
        if is_variant_arrow_field(field) {
            column_types[index] = infer_variant_shredding_type(
                index,
                batches,
                inference_buffer_size,
                inference_node_budget,
            )?;
        }
    }

    Ok(VariantShreddingPlan { column_types })
}

pub fn apply_variant_shredding_plan(
    batch: &RecordBatch,
    plan: &VariantShreddingPlan,
) -> Result<RecordBatch, String> {
    if plan.is_noop() {
        return Ok(batch.clone());
    }

    let schema = batch.schema();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    let mut fields = Vec::with_capacity(schema.fields().len());

    for (index, field) in schema.fields().iter().enumerate() {
        if let Some(shredding_type) = plan.column_types[index].as_ref() {
            let variant = VariantArray::try_new(batch.column(index).as_ref())
                .map_err(|e| format!("variant shredding input '{}': {e}", field.name()))?;
            let shredded = shred_variant(&variant, shredding_type)
                .map_err(|e| format!("variant shredding column '{}': {e}", field.name()))?;
            let physical_field = field
                .as_ref()
                .clone()
                .with_data_type(shredded.data_type().clone());
            fields.push(Arc::new(physical_field));
            columns.push(Arc::new(shredded.into_inner()) as ArrayRef);
        } else {
            fields.push(field.clone());
            columns.push(batch.column(index).clone());
        }
    }

    let physical_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    RecordBatch::try_new(physical_schema, columns).map_err(|e| e.to_string())
}

pub fn unshred_shredded_variants_for_write(
    batch: &RecordBatch,
    target_schema: &SchemaRef,
) -> Result<RecordBatch, String> {
    let schema = batch.schema();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    let mut fields: Vec<FieldRef> = Vec::with_capacity(schema.fields().len());
    let mut changed = false;

    for (index, source_field) in schema.fields().iter().enumerate() {
        let target_field = target_schema
            .field_with_name(source_field.name())
            .unwrap_or(source_field.as_ref());
        let (field, column, field_changed) =
            unshred_array_for_write(batch.column(index), source_field, target_field)?;
        fields.push(field);
        columns.push(column);
        changed |= field_changed;
    }

    if !changed {
        return Ok(batch.clone());
    }

    let normalized_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    RecordBatch::try_new(normalized_schema, columns).map_err(|e| e.to_string())
}

pub fn is_variant_arrow_field(field: &Field) -> bool {
    field.extension_type_name() == Some(VariantType::NAME)
}

pub fn with_variant_extension_if_marked_storage(field: Field) -> Field {
    if is_marked_variant_storage_type(field.data_type())
        && field.extension_type_name() != Some(VariantType::NAME)
    {
        field.with_extension_type(VariantType)
    } else {
        field
    }
}

pub fn is_variant_storage_field(field: &Field) -> bool {
    let has_variant_extension = field.extension_type_name() == Some(VariantType::NAME)
        && field.try_extension_type::<VariantType>().is_ok();
    has_variant_extension || is_marked_variant_storage_type(field.data_type())
}

pub fn variant_metadata_field(data_type: DataType, nullable: bool) -> Field {
    Field::new(VARIANT_METADATA_FIELD_NAME, data_type, nullable).with_metadata(HashMap::from([(
        VARIANT_METADATA_MARKER_KEY.to_string(),
        VARIANT_METADATA_MARKER_VALUE.to_string(),
    )]))
}

pub fn is_variant_metadata_field(field: &Field) -> bool {
    field.name() == VARIANT_METADATA_FIELD_NAME
        && field
            .metadata()
            .get(VARIANT_METADATA_MARKER_KEY)
            .is_some_and(|value| value == VARIANT_METADATA_MARKER_VALUE)
        && is_binary_variant_field(field)
}

pub fn is_marked_variant_storage_type(data_type: &DataType) -> bool {
    let DataType::Struct(fields) = data_type else {
        return false;
    };
    let has_metadata = fields.iter().any(|field| is_variant_metadata_field(field));
    let has_value = fields
        .iter()
        .any(|field| field.name() == VARIANT_VALUE_FIELD_NAME && is_binary_variant_field(field));
    let has_typed_value = fields
        .iter()
        .any(|field| field.name() == VARIANT_TYPED_VALUE_FIELD_NAME);
    has_metadata && (has_value || has_typed_value)
}

pub fn is_variant_storage_type(data_type: &DataType) -> bool {
    let DataType::Struct(fields) = data_type else {
        return false;
    };
    let has_metadata = fields
        .iter()
        .any(|field| field.name() == VARIANT_METADATA_FIELD_NAME && is_binary_variant_field(field));
    let has_value = fields
        .iter()
        .any(|field| field.name() == VARIANT_VALUE_FIELD_NAME && is_binary_variant_field(field));
    let has_typed_value = fields
        .iter()
        .any(|field| field.name() == VARIANT_TYPED_VALUE_FIELD_NAME);
    has_metadata && (has_value || has_typed_value)
}

pub fn variant_storage_types_equivalent(left: &DataType, right: &DataType) -> bool {
    is_variant_storage_type(left) && is_variant_storage_type(right)
}

pub fn is_shredded_variant_storage_type(data_type: &DataType) -> bool {
    let DataType::Struct(fields) = data_type else {
        return false;
    };
    let has_metadata = fields
        .iter()
        .any(|field| field.name() == VARIANT_METADATA_FIELD_NAME && is_binary_variant_field(field));
    let has_typed_value = fields
        .iter()
        .any(|field| field.name() == VARIANT_TYPED_VALUE_FIELD_NAME);
    has_metadata && has_typed_value
}

pub fn is_binary_variant_field(field: &Field) -> bool {
    matches!(
        field.data_type(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}

fn unshred_array_for_write(
    source: &ArrayRef,
    source_field: &FieldRef,
    target_field: &Field,
) -> Result<(FieldRef, ArrayRef, bool), String> {
    if is_variant_arrow_field(target_field) && is_shredded_variant_storage_type(source.data_type())
    {
        let variant = VariantArray::try_new(source.as_ref()).map_err(|e| {
            format!(
                "variant write alignment input '{}': {e}",
                source_field.name()
            )
        })?;
        let unshredded = unshred_variant(&variant).map_err(|e| {
            format!(
                "variant write alignment column '{}': {e}",
                source_field.name()
            )
        })?;
        let unshredded = Arc::new(unshredded.into_inner()) as ArrayRef;
        let field = Arc::new(
            source_field
                .as_ref()
                .clone()
                .with_data_type(unshredded.data_type().clone()),
        );
        return Ok((field, unshredded, true));
    }

    match (source.data_type(), target_field.data_type()) {
        (DataType::Struct(source_fields), DataType::Struct(target_fields)) => {
            let Some(source_struct) = source.as_any().downcast_ref::<StructArray>() else {
                return Err(format!(
                    "variant write alignment expected struct array for '{}'",
                    source_field.name()
                ));
            };
            let mut fields: Vec<FieldRef> = Vec::with_capacity(source_fields.len());
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(source_fields.len());
            let mut changed = false;

            for (index, child_field) in source_fields.iter().enumerate() {
                if let Some(target_child) = target_fields
                    .iter()
                    .find(|field| field.name() == child_field.name())
                {
                    let (field, column, child_changed) = unshred_array_for_write(
                        source_struct.column(index),
                        child_field,
                        target_child,
                    )?;
                    fields.push(field);
                    columns.push(column);
                    changed |= child_changed;
                } else {
                    fields.push(child_field.clone());
                    columns.push(source_struct.column(index).clone());
                }
            }

            if !changed {
                return Ok((source_field.clone(), source.clone(), false));
            }

            let fields = Fields::from(fields);
            let array = Arc::new(
                StructArray::try_new(fields, columns, source_struct.nulls().cloned())
                    .map_err(|e| e.to_string())?,
            ) as ArrayRef;
            let field = Arc::new(
                source_field
                    .as_ref()
                    .clone()
                    .with_data_type(array.data_type().clone()),
            );
            Ok((field, array, true))
        }
        (DataType::List(_), DataType::List(target_element)) => {
            let Some(source_list) = source.as_any().downcast_ref::<ListArray>() else {
                return Err(format!(
                    "variant write alignment expected list array for '{}'",
                    source_field.name()
                ));
            };
            let source_element = match source.data_type() {
                DataType::List(source_element) => source_element,
                _ => unreachable!(),
            };
            let (element_field, values, changed) =
                unshred_array_for_write(source_list.values(), source_element, target_element)?;
            if !changed {
                return Ok((source_field.clone(), source.clone(), false));
            }

            let array = Arc::new(
                ListArray::try_new(
                    element_field,
                    source_list.offsets().clone(),
                    values,
                    source_list.nulls().cloned(),
                )
                .map_err(|e| e.to_string())?,
            ) as ArrayRef;
            let field = Arc::new(
                source_field
                    .as_ref()
                    .clone()
                    .with_data_type(array.data_type().clone()),
            );
            Ok((field, array, true))
        }
        (DataType::LargeList(_), DataType::LargeList(target_element)) => {
            let Some(source_list) = source.as_any().downcast_ref::<LargeListArray>() else {
                return Err(format!(
                    "variant write alignment expected large list array for '{}'",
                    source_field.name()
                ));
            };
            let source_element = match source.data_type() {
                DataType::LargeList(source_element) => source_element,
                _ => unreachable!(),
            };
            let (element_field, values, changed) =
                unshred_array_for_write(source_list.values(), source_element, target_element)?;
            if !changed {
                return Ok((source_field.clone(), source.clone(), false));
            }

            let array = Arc::new(
                LargeListArray::try_new(
                    element_field,
                    source_list.offsets().clone(),
                    values,
                    source_list.nulls().cloned(),
                )
                .map_err(|e| e.to_string())?,
            ) as ArrayRef;
            let field = Arc::new(
                source_field
                    .as_ref()
                    .clone()
                    .with_data_type(array.data_type().clone()),
            );
            Ok((field, array, true))
        }
        (DataType::Map(_, source_sorted), DataType::Map(target_entries, _)) => {
            let Some(source_map) = source.as_any().downcast_ref::<MapArray>() else {
                return Err(format!(
                    "variant write alignment expected map array for '{}'",
                    source_field.name()
                ));
            };
            let source_entries = match source.data_type() {
                DataType::Map(source_entries, _) => source_entries,
                _ => unreachable!(),
            };
            let entries = Arc::new(source_map.entries().clone()) as ArrayRef;
            let (entries_field, entries, changed) =
                unshred_array_for_write(&entries, source_entries, target_entries)?;
            if !changed {
                return Ok((source_field.clone(), source.clone(), false));
            }

            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    format!(
                        "variant write alignment map entries for '{}' must be struct",
                        source_field.name()
                    )
                })?
                .clone();
            let array = Arc::new(
                MapArray::try_new(
                    entries_field,
                    source_map.offsets().clone(),
                    entries,
                    source_map.nulls().cloned(),
                    *source_sorted,
                )
                .map_err(|e| e.to_string())?,
            ) as ArrayRef;
            let field = Arc::new(
                source_field
                    .as_ref()
                    .clone()
                    .with_data_type(array.data_type().clone()),
            );
            Ok((field, array, true))
        }
        _ => Ok((source_field.clone(), source.clone(), false)),
    }
}

fn infer_variant_shredding_type(
    column_index: usize,
    batches: &[RecordBatch],
    inference_buffer_size: usize,
    inference_node_budget: usize,
) -> Result<Option<DataType>, String> {
    let mut root = PathNode::default();
    let mut budget = InferenceBudget::new(inference_node_budget);
    let mut rows_seen = 0usize;

    for batch in batches {
        let array = VariantArray::try_new(batch.column(column_index).as_ref())
            .map_err(|e| format!("variant shredding inference: {e}"))?;
        for row in 0..array.len() {
            if rows_seen >= inference_buffer_size || budget.is_exhausted() {
                break;
            }
            rows_seen += 1;
            if array.is_null(row) {
                continue;
            }
            let value = array
                .try_value(row)
                .map_err(|e| format!("variant shredding inference row {row}: {e}"))?;
            observe_variant(&mut root, &value, 0, &mut budget)?;
        }
        if rows_seen >= inference_buffer_size || budget.is_exhausted() {
            break;
        }
    }

    let total = root.info.observation_count;
    if total == 0 {
        return Ok(None);
    }
    prune_infrequent_fields(&mut root, total);
    Ok(build_shredding_type(&root))
}

#[derive(Debug, Default)]
struct PathNode {
    info: FieldInfo,
    object_children: BTreeMap<String, PathNode>,
    array_element: Option<Box<PathNode>>,
}

#[derive(Debug, Default)]
struct FieldInfo {
    type_counts: HashMap<PhysicalKind, usize>,
    max_decimal_scale: u8,
    observation_count: usize,
}

#[derive(Debug)]
struct InferenceBudget {
    remaining_nodes: usize,
}

impl InferenceBudget {
    fn new(nodes: usize) -> Self {
        Self {
            remaining_nodes: nodes,
        }
    }

    fn consume_node(&mut self) -> bool {
        if self.remaining_nodes == 0 {
            return false;
        }
        self.remaining_nodes -= 1;
        true
    }

    fn is_exhausted(&self) -> bool {
        self.remaining_nodes == 0
    }
}

impl FieldInfo {
    fn observe(&mut self, value: &Variant<'_, '_>) {
        let Some(kind) = PhysicalKind::from_variant(value) else {
            return;
        };
        self.observation_count += 1;
        *self.type_counts.entry(kind).or_default() += 1;
        match value {
            Variant::Decimal4(decimal) => {
                self.max_decimal_scale = self.max_decimal_scale.max(decimal.scale());
            }
            Variant::Decimal8(decimal) => {
                self.max_decimal_scale = self.max_decimal_scale.max(decimal.scale());
            }
            Variant::Decimal16(decimal) => {
                self.max_decimal_scale = self.max_decimal_scale.max(decimal.scale());
            }
            _ => {}
        }
    }

    fn most_common_type(&self) -> Option<PhysicalKind> {
        self.type_counts
            .iter()
            .max_by(|(left_kind, left_count), (right_kind, right_count)| {
                left_count
                    .cmp(right_count)
                    .then_with(|| left_kind.priority().cmp(&right_kind.priority()))
            })
            .map(|(kind, _)| *kind)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum PhysicalKind {
    Boolean,
    Int64,
    Float64,
    Decimal,
    Date,
    Time,
    TimestampMicros,
    TimestampNtzMicros,
    TimestampNanos,
    TimestampNtzNanos,
    Binary,
    String,
    Uuid,
    Object,
    List,
}

impl PhysicalKind {
    fn from_variant(value: &Variant<'_, '_>) -> Option<Self> {
        match value {
            Variant::Null => None,
            Variant::BooleanTrue | Variant::BooleanFalse => Some(Self::Boolean),
            Variant::Int8(_) | Variant::Int16(_) | Variant::Int32(_) | Variant::Int64(_) => {
                Some(Self::Int64)
            }
            Variant::Float(_) | Variant::Double(_) => Some(Self::Float64),
            Variant::Decimal4(_) | Variant::Decimal8(_) | Variant::Decimal16(_) => {
                Some(Self::Decimal)
            }
            Variant::Date(_) => Some(Self::Date),
            Variant::Time(_) => Some(Self::Time),
            Variant::TimestampMicros(_) => Some(Self::TimestampMicros),
            Variant::TimestampNtzMicros(_) => Some(Self::TimestampNtzMicros),
            Variant::TimestampNanos(_) => Some(Self::TimestampNanos),
            Variant::TimestampNtzNanos(_) => Some(Self::TimestampNtzNanos),
            Variant::Binary(_) => Some(Self::Binary),
            Variant::String(_) | Variant::ShortString(_) => Some(Self::String),
            Variant::Uuid(_) => Some(Self::Uuid),
            Variant::Object(_) => Some(Self::Object),
            Variant::List(_) => Some(Self::List),
        }
    }

    fn priority(self) -> i32 {
        match self {
            Self::Object | Self::List => -1,
            Self::Boolean => 0,
            Self::Int64 => 1,
            Self::Float64 => 2,
            Self::Decimal => 3,
            Self::Date => 4,
            Self::Time => 5,
            Self::TimestampMicros => 6,
            Self::TimestampNtzMicros => 7,
            Self::Binary => 8,
            Self::String => 9,
            Self::TimestampNanos => 10,
            Self::TimestampNtzNanos => 11,
            Self::Uuid => 12,
        }
    }
}

fn observe_variant(
    node: &mut PathNode,
    value: &Variant<'_, '_>,
    depth: usize,
    budget: &mut InferenceBudget,
) -> Result<(), String> {
    if !budget.consume_node() {
        return Ok(());
    }

    node.info.observe(value);

    if depth >= MAX_SHREDDING_DEPTH {
        return Ok(());
    }

    match value {
        Variant::Object(object) => {
            for item in object.iter_try() {
                if budget.is_exhausted() {
                    break;
                }
                let (name, child_value) =
                    item.map_err(|e| format!("variant object traversal: {e}"))?;
                if !node.object_children.contains_key(name)
                    && node.object_children.len() >= MAX_INTERMEDIATE_FIELDS
                {
                    continue;
                }
                observe_variant(
                    node.object_children.entry(name.to_string()).or_default(),
                    &child_value,
                    depth + 1,
                    budget,
                )?;
            }
        }
        Variant::List(list) => {
            let element = node.array_element.get_or_insert_with(Default::default);
            for item in list.iter_try() {
                if budget.is_exhausted() {
                    break;
                }
                let child_value = item.map_err(|e| format!("variant list traversal: {e}"))?;
                observe_variant(element, &child_value, depth + 1, budget)?;
            }
        }
        _ => {}
    }

    Ok(())
}

fn prune_infrequent_fields(node: &mut PathNode, total: usize) {
    if total == 0 {
        return;
    }

    node.object_children.retain(|_, child| {
        (child.info.observation_count as f64 / total as f64) >= MIN_FIELD_FREQUENCY
    });

    if node.object_children.len() > MAX_SHREDDED_FIELDS {
        let mut ranked = node
            .object_children
            .iter()
            .map(|(name, child)| (name.clone(), child.info.observation_count))
            .collect::<Vec<_>>();
        ranked.sort_by(|(left_name, left_count), (right_name, right_count)| {
            right_count
                .cmp(left_count)
                .then_with(|| left_name.cmp(right_name))
        });
        let keep = ranked
            .into_iter()
            .take(MAX_SHREDDED_FIELDS)
            .map(|(name, _)| name)
            .collect::<HashSet<_>>();
        node.object_children.retain(|name, _| keep.contains(name));
    }

    let total = node.info.observation_count;
    for child in node.object_children.values_mut() {
        prune_infrequent_fields(child, total);
    }
    if let Some(element) = node.array_element.as_mut() {
        prune_infrequent_fields(element, total);
    }
}

fn build_shredding_type(node: &PathNode) -> Option<DataType> {
    match node.info.most_common_type()? {
        PhysicalKind::Object => {
            let fields = node
                .object_children
                .iter()
                .filter_map(|(name, child)| {
                    build_shredding_type(child).map(|data_type| Field::new(name, data_type, true))
                })
                .map(Arc::new)
                .collect::<Vec<_>>();
            if fields.is_empty() {
                None
            } else {
                Some(DataType::Struct(Fields::from(fields)))
            }
        }
        PhysicalKind::List => {
            let element_type = node
                .array_element
                .as_deref()
                .and_then(build_shredding_type)?;
            Some(DataType::List(Arc::new(Field::new(
                "element",
                element_type,
                true,
            ))))
        }
        PhysicalKind::Decimal => Some(DataType::Decimal128(38, node.info.max_decimal_scale as i8)),
        PhysicalKind::Boolean => Some(DataType::Boolean),
        PhysicalKind::Int64 => Some(DataType::Int64),
        PhysicalKind::Float64 => Some(DataType::Float64),
        PhysicalKind::Date => Some(DataType::Date32),
        PhysicalKind::Time => Some(DataType::Time64(TimeUnit::Microsecond)),
        PhysicalKind::TimestampMicros => Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        PhysicalKind::TimestampNtzMicros => Some(DataType::Timestamp(TimeUnit::Microsecond, None)),
        PhysicalKind::TimestampNanos => Some(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("UTC".into()),
        )),
        PhysicalKind::TimestampNtzNanos => Some(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        PhysicalKind::Binary => Some(DataType::Binary),
        PhysicalKind::String => Some(DataType::Utf8),
        PhysicalKind::Uuid => Some(DataType::FixedSizeBinary(16)),
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::expect_used, clippy::panic)]

    use datafusion::arrow::array::StringArray;
    use datafusion_common::Result;
    use parquet_variant_compute::{json_to_variant, unshred_variant};

    use super::*;

    fn variant_batch(values: Vec<Option<&str>>) -> Result<RecordBatch> {
        let json = Arc::new(StringArray::from(values)) as ArrayRef;
        let variant = json_to_variant(&json)?;
        let field = Arc::new(variant.field("payload"));
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(variant.into_inner())],
        )?)
    }

    #[test]
    fn variant_shredding_plan_shreds_object_fields() -> Result<()> {
        let batch = variant_batch(vec![
            Some(r#"{"a":2,"b":"lakehouse","nested":{"c":7}}"#),
            Some(r#"{"a":5,"b":"sail","nested":{"c":9}}"#),
        ])?;
        let plan = build_variant_shredding_plan(
            &batch.schema(),
            std::slice::from_ref(&batch),
            100,
            DEFAULT_VARIANT_INFERENCE_NODE_BUDGET,
        )
        .expect("variant shredding plan");
        let shredded = apply_variant_shredding_plan(&batch, &plan).expect("shredded batch");

        let schema = shredded.schema();
        let payload_field = schema.field_with_name("payload")?;
        let DataType::Struct(payload_fields) = payload_field.data_type() else {
            panic!("expected variant struct");
        };
        assert!(
            payload_fields
                .iter()
                .any(|field| field.name() == "typed_value")
        );
        let typed_value = payload_fields
            .iter()
            .find(|field| field.name() == "typed_value")
            .expect("typed_value field");
        let DataType::Struct(fields) = typed_value.data_type() else {
            panic!("expected typed_value struct");
        };
        assert!(fields.iter().any(|field| field.name() == "a"));
        assert!(fields.iter().any(|field| field.name() == "b"));
        assert!(fields.iter().any(|field| field.name() == "nested"));

        let shredded_variant = VariantArray::try_new(shredded.column(0).as_ref())?;
        let unshredded = unshred_variant(&shredded_variant)?;
        assert!(unshredded.value_field().is_some());
        assert!(unshredded.typed_value_field().is_none());

        Ok(())
    }

    #[test]
    fn variant_shredding_plan_is_noop_for_non_variant_schema() -> Result<()> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(datafusion::arrow::array::Int64Array::from(vec![
                1, 2,
            ]))],
        )?;
        let plan = build_variant_shredding_plan(
            &batch.schema(),
            std::slice::from_ref(&batch),
            100,
            DEFAULT_VARIANT_INFERENCE_NODE_BUDGET,
        )
        .expect("variant shredding plan");
        assert!(plan.is_noop());
        let rewritten = apply_variant_shredding_plan(&batch, &plan).expect("rewritten batch");
        assert_eq!(rewritten.schema(), batch.schema());
        Ok(())
    }

    #[test]
    fn variant_shredding_inference_budget_limits_wide_objects() -> Result<()> {
        let fields = (0..20)
            .map(|index| format!(r#""field_{index}":{index}"#))
            .collect::<Vec<_>>()
            .join(",");
        let value = format!("{{{fields}}}");
        let batch = variant_batch(vec![Some(value.as_str())])?;
        let plan =
            build_variant_shredding_plan(&batch.schema(), std::slice::from_ref(&batch), 100, 5)
                .expect("variant shredding plan");
        let shredded = apply_variant_shredding_plan(&batch, &plan).expect("shredded batch");

        let schema = shredded.schema();
        let payload_field = schema.field_with_name("payload")?;
        let DataType::Struct(payload_fields) = payload_field.data_type() else {
            panic!("expected variant struct");
        };
        let typed_value = payload_fields
            .iter()
            .find(|field| field.name() == "typed_value")
            .expect("typed_value field");
        let DataType::Struct(fields) = typed_value.data_type() else {
            panic!("expected typed_value struct");
        };

        assert!(fields.len() < 20);
        assert!(fields.len() <= 4);
        Ok(())
    }
}
