// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_schema::extension::ExtensionType;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use parquet_variant::Variant;
use parquet_variant_compute::{shred_variant, VariantArray, VariantType};

const MIN_FIELD_FREQUENCY: f64 = 0.10;
const MAX_SHREDDED_FIELDS: usize = 300;
const MAX_SHREDDING_DEPTH: usize = 50;
const MAX_INTERMEDIATE_FIELDS: usize = 1000;

#[derive(Debug, Clone)]
pub struct VariantShreddingPlan {
    column_types: Vec<Option<DataType>>,
}

impl VariantShreddingPlan {
    pub fn is_noop(&self) -> bool {
        self.column_types.iter().all(Option::is_none)
    }
}

pub fn build_variant_shredding_plan(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    inference_buffer_size: usize,
) -> Result<VariantShreddingPlan, String> {
    let mut column_types = vec![None; schema.fields().len()];
    let inference_buffer_size = inference_buffer_size.max(1);

    for (index, field) in schema.fields().iter().enumerate() {
        if is_variant_arrow_field(field) {
            column_types[index] =
                infer_variant_shredding_type(index, batches, inference_buffer_size)?;
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

fn infer_variant_shredding_type(
    column_index: usize,
    batches: &[RecordBatch],
    inference_buffer_size: usize,
) -> Result<Option<DataType>, String> {
    let mut root = PathNode::default();
    let mut rows_seen = 0usize;

    for batch in batches {
        let array = VariantArray::try_new(batch.column(column_index).as_ref())
            .map_err(|e| format!("variant shredding inference: {e}"))?;
        for row in 0..array.len() {
            if rows_seen >= inference_buffer_size {
                break;
            }
            rows_seen += 1;
            if array.is_null(row) {
                continue;
            }
            let value = array
                .try_value(row)
                .map_err(|e| format!("variant shredding inference row {row}: {e}"))?;
            observe_variant(&mut root, &value, 0)?;
        }
        if rows_seen >= inference_buffer_size {
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

fn is_variant_arrow_field(field: &Field) -> bool {
    field.extension_type_name() == Some(VariantType::NAME)
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
) -> Result<(), String> {
    node.info.observe(value);

    if depth >= MAX_SHREDDING_DEPTH {
        return Ok(());
    }

    match value {
        Variant::Object(object) => {
            for item in object.iter_try() {
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
                )?;
            }
        }
        Variant::List(list) => {
            let element = node.array_element.get_or_insert_with(Default::default);
            for item in list.iter_try() {
                let child_value = item.map_err(|e| format!("variant list traversal: {e}"))?;
                observe_variant(element, &child_value, depth + 1)?;
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
            .collect::<std::collections::BTreeSet<_>>();
        node.object_children.retain(|name, _| keep.contains(name));
    }

    for child in node.object_children.values_mut() {
        prune_infrequent_fields(child, total);
    }
    if let Some(element) = node.array_element.as_mut() {
        prune_infrequent_fields(element, total);
    }
}

fn build_shredding_type(node: &PathNode) -> Option<DataType> {
    let kind = node.info.most_common_type()?;
    match kind {
        PhysicalKind::Object => {
            let fields = node
                .object_children
                .iter()
                .filter_map(|(name, child)| {
                    build_shredding_type(child).map(|data_type| Field::new(name, data_type, true))
                })
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
                .as_ref()
                .and_then(|element| build_shredding_type(element))?;
            Some(DataType::List(Arc::new(Field::new_list_field(
                element_type,
                true,
            ))))
        }
        PhysicalKind::Boolean => Some(DataType::Boolean),
        PhysicalKind::Int64 => Some(DataType::Int64),
        PhysicalKind::Float64 => Some(DataType::Float64),
        PhysicalKind::Decimal => Some(DataType::Decimal128(
            38,
            i8::try_from(node.info.max_decimal_scale).unwrap_or(38),
        )),
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
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::Field;
    use datafusion_common::{DataFusionError, Result};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet_variant_compute::{json_to_variant, unshred_variant};

    use super::*;

    fn variant_batch(values: Vec<Option<&str>>) -> Result<RecordBatch> {
        let json = Arc::new(StringArray::from(values)) as ArrayRef;
        let variant = json_to_variant(&json)?;
        let mut field = variant.field("payload");
        field
            .metadata_mut()
            .insert(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string());
        let field = Arc::new(field);
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(variant.into_inner())],
        )?)
    }

    #[test]
    fn variant_shredding_plan_shreds_object_fields() -> Result<()> {
        let batch = variant_batch(vec![
            Some(r#"{"a":2,"b":"iceberg","nested":{"c":7}}"#),
            Some(r#"{"a":5,"b":"sail","nested":{"c":9}}"#),
        ])?;
        let plan = build_variant_shredding_plan(&batch.schema(), std::slice::from_ref(&batch), 100)
            .map_err(DataFusionError::Plan)?;
        let shredded =
            apply_variant_shredding_plan(&batch, &plan).map_err(DataFusionError::Plan)?;

        let schema = shredded.schema();
        let payload_field = schema.field_with_name("payload")?;
        assert_eq!(
            payload_field.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"2".to_string())
        );

        let DataType::Struct(payload_fields) = payload_field.data_type() else {
            return Err(DataFusionError::Plan("expected variant struct".to_string()));
        };
        assert!(payload_fields
            .iter()
            .any(|field| field.name() == "typed_value"));
        let typed_value = payload_fields
            .iter()
            .find(|field| field.name() == "typed_value")
            .ok_or_else(|| DataFusionError::Plan("typed_value field missing".to_string()))?;
        let DataType::Struct(fields) = typed_value.data_type() else {
            return Err(DataFusionError::Plan(
                "expected typed_value struct".to_string(),
            ));
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
        let plan = build_variant_shredding_plan(&batch.schema(), std::slice::from_ref(&batch), 100)
            .map_err(DataFusionError::Plan)?;
        assert!(plan.is_noop());
        let rewritten =
            apply_variant_shredding_plan(&batch, &plan).map_err(DataFusionError::Plan)?;
        assert_eq!(rewritten.schema(), batch.schema());
        Ok(())
    }
}
