use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
    UInt32Array,
};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, SchemaRef as ArrowSchemaRef};
use datafusion::arrow::record_batch::RecordBatch;

use crate::spec::partition::UnboundPartitionSpec as PartitionSpec;
use crate::spec::schema::Schema as IcebergSchema;
use crate::spec::transform::Transform;
use crate::spec::types::values::{Literal, PrimitiveLiteral};
use crate::spec::types::{PrimitiveType, Type};

pub struct PartitionBatchResult {
    pub record_batch: RecordBatch,
    pub partition_values: Vec<Option<Literal>>, // aligned with PartitionSpec fields
    pub partition_dir: String, // formatted path segment like key=value/... or empty
    pub spec_id: i32,
}

fn scalar_to_literal(array: &ArrayRef, row: usize) -> Option<Literal> {
    match array.data_type() {
        ArrowDataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>()?;
            if a.is_null(row) {
                None
            } else {
                Some(Literal::Primitive(PrimitiveLiteral::Int(a.value(row))))
            }
        }
        ArrowDataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>()?;
            if a.is_null(row) {
                None
            } else {
                Some(Literal::Primitive(PrimitiveLiteral::Long(a.value(row))))
            }
        }
        ArrowDataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>()?;
            if a.is_null(row) {
                None
            } else {
                Some(Literal::Primitive(PrimitiveLiteral::String(
                    a.value(row).to_string(),
                )))
            }
        }
        ArrowDataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>()?;
            if a.is_null(row) {
                None
            } else {
                Some(Literal::Primitive(PrimitiveLiteral::Boolean(a.value(row))))
            }
        }
        ArrowDataType::Float32 => {
            let a = array.as_any().downcast_ref::<Float32Array>()?;
            if a.is_null(row) {
                None
            } else {
                Some(Literal::Primitive(PrimitiveLiteral::Float(
                    ordered_float::OrderedFloat(a.value(row)),
                )))
            }
        }
        ArrowDataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>()?;
            if a.is_null(row) {
                None
            } else {
                Some(Literal::Primitive(PrimitiveLiteral::Double(
                    ordered_float::OrderedFloat(a.value(row)),
                )))
            }
        }
        _ => None,
    }
}

fn apply_transform(
    transform: Transform,
    _field_type: &Type,
    value: Option<Literal>,
) -> Option<Literal> {
    match transform {
        Transform::Identity | Transform::Unknown | Transform::Void => value,
        Transform::Truncate(w) => match value {
            Some(Literal::Primitive(PrimitiveLiteral::String(s))) => {
                let taken = s.chars().take(w as usize).collect::<String>();
                Some(Literal::Primitive(PrimitiveLiteral::String(taken)))
            }
            Some(Literal::Primitive(PrimitiveLiteral::Int(v))) => {
                let w = w as i32;
                let rem = v.rem_euclid(w);
                Some(Literal::Primitive(PrimitiveLiteral::Int(v - rem)))
            }
            Some(Literal::Primitive(PrimitiveLiteral::Long(v))) => {
                let w = w as i64;
                let rem = v.rem_euclid(w);
                Some(Literal::Primitive(PrimitiveLiteral::Long(v - rem)))
            }
            other => other,
        },
        Transform::Bucket(_n) => value,
        Transform::Year | Transform::Month | Transform::Day | Transform::Hour => value,
    }
}

fn field_name_from_id(schema: &IcebergSchema, field_id: i32) -> Option<String> {
    schema
        .name_by_field_id(field_id)
        .map(|s| s.split('.').next_back().unwrap_or(s).to_string())
}

fn build_partition_dir(
    spec: &PartitionSpec,
    iceberg_schema: &IcebergSchema,
    values: &[Option<Literal>],
) -> String {
    if spec.fields.is_empty() {
        return String::new();
    }
    let mut segs = Vec::new();
    for (i, f) in spec.fields.iter().enumerate() {
        let field_type = iceberg_schema
            .field_by_id(f.source_id)
            .map(|nf| nf.field_type.as_ref())
            .unwrap_or(&Type::Primitive(PrimitiveType::String));
        let val = values.get(i).cloned().flatten();
        let human = f.transform.to_human_string(field_type, val.as_ref());
        segs.push(format!("{}={}", f.name, human));
    }
    segs.join("/")
}

#[allow(dead_code)]
pub fn compute_partition_values(
    batch: &RecordBatch,
    spec: &PartitionSpec,
    iceberg_schema: &IcebergSchema,
    partition_columns: &[String],
) -> Result<(Vec<Option<Literal>>, String), String> {
    let _ = partition_columns; // not used in single-group fallback
    let mut values = Vec::with_capacity(spec.fields.len());
    for f in &spec.fields {
        let col_name = field_name_from_id(iceberg_schema, f.source_id)
            .ok_or_else(|| format!("Unknown field id {}", f.source_id))?;
        let col_index = batch
            .schema()
            .index_of(&col_name)
            .map_err(|e| e.to_string())?;
        let lit = scalar_to_literal(batch.column(col_index), 0);
        let field_type = iceberg_schema
            .field_by_id(f.source_id)
            .map(|nf| nf.field_type.as_ref())
            .unwrap_or(&Type::Primitive(PrimitiveType::String));
        values.push(apply_transform(f.transform, field_type, lit));
    }
    let dir = build_partition_dir(spec, iceberg_schema, &values);
    Ok((values, dir))
}

/// Group a RecordBatch by partition values according to the given spec and schema.
pub fn group_by_partition(
    batch: &RecordBatch,
    spec: &PartitionSpec,
    iceberg_schema: &IcebergSchema,
) -> Result<Vec<PartitionBatchResult>, String> {
    if spec.fields.is_empty() {
        return Ok(vec![PartitionBatchResult {
            record_batch: batch.clone(),
            partition_values: Vec::new(),
            partition_dir: String::new(),
            spec_id: 0,
        }]);
    }

    // Pre-compute field mapping: source_id -> (col_index, field_type, transform, name)
    struct FieldCtx<'a> {
        col_index: usize,
        field_type: &'a Type,
        transform: Transform,
    }

    let mut fctx: Vec<FieldCtx> = Vec::with_capacity(spec.fields.len());
    for f in &spec.fields {
        let name = field_name_from_id(iceberg_schema, f.source_id)
            .ok_or_else(|| format!("Unknown field id {}", f.source_id))?;
        let col_index = batch.schema().index_of(&name).map_err(|e| e.to_string())?;
        let field_type = iceberg_schema
            .field_by_id(f.source_id)
            .map(|nf| nf.field_type.as_ref())
            .unwrap_or(&Type::Primitive(PrimitiveType::String));
        fctx.push(FieldCtx {
            col_index,
            field_type,
            transform: f.transform,
        });
    }

    // Build groups keyed by partition dir
    use std::collections::HashMap;
    struct Group {
        indices: Vec<u32>,
        values: Vec<Option<Literal>>,
        dir: String,
    }
    let mut groups: HashMap<String, Group> = HashMap::new();
    let num_rows = batch.num_rows();
    for row in 0..num_rows {
        let mut values: Vec<Option<Literal>> = Vec::with_capacity(fctx.len());
        for fc in &fctx {
            let lit = scalar_to_literal(batch.column(fc.col_index), row);
            let t = apply_transform(fc.transform, fc.field_type, lit);
            values.push(t);
        }
        let dir = build_partition_dir(spec, iceberg_schema, &values);
        let entry = groups.entry(dir.clone()).or_insert_with(|| Group {
            indices: Vec::new(),
            values: values.clone(),
            dir: dir.clone(),
        });
        entry.indices.push(row as u32);
    }

    // Build result batches
    let schema: ArrowSchemaRef = batch.schema();
    let mut results: Vec<PartitionBatchResult> = Vec::with_capacity(groups.len());
    for (_k, g) in groups.into_iter() {
        let idx = UInt32Array::from(g.indices);
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
        for col in batch.columns() {
            let taken = compute::take(col.as_ref(), &idx, None).map_err(|e| e.to_string())?;
            cols.push(taken);
        }
        let rb = RecordBatch::try_new(schema.clone(), cols).map_err(|e| e.to_string())?;
        results.push(PartitionBatchResult {
            record_batch: rb,
            partition_values: g.values,
            partition_dir: g.dir,
            spec_id: 0,
        });
    }

    Ok(results)
}
