use std::sync::Arc;

use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::array::{Array, ArrayRef, GenericListArray, MapArray, StructArray};
use datafusion::arrow::compute::CastOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory, SchemaMapper};
use datafusion_common::format::DEFAULT_CAST_OPTIONS;
use datafusion_common::nested_struct::{
    cast_column as cast_struct_column, validate_struct_compatibility,
};
use datafusion_common::Result;

#[derive(Debug, Clone, Default)]
pub struct DeltaSchemaAdapterFactory;

impl SchemaAdapterFactory for DeltaSchemaAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        _table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(DeltaSchemaAdapter {
            projected_table_schema,
        })
    }
}

#[derive(Debug, Clone)]
struct DeltaSchemaAdapter {
    projected_table_schema: SchemaRef,
}

impl SchemaAdapter for DeltaSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.projected_table_schema.field(index);
        Some(file_schema.fields.find(field.name())?.0)
    }

    fn map_schema(&self, file_schema: &Schema) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        fn can_map_field(file_field: &Field, table_field: &Field) -> Result<bool> {
            match (file_field.data_type(), table_field.data_type()) {
                (DataType::Struct(src), DataType::Struct(tgt)) => {
                    validate_struct_compatibility(src, tgt)?;
                    Ok(true)
                }
                (DataType::List(src_elem), DataType::List(tgt_elem))
                | (DataType::LargeList(src_elem), DataType::LargeList(tgt_elem)) => {
                    match (src_elem.data_type(), tgt_elem.data_type()) {
                        (DataType::Struct(src), DataType::Struct(tgt)) => {
                            validate_struct_compatibility(src, tgt)?;
                            Ok(true)
                        }
                        _ => Ok(datafusion::arrow::compute::can_cast_types(
                            src_elem.data_type(),
                            tgt_elem.data_type(),
                        )),
                    }
                }
                (DataType::Map(src_kv, _), DataType::Map(tgt_kv, _)) => {
                    match (src_kv.data_type(), tgt_kv.data_type()) {
                        (DataType::Struct(src_children), DataType::Struct(tgt_children)) => {
                            validate_struct_compatibility(src_children, tgt_children)?;
                            Ok(true)
                        }
                        _ => Ok(false),
                    }
                }
                _ => Ok(datafusion::arrow::compute::can_cast_types(
                    file_field.data_type(),
                    table_field.data_type(),
                )),
            }
        }

        let mut projection: Vec<usize> = Vec::new();
        let mut field_mappings: Vec<Option<usize>> =
            vec![None; self.projected_table_schema.fields().len()];
        for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
            if let Some((table_idx, table_field)) =
                self.projected_table_schema.fields().find(file_field.name())
            {
                if can_map_field(file_field, table_field)? {
                    field_mappings[table_idx] = Some(projection.len());
                    projection.push(file_idx);
                }
            }
        }

        #[derive(Debug)]
        struct DeltaSchemaMapping {
            projected_table_schema: SchemaRef,
            field_mappings: Vec<Option<usize>>,
        }

        impl SchemaMapper for DeltaSchemaMapping {
            fn map_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
                let (_old_schema, batch_cols, batch_rows) = batch.into_parts();

                let cols = self
                    .projected_table_schema
                    .fields()
                    .iter()
                    .zip(&self.field_mappings)
                    .map(|(field, file_idx)| {
                        file_idx.map_or_else(
                            || {
                                Ok(datafusion::arrow::array::new_null_array(
                                    field.data_type(),
                                    batch_rows,
                                ))
                            },
                            |batch_idx| {
                                cast_nested_column(
                                    &batch_cols[batch_idx],
                                    field,
                                    &DEFAULT_CAST_OPTIONS,
                                )
                            },
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

                let options = RecordBatchOptions::new().with_row_count(Some(batch_rows));
                let schema = Arc::clone(&self.projected_table_schema);
                let record_batch = RecordBatch::try_new_with_options(schema, cols, &options)?;
                Ok(record_batch)
            }

            fn map_column_statistics(
                &self,
                file_col_statistics: &[datafusion_common::ColumnStatistics],
            ) -> Result<Vec<datafusion_common::ColumnStatistics>> {
                let mut out = vec![];
                for (_field, idx) in self
                    .projected_table_schema
                    .fields()
                    .iter()
                    .zip(&self.field_mappings)
                {
                    if let Some(i) = idx {
                        out.push(file_col_statistics.get(*i).cloned().unwrap_or_default());
                    } else {
                        out.push(datafusion_common::ColumnStatistics::new_unknown());
                    }
                }
                Ok(out)
            }
        }

        let mapper: Arc<dyn SchemaMapper> = Arc::new(DeltaSchemaMapping {
            projected_table_schema: Arc::clone(&self.projected_table_schema),
            field_mappings,
        });
        Ok((mapper, projection))
    }
}

fn cast_nested_column(
    array: &ArrayRef,
    target_field: &Field,
    opts: &CastOptions,
) -> Result<ArrayRef> {
    match target_field.data_type() {
        DataType::Struct(_) => cast_struct_column(array, target_field, opts),
        DataType::List(elem_field) => cast_list(array, elem_field.as_ref(), opts),
        DataType::LargeList(elem_field) => cast_large_list(array, elem_field.as_ref(), opts),
        DataType::Map(kv_field, ordered) => cast_map(array, kv_field.as_ref(), *ordered, opts),
        DataType::FixedSizeList(elem_field, len) => {
            cast_fixed_size_list(array, elem_field.as_ref(), *len)
        }
        _ => Ok(datafusion::arrow::compute::cast(
            array,
            target_field.data_type(),
        )?),
    }
}

fn cast_list(array: &ArrayRef, target_elem: &Field, opts: &CastOptions) -> Result<ArrayRef> {
    let list = array.as_list::<i32>().clone();
    let (_list_field, offsets, values, nulls) = list.into_parts();
    let casted_values = match (values.data_type(), target_elem.data_type()) {
        (DataType::Struct(_), DataType::Struct(_)) => {
            cast_struct_column(&values, target_elem, opts)?
        }
        _ => datafusion::arrow::compute::cast(&values, target_elem.data_type())?,
    };
    let new_elem_field = Arc::new(target_elem.clone());
    let new_list = GenericListArray::try_new(new_elem_field, offsets, casted_values, nulls)?;
    Ok(Arc::new(new_list))
}

fn cast_large_list(array: &ArrayRef, target_elem: &Field, opts: &CastOptions) -> Result<ArrayRef> {
    let list = array.as_list::<i64>().clone();
    let (_list_field, offsets, values, nulls) = list.into_parts();
    let casted_values = match (values.data_type(), target_elem.data_type()) {
        (DataType::Struct(_), DataType::Struct(_)) => {
            cast_struct_column(&values, target_elem, opts)?
        }
        _ => datafusion::arrow::compute::cast(&values, target_elem.data_type())?,
    };
    let new_elem_field = Arc::new(target_elem.clone());
    let new_list = GenericListArray::try_new(new_elem_field, offsets, casted_values, nulls)?;
    Ok(Arc::new(new_list))
}

fn cast_fixed_size_list(array: &ArrayRef, target_elem: &Field, len: i32) -> Result<ArrayRef> {
    Ok(datafusion::arrow::compute::cast(
        array,
        &DataType::FixedSizeList(Arc::new(target_elem.clone()), len),
    )?)
}

fn cast_map(
    array: &ArrayRef,
    target_kv: &Field,
    ordered: bool,
    opts: &CastOptions,
) -> Result<ArrayRef> {
    let map = array.as_map().clone();
    let (_map_field, offsets, entries, nulls, _ord) = map.into_parts();
    let casted_entries = match (entries.data_type(), target_kv.data_type()) {
        (DataType::Struct(_), DataType::Struct(_)) => {
            let entries_arr: ArrayRef = Arc::new(entries.clone());
            cast_struct_column(&entries_arr, target_kv, opts)?
        }
        _ => datafusion::arrow::compute::cast(&entries, target_kv.data_type())?,
    };
    let entries_sa = casted_entries
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "Failed to downcast casted map entries to StructArray".to_string(),
            )
        })?
        .clone();
    let new_map = MapArray::try_new(
        Arc::new(target_kv.clone()),
        offsets,
        entries_sa,
        nulls,
        ordered,
    )?;
    Ok(Arc::new(new_map))
}
