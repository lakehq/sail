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

use std::sync::Arc;

use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::Result;
use datafusion::physical_expr::expressions::CastExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_common::DataFusionError;
use indexmap::IndexMap;

/// Shared type conversion utilities for Delta Lake operations.
#[derive(Debug)]
pub struct DeltaTypeConverter;

impl DeltaTypeConverter {
    pub fn can_promote_types(from_type: &DataType, to_type: &DataType) -> bool {
        if from_type == to_type {
            return true;
        }

        match (from_type, to_type) {
            (
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            ) => true,
            (
                DataType::Binary | DataType::BinaryView | DataType::LargeBinary,
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
            ) => true,
            (
                DataType::Decimal128(from_precision, from_scale)
                | DataType::Decimal256(from_precision, from_scale),
                DataType::Decimal128(to_precision, to_scale),
            ) => from_precision <= to_precision && from_scale <= to_scale,
            (from_dt, to_dt) => can_cast_types(from_dt, to_dt) || can_cast_types(to_dt, from_dt),
        }
    }

    pub fn promote_field_types(table_field: &Field, input_field: &Field) -> Result<Field> {
        let table_type = table_field.data_type();
        let input_type = input_field.data_type();

        if table_type == input_type {
            let is_nullable = table_field.is_nullable() || input_field.is_nullable();
            return Ok(Field::new(
                table_field.name(),
                table_type.clone(),
                is_nullable,
            ));
        }

        let merged_type = match (table_type, input_type) {
            (DataType::Struct(table_fields), DataType::Struct(input_fields)) => {
                let mut merged_fields: IndexMap<&str, Field> = table_fields
                    .iter()
                    .map(|f| (f.name().as_str(), f.as_ref().clone()))
                    .collect();
                for inf in input_fields {
                    let name = inf.name().as_str();
                    if let Some(existing) = merged_fields.get(name).cloned() {
                        let merged_child = Self::promote_field_types(&existing, inf.as_ref())?;
                        merged_fields.insert(name, merged_child);
                    } else {
                        merged_fields.insert(name, inf.as_ref().clone());
                    }
                }
                let merged_children: Vec<Field> = merged_fields.into_values().collect();
                Some(DataType::Struct(merged_children.into()))
            }
            (DataType::List(table_elem), DataType::List(input_elem)) => {
                let merged_elem =
                    Self::promote_field_types(table_elem.as_ref(), input_elem.as_ref())?;
                Some(DataType::List(Arc::new(merged_elem)))
            }
            (DataType::LargeList(table_elem), DataType::LargeList(input_elem)) => {
                let merged_elem =
                    Self::promote_field_types(table_elem.as_ref(), input_elem.as_ref())?;
                Some(DataType::LargeList(Arc::new(merged_elem)))
            }
            (
                DataType::FixedSizeList(table_elem, t_len),
                DataType::FixedSizeList(input_elem, i_len),
            ) => {
                if t_len != i_len {
                    return Err(DataFusionError::Plan(format!(
                        "Schema evolution failed: incompatible sizes for fixed-size list field '{}': {t_len} vs {i_len}",
                        table_field.name()
                    )));
                }
                let merged_elem =
                    Self::promote_field_types(table_elem.as_ref(), input_elem.as_ref())?;
                Some(DataType::FixedSizeList(Arc::new(merged_elem), *t_len))
            }
            (DataType::Map(table_kv, t_sorted), DataType::Map(input_kv, i_sorted)) => {
                let t_kv_dt = table_kv.data_type();
                let i_kv_dt = input_kv.data_type();
                let (t_key, t_val) = match t_kv_dt {
                    DataType::Struct(children) if children.len() == 2 => {
                        (&children[0], &children[1])
                    }
                    _ => {
                        return Err(DataFusionError::Plan(format!(
                            "Unexpected map child layout for field '{}': {:?}",
                            table_field.name(),
                            t_kv_dt
                        )))
                    }
                };
                let (i_key, i_val) = match i_kv_dt {
                    DataType::Struct(children) if children.len() == 2 => {
                        (&children[0], &children[1])
                    }
                    _ => {
                        return Err(DataFusionError::Plan(format!(
                            "Unexpected map child layout for input field '{}': {:?}",
                            table_field.name(),
                            i_kv_dt
                        )))
                    }
                };
                if t_key.data_type() != i_key.data_type() {
                    return Err(DataFusionError::Plan(format!(
                        "Schema evolution failed: incompatible map key types for field '{}': {:?} vs {:?}",
                        table_field.name(),
                        t_key.data_type(),
                        i_key.data_type()
                    )));
                }
                let merged_val = Self::promote_field_types(t_val, i_val)?;
                let kv_struct = DataType::Struct(vec![t_key.as_ref().clone(), merged_val].into());
                let kv_field = Field::new(table_kv.name(), kv_struct, false);
                let _ = i_sorted;
                Some(DataType::Map(Arc::new(kv_field), *t_sorted))
            }
            (
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            ) => Some(input_type.clone()),
            (
                DataType::Binary | DataType::BinaryView | DataType::LargeBinary,
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
            ) => Some(input_type.clone()),
            (
                DataType::Decimal128(table_precision, table_scale)
                | DataType::Decimal256(table_precision, table_scale),
                DataType::Decimal128(input_precision, input_scale),
            ) => {
                if input_precision <= table_precision && input_scale <= table_scale {
                    Some(table_type.clone())
                } else {
                    return Err(DataFusionError::Plan(format!(
                        "Cannot merge field {} from {} to {}. Decimal precision/scale mismatch.",
                        table_field.name(),
                        input_type,
                        table_type
                    )));
                }
            }
            (table_dt, input_dt) => {
                if can_cast_types(table_dt, input_dt) {
                    Some(input_type.clone())
                } else if can_cast_types(input_dt, table_dt) {
                    Some(table_type.clone())
                } else {
                    None
                }
            }
        };

        if let Some(new_type) = merged_type {
            let is_nullable = table_field.is_nullable() || input_field.is_nullable();
            Ok(Field::new(table_field.name(), new_type, is_nullable))
        } else {
            Err(DataFusionError::Plan(format!(
                "Schema evolution failed: incompatible types for field '{}'. Cannot merge from {:?} to {:?}. Consider using overwriteSchema=true if you want to replace the schema entirely.",
                table_field.name(),
                table_type,
                input_type
            )))
        }
    }

    pub fn create_cast_expr(
        expr: Arc<dyn PhysicalExpr>,
        target_type: &DataType,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(CastExpr::new(expr, target_type.clone(), None)))
    }

    pub fn validate_cast_safety(
        from_type: &DataType,
        to_type: &DataType,
        field_name: &str,
    ) -> Result<()> {
        if !can_cast_types(from_type, to_type) {
            return Err(DataFusionError::Plan(format!(
                "Unsafe cast for field '{field_name}': cannot cast from '{from_type}' to '{to_type}'"
            )));
        }

        match (from_type, to_type) {
            (
                DataType::Decimal128(from_precision, from_scale),
                DataType::Decimal128(to_precision, to_scale),
            ) => {
                if from_precision > to_precision || from_scale > to_scale {
                    return Err(DataFusionError::Plan(format!(
                        "Potential precision loss in decimal cast for field '{field_name}': from Decimal({from_precision},{from_scale}) to Decimal({to_precision},{to_scale})"
                    )));
                }
            }
            (DataType::Int64, DataType::Int32)
            | (DataType::Int32, DataType::Int16)
            | (DataType::Int16, DataType::Int8) => {}
            _ => {}
        }

        Ok(())
    }

    pub fn get_wider_type(type1: &DataType, type2: &DataType) -> Option<DataType> {
        if type1 == type2 {
            return Some(type1.clone());
        }

        match (type1, type2) {
            // Integer promotions
            (DataType::Int8, DataType::Int16 | DataType::Int32 | DataType::Int64)
            | (DataType::Int16, DataType::Int32 | DataType::Int64)
            | (DataType::Int32, DataType::Int64) => Some(type2.clone()),
            (DataType::Int16 | DataType::Int32 | DataType::Int64, DataType::Int8)
            | (DataType::Int32 | DataType::Int64, DataType::Int16)
            | (DataType::Int64, DataType::Int32) => Some(type1.clone()),

            // Float promotions
            (DataType::Float32, DataType::Float64) | (DataType::Float64, DataType::Float32) => {
                Some(DataType::Float64)
            }

            // String promotions
            (DataType::Utf8, DataType::LargeUtf8) | (DataType::LargeUtf8, DataType::Utf8) => {
                Some(DataType::LargeUtf8)
            }

            // Binary promotions
            (DataType::Binary, DataType::LargeBinary)
            | (DataType::LargeBinary, DataType::Binary) => Some(DataType::LargeBinary),

            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Field;

    use super::*;

    #[test]
    fn test_can_promote_types() {
        assert!(DeltaTypeConverter::can_promote_types(
            &DataType::Int32,
            &DataType::Int32
        ));
        assert!(DeltaTypeConverter::can_promote_types(
            &DataType::Utf8,
            &DataType::LargeUtf8
        ));
        assert!(DeltaTypeConverter::can_promote_types(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(12, 2)
        ));
        assert!(DeltaTypeConverter::can_promote_types(
            &DataType::Int32,
            &DataType::Int64
        ));
    }

    #[test]
    fn test_promote_field_types() {
        let table_field = Field::new("test", DataType::Int32, false);
        let input_field = Field::new("test", DataType::Int32, true);

        let result = DeltaTypeConverter::promote_field_types(&table_field, &input_field)
            .unwrap_or(Field::new("test", DataType::Null, true));

        assert!(result.is_nullable());
        assert_eq!(result.data_type(), &DataType::Int32);
        assert_eq!(result.name(), "test");
    }

    #[test]
    fn test_string_type_promotion() {
        let table_field = Field::new("test", DataType::Utf8, false);
        let input_field = Field::new("test", DataType::LargeUtf8, true);

        let result = DeltaTypeConverter::promote_field_types(&table_field, &input_field)
            .unwrap_or(Field::new("test", DataType::Null, true));

        assert!(result.is_nullable());
        assert_eq!(result.data_type(), &DataType::LargeUtf8);
    }

    #[test]
    fn test_decimal_type_promotion() {
        let table_field = Field::new("test", DataType::Decimal128(12, 2), false);
        let input_field = Field::new("test", DataType::Decimal128(10, 2), true);

        let result = DeltaTypeConverter::promote_field_types(&table_field, &input_field)
            .unwrap_or(Field::new("test", DataType::Null, true));

        assert!(result.is_nullable());
        assert_eq!(result.data_type(), &DataType::Decimal128(12, 2));
    }

    #[test]
    fn test_incompatible_decimal_promotion() {
        let table_field = Field::new("test", DataType::Decimal128(10, 2), false);
        let input_field = Field::new("test", DataType::Decimal128(12, 3), true);

        let result = DeltaTypeConverter::promote_field_types(&table_field, &input_field);

        assert!(result.is_err());
    }

    #[test]
    fn test_get_wider_type() {
        assert_eq!(
            DeltaTypeConverter::get_wider_type(&DataType::Int32, &DataType::Int64),
            Some(DataType::Int64)
        );
        assert_eq!(
            DeltaTypeConverter::get_wider_type(&DataType::Float32, &DataType::Float64),
            Some(DataType::Float64)
        );
        assert_eq!(
            DeltaTypeConverter::get_wider_type(&DataType::Utf8, &DataType::Int32),
            None
        );
    }
}
