use std::sync::Arc;

use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::Result;
use datafusion::physical_expr::expressions::CastExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_common::DataFusionError;

/// Shared type conversion utilities for Delta Lake operations.
#[derive(Debug)]
pub struct DeltaTypeConverter;

impl DeltaTypeConverter {
    /// Check if two data types can be promoted/merged
    pub fn can_promote_types(from_type: &DataType, to_type: &DataType) -> bool {
        if from_type == to_type {
            return true;
        }

        match (from_type, to_type) {
            // String type promotions
            (
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            ) => true,

            // Binary type promotions
            (
                DataType::Binary | DataType::BinaryView | DataType::LargeBinary,
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
            ) => true,

            // Decimal type promotions
            (
                DataType::Decimal128(from_precision, from_scale)
                | DataType::Decimal256(from_precision, from_scale),
                DataType::Decimal128(to_precision, to_scale),
            ) => from_precision <= to_precision && from_scale <= to_scale,

            // For other types, use Arrow's can_cast_types to determine compatibility
            (from_dt, to_dt) => can_cast_types(from_dt, to_dt) || can_cast_types(to_dt, from_dt),
        }
    }

    /// Promote/merge two field types
    /// Returns the promoted field type, combining nullability.
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
            // String type promotions
            (
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            ) => Some(input_type.clone()),

            // Binary type promotions
            (
                DataType::Binary | DataType::BinaryView | DataType::LargeBinary,
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
            ) => Some(input_type.clone()),

            // Decimal type promotions
            (
                DataType::Decimal128(table_precision, table_scale)
                | DataType::Decimal256(table_precision, table_scale),
                DataType::Decimal128(input_precision, input_scale),
            ) => {
                if input_precision <= table_precision && input_scale <= table_scale {
                    Some(table_type.clone()) // Keep table type for decimals
                } else {
                    return Err(DataFusionError::Plan(format!(
                        "Cannot merge field {} from {} to {}. Decimal precision/scale mismatch.",
                        table_field.name(),
                        input_type,
                        table_type
                    )));
                }
            }

            // For other types, use Arrow's can_cast_types to determine compatibility
            // Prefer widening conversions (table -> input) over narrowing (input -> table)
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
            // Create new field with merged type, combining nullability
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

    /// Validate that a cast operation is safe
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
            | (DataType::Int16, DataType::Int8) => {
                // Allow these casts now but could add runtime checks sooner
            }

            _ => {}
        }

        Ok(())
    }

    /// Get the "wider" type between two compatible types for promotion
    pub fn get_wider_type(type1: &DataType, type2: &DataType) -> Option<DataType> {
        if type1 == type2 {
            return Some(type1.clone());
        }

        match (type1, type2) {
            // Integer promotions
            (DataType::Int8, DataType::Int16 | DataType::Int32 | DataType::Int64) => {
                Some(type2.clone())
            }
            (DataType::Int16, DataType::Int32 | DataType::Int64) => Some(type2.clone()),
            (DataType::Int32, DataType::Int64) => Some(type2.clone()),

            // Reverse cases
            (DataType::Int16 | DataType::Int32 | DataType::Int64, DataType::Int8) => {
                Some(type1.clone())
            }
            (DataType::Int32 | DataType::Int64, DataType::Int16) => Some(type1.clone()),
            (DataType::Int64, DataType::Int32) => Some(type1.clone()),

            // Float promotions
            (DataType::Float32, DataType::Float64) => Some(DataType::Float64),
            (DataType::Float64, DataType::Float32) => Some(DataType::Float64),

            // String promotions
            (DataType::Utf8, DataType::LargeUtf8) => Some(DataType::LargeUtf8),
            (DataType::LargeUtf8, DataType::Utf8) => Some(DataType::LargeUtf8),

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
        // Same types
        assert!(DeltaTypeConverter::can_promote_types(
            &DataType::Int32,
            &DataType::Int32
        ));

        // String types
        assert!(DeltaTypeConverter::can_promote_types(
            &DataType::Utf8,
            &DataType::LargeUtf8
        ));

        // Decimal types
        assert!(DeltaTypeConverter::can_promote_types(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(12, 2)
        ));

        // Test that our method works correctly for compatible types
        // Note: Arrow's can_cast_types is quite permissive, so we focus on testing our specific logic
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

        // Should combine nullability - when types are identical, we return table field
        // but with combined nullability
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

        // Should prefer input type (LargeUtf8) and combine nullability
        assert!(result.is_nullable());
        assert_eq!(result.data_type(), &DataType::LargeUtf8);
    }

    #[test]
    fn test_decimal_type_promotion() {
        let table_field = Field::new("test", DataType::Decimal128(12, 2), false);
        let input_field = Field::new("test", DataType::Decimal128(10, 2), true);

        let result = DeltaTypeConverter::promote_field_types(&table_field, &input_field)
            .unwrap_or(Field::new("test", DataType::Null, true));

        // Should keep table type for decimals when input fits
        assert!(result.is_nullable());
        assert_eq!(result.data_type(), &DataType::Decimal128(12, 2));
    }

    #[test]
    fn test_incompatible_decimal_promotion() {
        let table_field = Field::new("test", DataType::Decimal128(10, 2), false);
        let input_field = Field::new("test", DataType::Decimal128(12, 3), true);

        let result = DeltaTypeConverter::promote_field_types(&table_field, &input_field);

        // Should fail for incompatible decimal precision/scale
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
