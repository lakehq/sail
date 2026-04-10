use sail_common_datafusion::catalog::{CatalogPartitionField, PartitionTransform};

use crate::spec::transform::Transform;

pub fn partition_field_name(field: &CatalogPartitionField) -> String {
    match field.transform.unwrap_or(PartitionTransform::Identity) {
        PartitionTransform::Identity => field.column.clone(),
        PartitionTransform::Year => format!("{}_year", field.column),
        PartitionTransform::Month => format!("{}_month", field.column),
        PartitionTransform::Day => format!("{}_day", field.column),
        PartitionTransform::Hour => format!("{}_hour", field.column),
        PartitionTransform::Bucket(_) => format!("{}_bucket", field.column),
        PartitionTransform::Truncate(_) => format!("{}_trunc", field.column),
    }
}

pub fn format_partition_expr(field: &CatalogPartitionField) -> String {
    match field.transform.unwrap_or(PartitionTransform::Identity) {
        PartitionTransform::Identity => field.column.clone(),
        PartitionTransform::Year => format!("years({})", field.column),
        PartitionTransform::Month => format!("months({})", field.column),
        PartitionTransform::Day => format!("days({})", field.column),
        PartitionTransform::Hour => format!("hours({})", field.column),
        PartitionTransform::Bucket(n) => format!("bucket({n}, {})", field.column),
        PartitionTransform::Truncate(w) => format!("truncate({w}, {})", field.column),
    }
}

pub fn format_partition_exprs(fields: &[CatalogPartitionField]) -> Vec<String> {
    fields.iter().map(format_partition_expr).collect()
}

pub fn iceberg_transform_from_partition_field(field: &CatalogPartitionField) -> Transform {
    match field.transform.unwrap_or(PartitionTransform::Identity) {
        PartitionTransform::Identity => Transform::Identity,
        PartitionTransform::Year => Transform::Year,
        PartitionTransform::Month => Transform::Month,
        PartitionTransform::Day => Transform::Day,
        PartitionTransform::Hour => Transform::Hour,
        PartitionTransform::Bucket(n) => Transform::Bucket(n),
        PartitionTransform::Truncate(w) => Transform::Truncate(w),
    }
}

pub fn catalog_partition_field_from_iceberg(
    source_column: String,
    transform: Transform,
) -> Result<CatalogPartitionField, String> {
    let transform = match transform {
        Transform::Identity => None,
        Transform::Year => Some(PartitionTransform::Year),
        Transform::Month => Some(PartitionTransform::Month),
        Transform::Day => Some(PartitionTransform::Day),
        Transform::Hour => Some(PartitionTransform::Hour),
        Transform::Bucket(n) => Some(PartitionTransform::Bucket(n)),
        Transform::Truncate(w) => Some(PartitionTransform::Truncate(w)),
        other => {
            return Err(format!(
                "unsupported Iceberg partition transform '{other}' for column '{source_column}'"
            ))
        }
    };
    Ok(CatalogPartitionField {
        column: source_column,
        transform,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_identity_column() {
        let field = CatalogPartitionField {
            column: "event_date".to_string(),
            transform: None,
        };
        assert_eq!(partition_field_name(&field), "event_date");
        assert_eq!(format_partition_expr(&field), "event_date");
    }

    #[test]
    fn format_years() {
        let field = CatalogPartitionField {
            column: "event_date".to_string(),
            transform: Some(PartitionTransform::Year),
        };
        assert_eq!(partition_field_name(&field), "event_date_year");
        assert_eq!(format_partition_expr(&field), "years(event_date)");
    }

    #[test]
    fn format_truncate() {
        let field = CatalogPartitionField {
            column: "user_id".to_string(),
            transform: Some(PartitionTransform::Truncate(8)),
        };
        assert_eq!(partition_field_name(&field), "user_id_trunc");
        assert_eq!(format_partition_expr(&field), "truncate(8, user_id)");
    }

    #[test]
    fn convert_from_iceberg_transform() -> Result<(), String> {
        let field =
            catalog_partition_field_from_iceberg("event_date".to_string(), Transform::Year)?;
        assert_eq!(
            field,
            CatalogPartitionField {
                column: "event_date".to_string(),
                transform: Some(PartitionTransform::Year),
            }
        );
        Ok(())
    }
}
