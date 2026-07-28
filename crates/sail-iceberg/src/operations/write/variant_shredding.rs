pub use sail_common_datafusion::variant::{
    DEFAULT_VARIANT_INFERENCE_NODE_BUDGET, VariantShreddingPlan, apply_variant_shredding_plan,
    build_variant_shredding_plan, unshred_shredded_variants_for_write,
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, StringArray, StructArray};
    use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion_common::{DataFusionError, Result};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet_variant_compute::{
        VariantArray, VariantType, json_to_variant, shred_variant, unshred_variant, variant_to_json,
    };
    use sail_common_datafusion::array::record_batch::cast_record_batch_relaxed_tz;

    use super::*;

    fn logical_variant_field(name: &str) -> Field {
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

    fn shredded_variant_column(
        name: &str,
        value: &str,
        shredding_type: &DataType,
    ) -> Result<(FieldRef, ArrayRef)> {
        let json = Arc::new(StringArray::from(vec![Some(value)])) as ArrayRef;
        let variant = json_to_variant(&json)?;
        let shredded = shred_variant(&variant, shredding_type)?;
        let field = Arc::new(
            variant
                .field(name)
                .with_data_type(shredded.data_type().clone()),
        );
        Ok((field, Arc::new(shredded.into_inner()) as ArrayRef))
    }

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
    fn write_alignment_unshreds_typed_value_only_variant() -> Result<()> {
        let (field, column) = shredded_variant_column("payload", "42", &DataType::Int64)?;
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![column])?;
        let target_schema = Arc::new(Schema::new(vec![logical_variant_field("payload")]));

        let normalized = unshred_shredded_variants_for_write(&batch, &target_schema)
            .map_err(DataFusionError::Plan)?;
        let normalized_schema = normalized.schema();
        let normalized_field = normalized_schema.field_with_name("payload")?;
        let DataType::Struct(fields) = normalized_field.data_type() else {
            return Err(DataFusionError::Plan(
                "expected normalized variant struct".to_string(),
            ));
        };
        assert!(fields.iter().any(|field| field.name() == "value"));
        assert!(!fields.iter().any(|field| field.name() == "typed_value"));

        let aligned = cast_record_batch_relaxed_tz(&normalized, &target_schema)?;
        let json = variant_to_json(aligned.column(0))?;
        assert_eq!(json.value(0), "42");
        Ok(())
    }

    #[test]
    fn write_alignment_unshreds_nested_variant() -> Result<()> {
        let (payload_field, payload_column) = shredded_variant_column(
            "payload",
            r#"{"a":2}"#,
            &DataType::Struct(vec![Field::new("a", DataType::Int64, true)].into()),
        )?;
        let wrapper_column = Arc::new(StructArray::try_new(
            Fields::from(vec![payload_field.clone()]),
            vec![payload_column],
            None,
        )?) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "wrapper",
                DataType::Struct(Fields::from(vec![payload_field])),
                true,
            )])),
            vec![wrapper_column],
        )?;
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "wrapper",
            DataType::Struct(Fields::from(vec![Arc::new(logical_variant_field(
                "payload",
            ))])),
            true,
        )]));

        let normalized = unshred_shredded_variants_for_write(&batch, &target_schema)
            .map_err(DataFusionError::Plan)?;
        let aligned = cast_record_batch_relaxed_tz(&normalized, &target_schema)?;
        let wrapper = aligned
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Plan("expected wrapper struct".to_string()))?;
        let payload = wrapper
            .column_by_name("payload")
            .ok_or_else(|| DataFusionError::Plan("expected payload field".to_string()))?;
        let json = variant_to_json(payload)?;
        assert_eq!(json.value(0), r#"{"a":2}"#);
        Ok(())
    }

    #[test]
    fn variant_shredding_plan_preserves_iceberg_field_metadata() -> Result<()> {
        let batch = variant_batch(vec![
            Some(r#"{"a":2,"b":"iceberg","nested":{"c":7}}"#),
            Some(r#"{"a":5,"b":"sail","nested":{"c":9}}"#),
        ])?;
        let plan = build_variant_shredding_plan(
            &batch.schema(),
            std::slice::from_ref(&batch),
            100,
            DEFAULT_VARIANT_INFERENCE_NODE_BUDGET,
        )
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
        assert!(
            payload_fields
                .iter()
                .any(|field| field.name() == "typed_value")
        );
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
}
