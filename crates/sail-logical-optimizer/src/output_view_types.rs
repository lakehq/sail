use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef, Fields, Schema};
use datafusion::optimizer::AnalyzerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DFSchema, DFSchemaRef, Result, TableReference};
use datafusion_expr::LogicalPlan;
use datafusion_expr::expr_rewriter::coerce_plan_expr_for_schema;

#[derive(Debug)]
pub struct ExpandViewTypesAtOutput;

impl AnalyzerRule for ExpandViewTypesAtOutput {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        if !config.optimizer.expand_views_at_output {
            return Ok(plan);
        }

        match expanded_output_schema(plan.schema())? {
            Some(schema) => coerce_plan_expr_for_schema(plan, &schema),
            None => Ok(plan),
        }
    }

    fn name(&self) -> &str {
        "expand_view_types_at_output"
    }
}

fn expanded_output_schema(schema: &DFSchemaRef) -> Result<Option<DFSchema>> {
    let mut transformed = false;
    let (qualifiers, fields): (Vec<Option<TableReference>>, Vec<FieldRef>) = schema
        .iter()
        .map(|(qualifier, field)| {
            (
                qualifier.cloned(),
                expand_output_field(field, &mut transformed),
            )
        })
        .unzip();

    if !transformed {
        return Ok(None);
    }

    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        schema.as_arrow().metadata().clone(),
    ));
    Ok(Some(DFSchema::from_field_specific_qualified_schema(
        qualifiers, &schema,
    )?))
}

fn expand_output_field(field: &FieldRef, transformed: &mut bool) -> FieldRef {
    let data_type = expand_output_data_type(field.data_type(), transformed);
    if &data_type == field.data_type() {
        Arc::clone(field)
    } else {
        Arc::new(field.as_ref().clone().with_data_type(data_type))
    }
}

fn expand_output_data_type(data_type: &DataType, transformed: &mut bool) -> DataType {
    match data_type {
        DataType::Utf8View => {
            *transformed = true;
            DataType::LargeUtf8
        }
        DataType::BinaryView => {
            *transformed = true;
            DataType::LargeBinary
        }
        DataType::List(field) => DataType::List(expand_output_field(field, transformed)),
        DataType::ListView(field) => DataType::ListView(expand_output_field(field, transformed)),
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(expand_output_field(field, transformed), *size)
        }
        DataType::LargeList(field) => DataType::LargeList(expand_output_field(field, transformed)),
        DataType::LargeListView(field) => {
            DataType::LargeListView(expand_output_field(field, transformed))
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| expand_output_field(field, transformed))
                .collect::<Fields>(),
        ),
        DataType::Union(fields, mode) => DataType::Union(
            fields
                .iter()
                .map(|(type_id, field)| (type_id, expand_output_field(field, transformed)))
                .collect(),
            *mode,
        ),
        DataType::Dictionary(key, value) => DataType::Dictionary(
            Box::new(expand_output_data_type(key, transformed)),
            Box::new(expand_output_data_type(value, transformed)),
        ),
        DataType::Map(field, sorted) => {
            DataType::Map(expand_output_field(field, transformed), *sorted)
        }
        DataType::RunEndEncoded(run_ends, values) => DataType::RunEndEncoded(
            expand_output_field(run_ends, transformed),
            expand_output_field(values, transformed),
        ),
        data_type => data_type.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::Field;
    use datafusion_expr::logical_plan::EmptyRelation;

    use super::*;

    fn nested_view_type() -> DataType {
        DataType::Struct(
            vec![
                Field::new("name", DataType::Utf8View, true),
                Field::new(
                    "aliases",
                    DataType::List(Arc::new(Field::new_list_field(DataType::Utf8View, true))),
                    true,
                ),
                Field::new("payload", DataType::BinaryView, true),
            ]
            .into(),
        )
    }

    fn nested_expanded_type() -> DataType {
        DataType::Struct(
            vec![
                Field::new("name", DataType::LargeUtf8, true),
                Field::new(
                    "aliases",
                    DataType::List(Arc::new(Field::new_list_field(DataType::LargeUtf8, true))),
                    true,
                ),
                Field::new("payload", DataType::LargeBinary, true),
            ]
            .into(),
        )
    }

    #[test]
    fn expands_nested_views_only_at_the_root_output() -> Result<()> {
        let input_schema = Arc::new(DFSchema::from_unqualified_fields(
            vec![Field::new("details", nested_view_type(), true)].into(),
            HashMap::new(),
        )?);
        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::clone(&input_schema),
        });
        let mut config = ConfigOptions::default();
        config.optimizer.expand_views_at_output = true;

        let output = ExpandViewTypesAtOutput.analyze(input, &config)?;

        assert_eq!(
            output.schema().field(0).data_type(),
            &nested_expanded_type()
        );
        let LogicalPlan::Projection(projection) = output else {
            return datafusion_common::internal_err!("expected an output projection");
        };
        assert_eq!(
            projection.input.schema().field(0).data_type(),
            &nested_view_type()
        );
        Ok(())
    }

    #[test]
    fn preserves_views_when_output_expansion_is_disabled() -> Result<()> {
        let schema = Arc::new(DFSchema::from_unqualified_fields(
            vec![Field::new("details", nested_view_type(), true)].into(),
            HashMap::new(),
        )?);
        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema,
        });

        let output = ExpandViewTypesAtOutput.analyze(input, &ConfigOptions::default())?;

        assert!(matches!(output, LogicalPlan::EmptyRelation(_)));
        assert_eq!(output.schema().field(0).data_type(), &nested_view_type());
        Ok(())
    }
}
