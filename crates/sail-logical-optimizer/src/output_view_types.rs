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
